package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaProducer wraps the Kafka producer client
type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

// SendEvents sends events to Event Hub using the Kafka protocol
func (p *KafkaProducer) SendEvents(ctx context.Context, events []SKUEvent, partitionKey string) (int, error) {
	var successCount int
	deliveryChan := make(chan kafka.Event, len(events))

	// Send each event to Kafka
	for i, event := range events {
		// Serialize to JSON
		jsonData, err := json.Marshal(event)
		if err != nil {
			return successCount, fmt.Errorf("failed to marshal event data for event %d: %v", i+1, err)
		}

		// Create and send a Kafka message with the partition key
		err = p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &p.topic,
				Partition: kafka.PartitionAny, // Let Kafka handle partitioning based on the key
			},
			Key:   []byte(partitionKey), // Use CCP ID as the partition key
			Value: jsonData,
		}, deliveryChan)

		if err != nil {
			return successCount, fmt.Errorf("failed to produce Kafka message for event %d: %v", i+1, err)
		}
	}

	// Wait for delivery reports
	for i := 0; i < len(events); i++ {
		select {
		case <-ctx.Done():
			return successCount, ctx.Err()
		case ev := <-deliveryChan:
			m := ev.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				log.Printf("ERROR: Failed to deliver message: %v", m.TopicPartition.Error)
			} else {
				successCount++
			}
		case <-time.After(30 * time.Second):
			return successCount, fmt.Errorf("timeout waiting for message delivery")
		}
	}

	return successCount, nil
}

// Close closes the Kafka producer
func (p *KafkaProducer) Close(_ context.Context) error {
	p.producer.Close()
	return nil
}

// createKafkaProducer creates a Kafka producer client with MSI authentication
func createKafkaProducer(config *Config) (*KafkaProducer, error) {
	log.Println("INFO: Initializing Kafka producer with MSI authentication...")

	// Create Azure Default Credential
	log.Println("DEBUG: Creating Azure Default Credential for Kafka OAuth...")
	credentialStart := time.Now()
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("ERROR: Failed to create Azure Default Credential: %v", err)
		return nil, fmt.Errorf("failed to create Azure Default Credential for Kafka: %v", err)
	}
	credentialDuration := time.Since(credentialStart)
	log.Printf("SUCCESS: Azure Default Credential created (took %v)", credentialDuration)

	// Get initial token for Event Hubs
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get token with Event Hubs scope
	scope := fmt.Sprintf("https://%s/.default", config.KafkaBroker)
	log.Printf("DEBUG: Requesting initial OAuth token with scope: %s", scope)

	tokenOptions := policy.TokenRequestOptions{
		Scopes: []string{scope},
	}

	token, err := credential.GetToken(ctx, tokenOptions)

	if err != nil {
		log.Printf("ERROR: Failed to get initial OAuth token: %v", err)
		return nil, fmt.Errorf("failed to get initial OAuth token: %v", err)
	}

	log.Printf("DEBUG: Successfully obtained initial OAuth token (expires: %s)", token.ExpiresOn.Format(time.RFC3339))

	// Construct the SASL OAUTHBEARER token
	tokenExpiryMs := token.ExpiresOn

	// Create Kafka configuration with initial OAuth token
	log.Println("DEBUG: Setting up Kafka configuration with MSI OAuth authentication...")
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":                   fmt.Sprintf("%s:9093", config.KafkaBroker),
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanisms":                     "OAUTHBEARER",
		"enable.ssl.certificate.verification": "false", // For dev/test only, enable in production

		// Producer-specific configs
		"acks":                         "all",  // Wait for full acknowledgment
		"delivery.timeout.ms":          30000,  // 30 seconds delivery timeout
		"message.timeout.ms":           10000,  // 10 seconds message timeout
		"retries":                      5,      // Retry up to 5 times
		"retry.backoff.ms":             500,    // 500ms between retries
		"queue.buffering.max.messages": 100000, // Size of the producer queue
		"queue.buffering.max.ms":       5,      // Batch messages for 5ms
		"batch.size":                   16384,  // Batch size in bytes
	}

	// Create the Kafka producer
	log.Println("DEBUG: Creating Kafka producer...")
	producer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		log.Printf("ERROR: Failed to create Kafka producer: %v", err)
		return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
	}

	// Set the initial token
	oauthToken := kafka.OAuthBearerToken{
		Principal:  "azure-msi",
		TokenValue: token.Token,
		Extensions: map[string]string{},
		Expiration: tokenExpiryMs,
	}

	if err := producer.SetOAuthBearerToken(oauthToken); err != nil {
		log.Printf("ERROR: Failed to set OAuth bearer token: %v", err)
		producer.Close()
		return nil, fmt.Errorf("failed to set OAuth bearer token: %v", err)
	}

	// Start a goroutine to handle events like token refresh
	go func() {
		for event := range producer.Events() {
			switch ev := event.(type) {
			case kafka.OAuthBearerTokenRefresh:
				log.Println("DEBUG: OAuth bearer token refresh event received")

				// Get a new token
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				token, err := credential.GetToken(ctx, tokenOptions)
				cancel()

				if err != nil {
					log.Printf("ERROR: Failed to refresh OAuth token: %v", err)
					producer.SetOAuthBearerTokenFailure(fmt.Sprintf("failed to refresh token: %v", err))
					continue
				}

				// Set the new token
				oauthToken := kafka.OAuthBearerToken{
					Principal:  "azure-msi",
					TokenValue: token.Token,
					Extensions: map[string]string{},
					Expiration: token.ExpiresOn,
				}

				if err := producer.SetOAuthBearerToken(oauthToken); err != nil {
					log.Printf("ERROR: Failed to set refreshed OAuth bearer token: %v", err)
					producer.SetOAuthBearerTokenFailure(fmt.Sprintf("failed to set token: %v", err))
					continue
				}

				log.Printf("DEBUG: Successfully refreshed OAuth token (expires: %s)", token.ExpiresOn.Format(time.RFC3339))

			case *kafka.Message:
				// Delivery report
				if ev.TopicPartition.Error != nil {
					log.Printf("ERROR: Failed to deliver message: %v", ev.TopicPartition.Error)
				} else {
					log.Printf("DEBUG: Message delivered to topic %s [%d] at offset %v",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}

			case kafka.Error:
				log.Printf("ERROR: Kafka error: %v", ev)
			}
		}
	}()

	log.Printf("SUCCESS: Kafka producer created for broker %s and topic %s", config.KafkaBroker, config.EventHubName)
	return &KafkaProducer{
		producer: producer,
		topic:    config.EventHubName,
	}, nil
}
