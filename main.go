package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// SKUEvent represents the structure of events we'll send to Event Hub
type SKUEvent struct {
	SKUFamily string    `json:"sku_family"`
	Cores     int       `json:"cores"`
	Timestamp time.Time `json:"timestamp"`
	CcpId     string    `json:"ccp_id"`
}

// Configuration holds all the configuration parameters
type Config struct {
	EventHubNamespace string
	EventHubName      string
	GoroutineCount    int
	IntervalSeconds   int
	UseKafka          bool   // Whether to use Kafka API instead of AMQP
	KafkaBroker       string // Kafka broker endpoint
}

// Static list of 100 SKU family names
var skuFamilies = []string{
	"Standard_A1_v2", "Standard_A2_v2", "Standard_A4_v2", "Standard_A8_v2",
	"Standard_B1s", "Standard_B1ms", "Standard_B2s", "Standard_B2ms", "Standard_B4ms", "Standard_B8ms",
	"Standard_D1_v2", "Standard_D2_v2", "Standard_D3_v2", "Standard_D4_v2", "Standard_D5_v2",
	"Standard_D11_v2", "Standard_D12_v2", "Standard_D13_v2", "Standard_D14_v2", "Standard_D15_v2",
	"Standard_DS1_v2", "Standard_DS2_v2", "Standard_DS3_v2", "Standard_DS4_v2", "Standard_DS5_v2",
	"Standard_DS11_v2", "Standard_DS12_v2", "Standard_DS13_v2", "Standard_DS14_v2", "Standard_DS15_v2",
	"Standard_D2s_v3", "Standard_D4s_v3", "Standard_D8s_v3", "Standard_D16s_v3", "Standard_D32s_v3", "Standard_D64s_v3",
	"Standard_D2_v3", "Standard_D4_v3", "Standard_D8_v3", "Standard_D16_v3", "Standard_D32_v3", "Standard_D64_v3",
	"Standard_E2s_v3", "Standard_E4s_v3", "Standard_E8s_v3", "Standard_E16s_v3", "Standard_E32s_v3", "Standard_E64s_v3",
	"Standard_E2_v3", "Standard_E4_v3", "Standard_E8_v3", "Standard_E16_v3", "Standard_E32_v3", "Standard_E64_v3",
	"Standard_F1s", "Standard_F2s", "Standard_F4s", "Standard_F8s", "Standard_F16s", "Standard_F32s", "Standard_F64s", "Standard_F72s",
	"Standard_F1", "Standard_F2", "Standard_F4", "Standard_F8", "Standard_F16",
	"Standard_G1", "Standard_G2", "Standard_G3", "Standard_G4", "Standard_G5",
	"Standard_GS1", "Standard_GS2", "Standard_GS3", "Standard_GS4", "Standard_GS5",
	"Standard_H8", "Standard_H16", "Standard_H8m", "Standard_H16m", "Standard_H16r", "Standard_H16mr",
	"Standard_L4s", "Standard_L8s", "Standard_L16s", "Standard_L32s",
	"Standard_M64s", "Standard_M64ms", "Standard_M128s", "Standard_M128ms",
	"Standard_NC6", "Standard_NC12", "Standard_NC24", "Standard_NC24r",
	"Standard_NV6", "Standard_NV12", "Standard_NV24",
	"Standard_A0", "Standard_A1", "Standard_A2", "Standard_A3", "Standard_A4", "Standard_A5", "Standard_A6", "Standard_A7",
	"Basic_A0", "Basic_A1", "Basic_A2", "Basic_A3", "Basic_A4",
	"Standard_D1", "Standard_D2", "Standard_D3", "Standard_D4",
	"Standard_D11", "Standard_D12", "Standard_D13", "Standard_D14",
	"Standard_DS1", "Standard_DS2", "Standard_DS3", "Standard_DS4",
	"Standard_DS11", "Standard_DS12", "Standard_DS13", "Standard_DS14",
}

func main() {
	log.Println("=== Event Hub Producer Starting ===")
	startTime := time.Now()

	// Load environment variables
	log.Println("Loading environment variables...")
	if err := godotenv.Load(); err != nil {
		log.Println("DEBUG: No .env file found, using system environment variables")
	} else {
		log.Println("DEBUG: Successfully loaded .env file")
	}

	log.Println("Loading configuration...")
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("FATAL: Failed to load configuration: %v", err)
	}

	log.Printf("=== Configuration Loaded Successfully ===")
	log.Printf("  Event Hub Namespace: %s", config.EventHubNamespace)
	log.Printf("  Event Hub Name: %s", config.EventHubName)
	log.Printf("  Use Kafka API: %t", config.UseKafka)
	if config.UseKafka {
		log.Printf("  Kafka Broker: %s", config.KafkaBroker)
	}
	log.Printf("  Goroutine Count: %d", config.GoroutineCount)
	log.Printf("  Interval Seconds: %d", config.IntervalSeconds)
	log.Printf("  Total SKU Families Available: %d", len(skuFamilies))

	// Create producer client (either Event Hub AMQP or Kafka)
	var producer EventProducer
	var err2 error

	if config.UseKafka {
		log.Println("Creating Kafka producer client...")
		producer, err2 = createKafkaProducer(config)
		if err2 != nil {
			log.Fatalf("FATAL: Failed to create Kafka producer client: %v", err2)
		}
		log.Println("SUCCESS: Kafka producer client created successfully")
	} else {
		log.Println("Creating Event Hub AMQP client...")
		client, err2 := createEventHubClient(config)
		if err2 != nil {
			log.Fatalf("FATAL: Failed to create Event Hub client: %v", err2)
		}
		producer = &EventHubProducer{client: client}
		log.Println("SUCCESS: Event Hub client created successfully")
	}

	defer func() {
		log.Println("Closing producer client...")
		if err := producer.Close(context.Background()); err != nil {
			log.Printf("ERROR: Failed to close producer client: %v", err)
		} else {
			log.Println("SUCCESS: Producer client closed successfully")
		}
	}()

	// Setup graceful shutdown
	log.Println("Setting up graceful shutdown handling...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	log.Println("DEBUG: Signal handlers registered for SIGINT and SIGTERM")

	var wg sync.WaitGroup

	initDuration := time.Since(startTime)
	log.Printf("=== Initialization Complete (took %v) ===", initDuration)
	log.Printf("Starting %d goroutines...", config.GoroutineCount)

	// Start goroutines
	for i := 0; i < config.GoroutineCount; i++ {
		wg.Add(1)
		go func(goroutineIndex int) {
			defer wg.Done()
			ccpID := uuid.New().String()
			log.Printf("DEBUG: Goroutine %d starting with CCP ID: %s", goroutineIndex+1, ccpID)
			runEventGenerator(ctx, producer, config, ccpID)
			log.Printf("DEBUG: Goroutine %d (CCP %s) finished", goroutineIndex+1, ccpID)
		}(i)
	}

	log.Printf("SUCCESS: All %d goroutines started successfully", config.GoroutineCount)
	log.Println("=== Producer is now running - Press Ctrl+C to stop ===")

	// Wait for interrupt signal
	<-sigChan
	shutdownStart := time.Now()
	log.Println("=== SHUTDOWN INITIATED ===")
	log.Println("INFO: Received interrupt signal, shutting down gracefully...")
	cancel()

	// Wait for all goroutines to complete
	log.Println("INFO: Waiting for all goroutines to complete...")
	wg.Wait()
	shutdownDuration := time.Since(shutdownStart)
	totalRuntime := time.Since(startTime)

	log.Printf("=== SHUTDOWN COMPLETE ===")
	log.Printf("  Shutdown Duration: %v", shutdownDuration)
	log.Printf("  Total Runtime: %v", totalRuntime)
	log.Println("INFO: All goroutines stopped. Exiting.")
}

// loadConfig loads configuration from environment variables
func loadConfig() (*Config, error) {
	log.Println("DEBUG: Reading environment variables...")
	config := &Config{
		EventHubNamespace: os.Getenv("EVENT_HUB_NAMESPACE"),
		EventHubName:      os.Getenv("EVENT_HUB_NAME"),
		UseKafka:          os.Getenv("USE_KAFKA") == "true",
		KafkaBroker:       os.Getenv("KAFKA_BROKER"),
	}

	log.Printf("DEBUG: EVENT_HUB_NAMESPACE=%s", config.EventHubNamespace)
	log.Printf("DEBUG: EVENT_HUB_NAME=%s", config.EventHubName)
	log.Printf("DEBUG: USE_KAFKA=%t", config.UseKafka)
	log.Printf("DEBUG: KAFKA_BROKER=%s", config.KafkaBroker)

	// Parse goroutine count
	goroutineCountStr := os.Getenv("GOROUTINE_COUNT")
	log.Printf("DEBUG: GOROUTINE_COUNT=%s", goroutineCountStr)
	if goroutineCountStr == "" {
		config.GoroutineCount = 5 // Default
		log.Println("DEBUG: Using default GOROUTINE_COUNT=5")
	} else {
		count, err := strconv.Atoi(goroutineCountStr)
		if err != nil {
			return nil, fmt.Errorf("invalid GOROUTINE_COUNT '%s': %v", goroutineCountStr, err)
		}
		config.GoroutineCount = count
		log.Printf("DEBUG: Parsed GOROUTINE_COUNT=%d", count)
	}

	// Parse interval seconds
	intervalStr := os.Getenv("INTERVAL_SECONDS")
	log.Printf("DEBUG: INTERVAL_SECONDS=%s", intervalStr)
	if intervalStr == "" {
		config.IntervalSeconds = 60 // Default to 1 minute
		log.Println("DEBUG: Using default INTERVAL_SECONDS=60")
	} else {
		interval, err := strconv.Atoi(intervalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid INTERVAL_SECONDS '%s': %v", intervalStr, err)
		}
		config.IntervalSeconds = interval
		log.Printf("DEBUG: Parsed INTERVAL_SECONDS=%d", interval)
	}

	// Validate required fields
	log.Println("DEBUG: Validating required configuration...")

	if config.UseKafka {
		// Validate Kafka configuration
		if config.KafkaBroker == "" {
			return nil, fmt.Errorf("KAFKA_BROKER is required but not set when USE_KAFKA=true")
		}
		if config.EventHubName == "" {
			return nil, fmt.Errorf("EVENT_HUB_NAME is required but not set (used as Kafka topic)")
		}
		log.Println("DEBUG: Kafka configuration validation passed")
	} else {
		// Validate Event Hub configuration
		if config.EventHubName == "" {
			return nil, fmt.Errorf("EVENT_HUB_NAME is required but not set")
		}
		if config.EventHubNamespace == "" {
			return nil, fmt.Errorf("EVENT_HUB_NAMESPACE is required but not set")
		}
		log.Println("DEBUG: Event Hub configuration validation passed")
	}

	log.Println("DEBUG: Configuration validation passed")
	return config, nil
}

// createEventHubClient creates an Event Hub producer client using Azure Default Credentials
func createEventHubClient(config *Config) (*azeventhubs.ProducerClient, error) {
	log.Println("INFO: Initializing Azure Default Credentials authentication...")
	log.Println("DEBUG: Azure Default Credential will try authentication methods in this order:")
	log.Println("  1. Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)")
	log.Println("  2. Managed Identity (when running in Azure)")
	log.Println("  3. Azure CLI credentials (for local development)")
	log.Println("  4. Azure PowerShell credentials")
	log.Println("  5. Interactive browser authentication (as fallback)")

	// Check for environment variables (for debugging)
	clientID := os.Getenv("AZURE_CLIENT_ID")
	tenantID := os.Getenv("AZURE_TENANT_ID")
	if clientID != "" && tenantID != "" {
		log.Printf("DEBUG: Found Service Principal environment variables (Client ID: %s...)", clientID[:8])
	} else {
		log.Println("DEBUG: No Service Principal environment variables found")
	}

	// Create Azure Default Credential
	log.Println("DEBUG: Creating Azure Default Credential...")
	credentialStart := time.Now()
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("ERROR: Failed to create Azure Default Credential: %v", err)
		return nil, fmt.Errorf("failed to create Azure Default Credential: %v", err)
	}
	credentialDuration := time.Since(credentialStart)
	log.Printf("SUCCESS: Azure Default Credential created (took %v)", credentialDuration)

	// Create the Event Hub producer client
	log.Printf("DEBUG: Creating Event Hub producer client for namespace: %s, hub: %s", config.EventHubNamespace, config.EventHubName)
	clientStart := time.Now()
	client, err := azeventhubs.NewProducerClient(config.EventHubNamespace, config.EventHubName, credential, nil)
	if err != nil {
		log.Printf("ERROR: Failed to create Event Hub producer client: %v", err)
		return nil, fmt.Errorf("failed to create Event Hub producer client: %v", err)
	}
	clientDuration := time.Since(clientStart)
	log.Printf("SUCCESS: Event Hub producer client created (took %v)", clientDuration)

	return client, nil
}

// EventProducer is an interface for different event producer implementations
type EventProducer interface {
	SendEvents(ctx context.Context, events []SKUEvent, partitionKey string) (int, error)
	Close(ctx context.Context) error
}

// EventHubProducer wraps the Event Hub producer client
type EventHubProducer struct {
	client *azeventhubs.ProducerClient
}

// SendEvents sends events to Event Hub using the AMQP protocol
func (p *EventHubProducer) SendEvents(ctx context.Context, events []SKUEvent, partitionKey string) (int, error) {
	// Create batch with partitionKey as partition key
	batchOptions := &azeventhubs.EventDataBatchOptions{
		PartitionKey: &partitionKey,
	}

	sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	batch, err := p.client.NewEventDataBatch(sendCtx, batchOptions)
	if err != nil {
		return 0, fmt.Errorf("failed to create event batch: %v", err)
	}

	// Convert SKU events to Event Hub events
	for i, event := range events {
		// Serialize to JSON
		jsonData, err := json.Marshal(event)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal event data for event %d: %v", i+1, err)
		}

		// Create Event Hub event
		eventData := &azeventhubs.EventData{
			Body: jsonData,
		}

		if err := batch.AddEventData(eventData, nil); err != nil {
			return 0, fmt.Errorf("failed to add event %d to batch: %v", i+1, err)
		}
	}

	if err := p.client.SendEventDataBatch(sendCtx, batch, nil); err != nil {
		return 0, fmt.Errorf("failed to send events: %v", err)
	}

	return len(events), nil
}

// Close closes the Event Hub producer client
func (p *EventHubProducer) Close(ctx context.Context) error {
	return p.client.Close(ctx)
}

// runEventGenerator runs the event generation loop for a single goroutine
func runEventGenerator(ctx context.Context, producer EventProducer, config *Config, ccpID string) {
	ticker := time.NewTicker(time.Duration(config.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	log.Printf("INFO: CCP %s - Event generator started (interval: %ds)", ccpID, config.IntervalSeconds)

	iterationCount := 0
	var totalEventsSent int64
	var totalErrors int64
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			duration := time.Since(startTime)
			log.Printf("INFO: CCP %s - Shutdown signal received after %v", ccpID, duration)
			log.Printf("STATS: CCP %s - Final stats: %d iterations, %d events sent, %d errors",
				ccpID, iterationCount, totalEventsSent, totalErrors)
			return
		case tick := <-ticker.C:
			iterationCount++
			log.Printf("DEBUG: CCP %s - Starting iteration %d at %v", ccpID, iterationCount, tick.Format(time.RFC3339))

			iterationStart := time.Now()
			eventCount, err := generateAndSendEvents(ctx, producer, ccpID)
			iterationDuration := time.Since(iterationStart)

			if err != nil {
				totalErrors++
				log.Printf("ERROR: CCP %s - Iteration %d failed (took %v): %v", ccpID, iterationCount, iterationDuration, err)
			} else {
				totalEventsSent += int64(eventCount)
				log.Printf("SUCCESS: CCP %s - Iteration %d completed (took %v) - %d events sent",
					ccpID, iterationCount, iterationDuration, eventCount)
			}

			// Log stats every 10 iterations
			if iterationCount%10 == 0 {
				avgEventsPerIteration := float64(totalEventsSent) / float64(iterationCount)
				runtime := time.Since(startTime)
				log.Printf("STATS: CCP %s - %d iterations completed in %v (avg %.1f events/iteration, %d errors)",
					ccpID, iterationCount, runtime, avgEventsPerIteration, totalErrors)
			}
		}
	}
}

// generateAndSendEvents generates a random number of SKU events and sends them to Event Hub
func generateAndSendEvents(ctx context.Context, producer EventProducer, ccpID string) (int, error) {
	// Choose a random number of SKU families (1 to 100)
	numSkus := rand.Intn(100) + 1
	log.Printf("DEBUG: CCP %s - Generating %d events", ccpID, numSkus)

	// Randomly select SKU families
	selectedSkus := make([]string, numSkus)
	for i := 0; i < numSkus; i++ {
		selectedSkus[i] = skuFamilies[rand.Intn(len(skuFamilies))]
	}

	log.Printf("DEBUG: CCP %s - Selected SKU families: %v", ccpID, selectedSkus[:min(5, len(selectedSkus))])
	if len(selectedSkus) > 5 {
		log.Printf("DEBUG: CCP %s - ... and %d more SKU families", ccpID, len(selectedSkus)-5)
	}

	// Create SKU events
	events := make([]SKUEvent, 0, numSkus)
	eventCreationStart := time.Now()

	for _, skuFamily := range selectedSkus {
		// Generate random core count (0 to 100)
		cores := rand.Intn(101)

		// Create event data
		eventData := SKUEvent{
			SKUFamily: skuFamily,
			Cores:     cores,
			Timestamp: time.Now().UTC(),
			CcpId:     ccpID,
		}

		events = append(events, eventData)
	}

	eventCreationDuration := time.Since(eventCreationStart)
	log.Printf("DEBUG: CCP %s - Created %d events for single partition (took %v)",
		ccpID, numSkus, eventCreationDuration)

	// Send events using the producer interface
	log.Printf("DEBUG: CCP %s - Starting send operation with CCP ID as partition key...", ccpID)
	sendStart := time.Now()

	// Create timeout context
	sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Send events using the producer interface
	sentCount, err := producer.SendEvents(sendCtx, events, ccpID)
	if err != nil {
		log.Printf("ERROR: CCP %s - Failed to send events: %v", ccpID, err)
		return 0, fmt.Errorf("failed to send events: %v", err)
	}

	sendDuration := time.Since(sendStart)
	log.Printf("SUCCESS: CCP %s - Send completed: %d events to partition '%s' (took %v)",
		ccpID, sentCount, ccpID, sendDuration)

	return sentCount, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
