# Event Hub to ASA POC

This Go application creates a configurable number of goroutines that generate and send events to Azure Event Hub. Each goroutine randomly selects SKU family names and core counts, then sends these as events to Event Hub using the latest Azure SDK.

## Features

- **Modern Azure SDK**: Uses `github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs` for optimal performance
- **Configurable Goroutines**: Set the number of concurrent goroutines via environment variables
- **Random Event Generation**: Each goroutine randomly selects 1-100 SKU families and assigns 0-100 cores
- **Unique CCP Identification**: Each goroutine is assigned a unique GUID (CCP ID) for event tracking
- **Azure Default Credentials**: Secure authentication using Azure Default Credential chain
- **Intelligent Partitioning**: Groups events by SKU family as partition keys for better distribution
- **Batch Processing**: Events are sent in batches for optimal performance
- **Graceful Shutdown**: Handles interrupt signals and stops gracefully
- **Structured Logging**: Clear logging for monitoring and debugging

## Configuration

Copy `.env.example` to `.env` and configure the following variables:

```bash
# Required: Event Hub namespace (FQDN format)
EVENT_HUB_NAMESPACE=your-eventhub-namespace.servicebus.windows.net

# Required: Event Hub name
EVENT_HUB_NAME=your-event-hub-name

# Optional: Number of goroutines (default: 5)
GOROUTINE_COUNT=5

# Optional: Interval between event generation in seconds (default: 60)
INTERVAL_SECONDS=60
```

## Authentication

This application uses **Azure Default Credentials** for secure authentication. The Azure Default Credential tries multiple authentication methods in the following order:

1. **Environment Variables** (Service Principal):
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`
   - `AZURE_TENANT_ID`

2. **Managed Identity** (when running in Azure):
   - System-assigned managed identity
   - User-assigned managed identity

3. **Azure CLI** (for local development):
   - Run `az login` to authenticate

4. **Azure PowerShell** (alternative for local development)

5. **Interactive Browser** (fallback method)

### Required Azure Permissions

Ensure your identity (Service Principal, Managed Identity, or user account) has the following role assignment on the Event Hub:
- **Azure Event Hubs Data Sender** role

## Setup and Installation

1. **Install Go dependencies**:
   ```bash
   go mod tidy
   ```

2. **Set up Azure Event Hub**:
   - Create an Event Hub namespace and Event Hub in Azure
   - Assign appropriate permissions (see Authentication section)

3. **Configure authentication** (choose one method):

   **For local development with Azure CLI**:
   ```bash
   az login
   ```

   **For Service Principal authentication**:
   ```bash
   export AZURE_CLIENT_ID=your-client-id
   export AZURE_CLIENT_SECRET=your-client-secret
   export AZURE_TENANT_ID=your-tenant-id
   ```

   **For production (Managed Identity)**: No additional setup required when running in Azure

4. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your Azure Event Hub details
   ```

5. **Run the application**:
   ```bash
   go run main.go
   ```

## Event Structure

Each event sent to Event Hub has the following JSON structure:

```json
{
  "sku_family": "Standard_D2s_v3",
  "cores": 42,
  "timestamp": "2025-05-30T10:30:00Z",
  "ccp_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

## Authentication Methods

The application uses **Azure Default Credentials** which provides a secure, credential-chain approach:

### Local Development
- **Azure CLI**: Run `az login` to authenticate with your user account
- **Azure PowerShell**: Use `Connect-AzAccount`
- **Service Principal**: Set environment variables for automated scenarios

### Production (Azure-hosted)
- **Managed Identity**: Automatically uses the assigned managed identity
- **Service Principal**: Set environment variables in Azure App Service configuration

### Environment Variables for Service Principal
```bash
AZURE_CLIENT_ID=your-application-client-id
AZURE_CLIENT_SECRET=your-application-secret
AZURE_TENANT_ID=your-azure-tenant-id
```

## Build for Production

```bash
# Build binary
go build -o event-hub-producer main.go

# Run binary
./event-hub-producer
```

## Monitoring

The application provides structured logging that includes:
- CCP startup and shutdown messages with unique GUIDs
- Event generation counts per CCP
- Error handling and retry information
- Performance metrics

## Architecture Changes

### SDK Migration
The application has been refactored to use the latest Azure Event Hubs SDK:
- **Old SDK**: `github.com/Azure/azure-event-hubs-go/v3`
- **New SDK**: `github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs`

### Key Improvements
- **Better Performance**: The new SDK provides improved performance and reliability
- **Enhanced Partition Management**: Events are intelligently grouped by SKU family as partition keys
- **Modern Authentication**: Streamlined authentication with Azure Identity
- **Improved Error Handling**: Better error messages and retry mechanisms

## Security Best Practices

- ✅ Uses Azure Default Credentials (no connection strings)
- ✅ No hardcoded credentials or secrets
- ✅ Follows Azure Identity best practices
- ✅ Proper error handling and logging
- ✅ Graceful shutdown handling
- ✅ Timeout contexts for operations
- ✅ Batch processing for efficiency
- ✅ Secure authentication credential chain

## SKU Families

The application includes 100 predefined Azure VM SKU families including:
- A-series (Basic and Standard)
- B-series (Burstable)
- D-series and DS-series
- E-series (Memory optimized)
- F-series (Compute optimized)
- G-series and GS-series
- H-series (High performance compute)
- L-series (Storage optimized)
- M-series (Memory optimized)
- N-series (GPU enabled)
