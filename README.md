# HyperSync Go Client (Fork)

> **This is an unofficial fork of [enviodev/hypersync-client-go](https://github.com/enviodev/hypersync-client-go).**
>
> We maintain this fork for our own internal use at [terminally-online](https://github.com/terminally-online). It includes additional features and modifications that may diverge from the upstream implementation.
>
> **For production use, we recommend the official client:** [github.com/enviodev/hypersync-client-go](https://github.com/enviodev/hypersync-client-go)

## Fork Additions

This fork includes the following features not present in the upstream:

- **Dynamic batch size adjustment** - Automatically adjusts batch size based on response size (matching Rust canonical implementation)
- **`CollectParquet()`** - Stream data directly to Parquet files for local storage
- **`StreamHeight()`** - SSE-based real-time block height streaming with auto-reconnect
- **`GetChainId()`** - Retrieve chain ID from HyperSync server
- **`runQueryToEnd()`** - Proper pagination handling within batch queries
- **Environment-based configuration** - Examples use `.env` files for bearer tokens

## Installation

```bash
go get github.com/terminally-online/hypersync-client-go
```

## Quick Start

```go
package main

import (
    "context"
    "math/big"
    "os"

    hypersyncgo "github.com/terminally-online/hypersync-client-go"
    "github.com/terminally-online/hypersync-client-go/options"
    "github.com/terminally-online/hypersync-client-go/utils"
    "github.com/joho/godotenv"
)

func main() {
    _ = godotenv.Load()
    bearerToken := os.Getenv("HYPERSYNC_BEARER_TOKEN")

    opts := options.Options{
        Blockchains: []options.Node{{
            Type:        utils.EthereumNetwork,
            NetworkId:   utils.EthereumNetworkID,
            Endpoint:    "https://eth.hypersync.xyz",
            RpcEndpoint: "https://eth.rpc.hypersync.xyz",
            BearerToken: &bearerToken,
        }},
    }

    ctx := context.Background()
    hsClient, _ := hypersyncgo.NewHyper(ctx, opts)
    client, _ := hsClient.GetClient(utils.EthereumNetworkID)

    // Get current height
    height, _ := client.GetHeight(ctx)

    // Stream blocks
    stream, _ := client.StreamBlocksInRange(ctx, big.NewInt(20000000), height, nil)
    for response := range stream.Channel() {
        // Process blocks...
        stream.Ack()
    }
}
```

## Examples

See the [examples](./examples) directory:

- `blocks_in_range.go` - Stream blocks
- `logs_in_range.go` - Stream event logs
- `transactions_in_range.go` - Stream transactions
- `traces_in_range.go` - Stream traces
- `parquet_collect.go` - Export data to Parquet files
- `stream_height.go` - Real-time height streaming via SSE
- `sighash.go` - Query by function signature
- `erc721_events_decoded.go` - Decode ERC721 events

## Configuration

Create a `.env` file (see `.env.example`):

```bash
HYPERSYNC_BEARER_TOKEN=your-api-token-here
```

Get your API token from [envio.dev/app/api-tokens](https://envio.dev/app/api-tokens)

## Documentation

- [HyperSync Documentation](https://docs.envio.dev/docs/HyperSync/overview)
- [Official Clients](https://docs.envio.dev/docs/HyperSync/hypersync-clients)
- [Envio](https://envio.dev)

## License

This fork maintains the same license as the upstream repository.

---

*This fork is maintained by [terminally-online](https://github.com/terminally-online). For issues specific to this fork, please open an issue in this repository. For general HyperSync issues, please use the [official repository](https://github.com/enviodev/hypersync-client-go).*
