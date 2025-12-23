//go:build ignore
// +build ignore

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	hypersyncgo "github.com/terminally-online/hypersync-client-go"
	"github.com/terminally-online/hypersync-client-go/logger"
	"github.com/terminally-online/hypersync-client-go/options"
	"github.com/terminally-online/hypersync-client-go/utils"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	_ = godotenv.Load()
	bearerToken := os.Getenv("HYPERSYNC_BEARER_TOKEN")

	opts := options.Options{
		Blockchains: []options.Node{
			{
				Type:        utils.EthereumNetwork,
				NetworkId:   utils.EthereumNetworkID,
				Endpoint:    "https://eth.hypersync.xyz",
				RpcEndpoint: "https://eth.rpc.hypersync.xyz",
				BearerToken: &bearerToken,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.L().Info("Received shutdown signal, closing stream...")
		cancel()
	}()

	hsClient, err := hypersyncgo.NewHyper(ctx, opts)
	if err != nil {
		logger.L().Error("failed to create hyper client", zap.Error(err))
		return
	}

	client, found := hsClient.GetClient(utils.EthereumNetworkID)
	if !found {
		logger.L().Error("failure to discover hyper client")
		return
	}

	chainId, err := client.GetChainId(ctx)
	if err != nil {
		logger.L().Error("failed to get chain id", zap.Error(err))
		return
	}
	logger.L().Info("Connected to chain", zap.Uint64("chain_id", chainId))

	currentHeight, err := client.GetHeight(ctx)
	if err != nil {
		logger.L().Error("failed to get current height", zap.Error(err))
		return
	}
	logger.L().Info("Current archive height", zap.Any("height", currentHeight))

	logger.L().Info("Starting height stream (Ctrl+C to stop)...")

	stream := client.StreamHeight(ctx)

	heightCount := 0
	startTime := time.Now()

	for {
		select {
		case event, ok := <-stream.Channel():
			if !ok {
				logger.L().Info("Height stream closed")
				return
			}

			if event.Error != nil {
				logger.L().Warn("Stream error (will reconnect)", zap.Error(event.Error))
				continue
			}

			if event.Connected {
				logger.L().Info("Connected to SSE endpoint")
				continue
			}

			if event.Height != nil {
				heightCount++
				logger.L().Info(
					"New height received",
					zap.Any("height", event.Height),
					zap.Int("heights_received", heightCount),
					zap.Duration("uptime", time.Since(startTime)),
				)
			}

		case <-stream.Done():
			logger.L().Info("Stream terminated")
			return

		case <-ctx.Done():
			logger.L().Info("Context cancelled, closing stream")
			stream.Close()
			return
		}
	}
}
