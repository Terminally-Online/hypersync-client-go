//go:build ignore
// +build ignore

package main

import (
	"context"
	"math/big"
	"os"
	"time"

	hypersyncgo "github.com/terminally-online/hypersync-client-go"
	"github.com/terminally-online/hypersync-client-go/logger"
	"github.com/terminally-online/hypersync-client-go/options"
	"github.com/terminally-online/hypersync-client-go/types"
	"github.com/terminally-online/hypersync-client-go/utils"
	"github.com/ethereum/go-ethereum/common"
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

	hsClient, err := hypersyncgo.NewHyper(ctx, opts)
	if err != nil {
		logger.L().Error(
			"failed to create hyper client",
			zap.Error(err),
		)
		return
	}

	client, found := hsClient.GetClient(utils.EthereumNetworkID)
	if !found {
		logger.L().Error(
			"failure to discover hyper client",
			zap.Error(err),
			zap.Any("network_id", utils.EthereumNetworkID),
		)
		return
	}

	/* 2025/10/15 15:14:54 period 23582233-23585404 */
	/* 2025/10/15 15:14:54 max block: 23582409 */

	startBlock := big.NewInt(23582233)
	endBlock := big.NewInt(23585404)
	startTime := time.Now()

	logger.L().Info(
		"New logs in range stream request started",
		zap.Error(err),
		zap.Any("network_id", utils.EthereumNetworkID),
		zap.Any("start_block", startBlock),
		zap.Any("end_block", endBlock),
	)

	selections := []types.LogSelection{
		{
			Topics: [][]common.Hash{
				{
					// Transfer(address,address,uint256)
					common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"),
				},
			},
		},
	}

	batchSize := big.NewInt(50)
	bStream, bsErr := client.StreamLogsInRange(ctx, startBlock, endBlock, selections, options.DefaultStreamOptionsWithBatchSize(batchSize))
	if bsErr != nil {
		logger.L().Error(
			"failure to execute hyper client stream in range",
			zap.Error(err),
			zap.Any("network_id", utils.EthereumNetworkID),
			zap.Any("start_block", startBlock),
			zap.Any("end_block", endBlock),
		)
		return
	}

	latestBatchReceived := big.NewInt(0)
	totalBlocks := make(map[uint64]struct{})
	totalTxns := 0
	for {
		select {
		case cErr := <-bStream.Err():
			logger.L().Error(
				"failure to execute hyper client stream in range",
				zap.Error(cErr),
				zap.Any("network_id", utils.EthereumNetworkID),
				zap.Any("start_block", startBlock),
				zap.Any("end_block", endBlock),
			)
			return
		case response := <-bStream.Channel():
			logger.L().Info(
				"New stream logs response",
				zap.Any("start_block", startBlock),
				zap.Any("current_sync_block", response.NextBlock),
				zap.Any("end_block", endBlock),
				zap.Duration("current_processing_time", time.Since(startTime)),
			)
			latestBatchReceived = response.NextBlock

			totalTxns += len(response.GetLogs())
			for _, tx := range response.GetLogs() {
				totalBlocks[tx.BlockNumber.Uint64()] = struct{}{}
			}

			// Worker may close a done channel at any point in time after it receives all the payload.
			// Usually and depending on the payload size,
			// it takes around 5-100ms after stream is completed for messages to fully be delivered.
			// Instead of using time.Sleep(), we have Ack() mechanism that allows you finer worker closure management.
			// WARN: This is critical part of communication with stream and should always be used unless you have
			// disabled it via configuration. By default, ack is a must!
			bStream.Ack()

		case <-bStream.Done():
			logger.L().Info(
				"Stream request successfully completed",
				zap.Duration("duration", time.Since(startTime)),
				zap.Any("total_blocks", len(totalBlocks)),
				zap.Any("total_logs", totalTxns),
			)
			return
		case <-time.After(15 * time.Second):
			logger.L().Error(
				"expected ranges to receive at least one logs range in 15s",
				zap.Any("network_id", utils.EthereumNetworkID),
				zap.Any("start_block", startBlock),
				zap.Any("latest_batch_block_received", latestBatchReceived),
				zap.Any("end_block", endBlock),
			)
			return
		}
	}
}
