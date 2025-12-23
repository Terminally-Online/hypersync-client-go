//go:build ignore
// +build ignore

package main

import (
	"context"
	"math/big"
	"os"

	hypersyncgo "github.com/enviodev/hypersync-client-go"
	"github.com/enviodev/hypersync-client-go/logger"
	"github.com/enviodev/hypersync-client-go/options"
	"github.com/enviodev/hypersync-client-go/types"
	"github.com/enviodev/hypersync-client-go/utils"
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
		logger.L().Error("failed to create hyper client", zap.Error(err))
		return
	}

	client, found := hsClient.GetClient(utils.EthereumNetworkID)
	if !found {
		logger.L().Error("failure to discover hyper client")
		return
	}

	startBlock := big.NewInt(20000000)
	endBlock := big.NewInt(20000100)

	logger.L().Info(
		"Starting parquet collection",
		zap.Any("start_block", startBlock),
		zap.Any("end_block", endBlock),
	)

	query := &types.Query{
		FromBlock: startBlock,
		ToBlock:   endBlock,
		Logs: []types.LogSelection{
			{
				Topics: [][]common.Hash{
					{
						common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					},
				},
			},
		},
		FieldSelection: types.FieldSelection{
			Block:       []string{"number", "timestamp", "hash"},
			Transaction: []string{"from", "to", "value", "block_number", "hash"},
			Log:         []string{"address", "data", "topic0", "topic1", "topic2", "topic3", "block_number", "transaction_hash"},
		},
	}

	outputPath := "./parquet_output"

	err = client.CollectParquet(ctx, query, outputPath, options.DefaultStreamOptions())
	if err != nil {
		logger.L().Error("failed to collect parquet data", zap.Error(err))
		return
	}

	logger.L().Info(
		"Parquet collection completed successfully",
		zap.String("output_path", outputPath),
	)
}
