package hypersyncgo

import (
	"os"

	"github.com/terminally-online/hypersync-client-go/options"
	"github.com/terminally-online/hypersync-client-go/utils"
	"go.uber.org/zap"
)

func getTestBearerToken() *string {
	token := os.Getenv("HYPERSYNC_BEARER_TOKEN")
	if token == "" {
		return nil
	}
	return &token
}

func getTestOptions() options.Options {
	return options.Options{
		LogLevel: zap.DebugLevel,
		Blockchains: []options.Node{
			{
				Type:        utils.EthereumNetwork,
				NetworkId:   utils.EthereumNetworkID,
				Endpoint:    "https://eth.hypersync.xyz",
				RpcEndpoint: "https://eth.rpc.hypersync.xyz",
				BearerToken: getTestBearerToken(),
			},
		},
	}
}
