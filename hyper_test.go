package hypersyncgo

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/terminally-online/hypersync-client-go/options"
	"github.com/terminally-online/hypersync-client-go/utils"
)

func TestHyperSync(t *testing.T) {
	testCases := []struct {
		name      string
		opts      options.Options
		networkId utils.NetworkID
		addresses []common.Address
	}{{
		name:      "Test Hyper Ethereum Client",
		opts:      getTestOptions(),
		networkId: utils.EthereumNetworkID,
		addresses: []common.Address{
			common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
		},
	}}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			hsClient, err := NewHyper(ctx, testCase.opts)
			require.NoError(t, err)
			require.NotNil(t, hsClient)

			client, found := hsClient.GetClient(testCase.networkId)
			require.True(t, found)
			require.NotNil(t, client)
		})
	}
}
