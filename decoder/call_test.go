package decoder

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestNewCallDecoderFromSignatures(t *testing.T) {
	testCases := []struct {
		name       string
		signatures []string
		wantErr    bool
	}{
		{
			name: "Basic signatures",
			signatures: []string{
				"transfer(address,uint256)",
				"approve(address,uint256)",
				"balanceOf(address)",
			},
			wantErr: false,
		},
		{
			name: "Complex signatures",
			signatures: []string{
				"swap(uint256,uint256,address[],address,uint256)",
				"multicall(bytes[])",
			},
			wantErr: false,
		},
		{
			name:       "Invalid signature",
			signatures: []string{"invalid"},
			wantErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoder, err := NewCallDecoderFromSignatures(tc.signatures)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, decoder)
		})
	}
}

func TestGetSelector(t *testing.T) {
	testCases := []struct {
		signature string
		expected  string
	}{
		{
			signature: "transfer(address,uint256)",
			expected:  "a9059cbb",
		},
		{
			signature: "approve(address,uint256)",
			expected:  "095ea7b3",
		},
		{
			signature: "balanceOf(address)",
			expected:  "70a08231",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.signature, func(t *testing.T) {
			selector := GetSelector(tc.signature)
			got := common.Bytes2Hex(selector[:])
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestDecodeInput(t *testing.T) {
	decoder, err := NewCallDecoderFromSignatures([]string{
		"transfer(address,uint256)",
	})
	require.NoError(t, err)

	// Build transfer calldata: transfer(0x1234...5678, 1000)
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	amount := big.NewInt(1000)

	// Selector for transfer(address,uint256)
	selector := common.Hex2Bytes("a9059cbb")
	// Pad address to 32 bytes
	toBytes := common.LeftPadBytes(to.Bytes(), 32)
	// Pad amount to 32 bytes
	amountBytes := common.LeftPadBytes(amount.Bytes(), 32)

	calldata := append(selector, toBytes...)
	calldata = append(calldata, amountBytes...)

	decoded, err := decoder.DecodeInput(calldata)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, "transfer", decoded.Name)
	require.Equal(t, to, decoded.Inputs["arg0"])

	decodedAmount, ok := decoded.Inputs["arg1"].(*big.Int)
	require.True(t, ok)
	require.Equal(t, amount.Int64(), decodedAmount.Int64())
}

func TestDecodeInputUnknownSelector(t *testing.T) {
	decoder, err := NewCallDecoderFromSignatures([]string{
		"transfer(address,uint256)",
	})
	require.NoError(t, err)

	// Unknown selector
	calldata := common.Hex2Bytes("12345678")
	decoded, err := decoder.DecodeInput(calldata)
	require.NoError(t, err)
	require.Nil(t, decoded)
}

func TestDecodeInputTooShort(t *testing.T) {
	decoder, err := NewCallDecoderFromSignatures([]string{
		"transfer(address,uint256)",
	})
	require.NoError(t, err)

	// Too short - less than 4 bytes
	calldata := common.Hex2Bytes("a905")
	decoded, err := decoder.DecodeInput(calldata)
	require.NoError(t, err)
	require.Nil(t, decoded)
}

func TestDecodeOutput(t *testing.T) {
	decoder := NewCallDecoder()

	// Encode a uint256 value
	amount := big.NewInt(1000000)
	data := common.LeftPadBytes(amount.Bytes(), 32)

	decoded, err := decoder.DecodeOutput("(uint256)", data)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Len(t, decoded.Outputs, 1)

	decodedAmount, ok := decoded.Outputs[0].(*big.Int)
	require.True(t, ok)
	require.Equal(t, amount.Int64(), decodedAmount.Int64())
}

func TestNewCallDecoderFromABI(t *testing.T) {
	abiJSON := `[
		{
			"type": "function",
			"name": "transfer",
			"inputs": [
				{"name": "to", "type": "address"},
				{"name": "amount", "type": "uint256"}
			]
		},
		{
			"type": "function",
			"name": "balanceOf",
			"inputs": [
				{"name": "account", "type": "address"}
			],
			"outputs": [
				{"name": "", "type": "uint256"}
			]
		}
	]`

	decoder, err := NewCallDecoderFromABI(abiJSON)
	require.NoError(t, err)
	require.NotNil(t, decoder)
	require.Len(t, decoder.methods, 2)
}
