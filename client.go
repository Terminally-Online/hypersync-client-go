package hypersyncgo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	arrowhs "github.com/terminally-online/hypersync-client-go/arrow"
	"github.com/terminally-online/hypersync-client-go/options"
	parquetpkg "github.com/terminally-online/hypersync-client-go/parquet"
	"github.com/terminally-online/hypersync-client-go/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
)

type bearerTokenTransport struct {
	transport http.RoundTripper
	token     string
}

func (t *bearerTokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	return t.transport.RoundTrip(req)
}

type Client struct {
	ctx       context.Context
	opts      options.Node
	client    *http.Client
	rpcClient *ethclient.Client
}

func NewClient(ctx context.Context, opts options.Node) (*Client, error) {
	baseTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	var rpcTransport http.RoundTripper = baseTransport
	if opts.BearerToken != nil && *opts.BearerToken != "" {
		rpcTransport = &bearerTokenTransport{
			transport: baseTransport,
			token:     *opts.BearerToken,
		}
	}

	rpcHttpClient := &http.Client{
		Timeout:   2 * time.Minute,
		Transport: rpcTransport,
	}

	var rpcClient *ethclient.Client
	if opts.RpcEndpoint != "" {
		rpcConn, err := rpc.DialOptions(ctx, opts.RpcEndpoint, rpc.WithHTTPClient(rpcHttpClient))
		if err != nil {
			return nil, errors.Wrap(err, "failed to connect to RPC client")
		}
		rpcClient = ethclient.NewClient(rpcConn)
	}

	return &Client{
		ctx:    ctx,
		opts:   opts,
		client: rpcHttpClient,
		rpcClient: rpcClient,
	}, nil
}

func (c *Client) GetRPC() *ethclient.Client {
	return c.rpcClient
}

func (c *Client) addRequestHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if c.opts.BearerToken != nil && *c.opts.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+*c.opts.BearerToken)
	}
}

func (c *Client) GetQueryUrlFromNode(node options.Node) string {
	return strings.Join([]string{node.Endpoint, "query"}, "/")
}

func (c *Client) GeUrlFromNodeAndPath(node options.Node, path ...string) string {
	paths := append([]string{node.Endpoint}, path...)
	return strings.Join(paths, "/")
}

func (c *Client) Stream(ctx context.Context, query *types.Query, opts *options.StreamOptions) (*Stream, error) {
	stream, err := NewStream(ctx, c, query, opts)
	if err != nil {
		return nil, err
	}

	go func() {
		if sErr := stream.Subscribe(); sErr != nil {
			stream.QueueError(sErr)
			return
		}
	}()

	return stream, nil
}

func (c *Client) ArrowStream(ctx context.Context, query *types.Query, opts *options.StreamOptions) (*ArrowStream, error) {
	stream, err := NewArrowStream(ctx, c, query, opts)
	if err != nil {
		return nil, err
	}

	go func() {
		if sErr := stream.Subscribe(); sErr != nil {
			stream.QueueError(sErr)
			return
		}
	}()

	return stream, nil
}

func (c *Client) GetArrow(ctx context.Context, query *types.Query) (*types.QueryResponse, error) {
	base := c.opts.RetryBaseMs

	c.opts.RetryBackoffMs = time.Duration(100)
	c.opts.MaxNumRetries = 3

	var lastErr error
	for i := 0; i < c.opts.MaxNumRetries+1; i++ {
		response, err := DoArrow[*types.Query](ctx, c, c.GeUrlFromNodeAndPath(c.opts, "query", "arrow-ipc"), http.MethodPost, query)
		if err == nil {
			return response, nil
		}
		lastErr = err

		baseMs := base * time.Millisecond

		jitter := time.Duration(rand.Int63n(int64(c.opts.RetryBackoffMs))) * time.Millisecond

		select {
		case <-time.After(baseMs + jitter):
			base = min(base+c.opts.RetryBackoffMs, c.opts.RetryCeilingMs)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, errors.Wrapf(lastErr, "failed to get arrow data after retries: %d", c.opts.MaxNumRetries)
}

func DoQuery[R any, T any](ctx context.Context, c *Client, method string, payload R) (*T, error) {
	nodeUrl := c.GetQueryUrlFromNode(c.opts)

	reqPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal envio payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, nodeUrl, strings.NewReader(string(reqPayload)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new request")
	}

	c.addRequestHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to perform request")
	}
	defer resp.Body.Close()

	responseData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(responseData))
	}

	var result T
	err = json.Unmarshal(responseData, &result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}

	return &result, nil
}

func Do[R any, T any](ctx context.Context, c *Client, url string, method string, payload R) (*T, error) {
	reqPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal envio payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(reqPayload)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new request")
	}

	c.addRequestHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to perform request")
	}
	defer resp.Body.Close()

	responseData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(responseData))
	}

	var result T
	err = json.Unmarshal(responseData, &result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}

	return &result, nil
}

func DoArrow[R any](ctx context.Context, c *Client, url string, method string, payload R) (*types.QueryResponse, error) {
	reqPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal envio payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(reqPayload)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new request")
	}

	c.addRequestHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to perform request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseData, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(responseData))
	}

	responseData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}
	responseSize := uint64(len(responseData))

	arrowReader, err := arrowhs.NewQueryResponseReader(io.NopCloser(bytes.NewReader(responseData)))
	if err != nil {
		return nil, errors.Wrap(err, "could not parse the ipc/arrow response while attempting to read")
	}

	queryResponse := arrowReader.GetQueryResponse()
	queryResponse.ResponseSize = responseSize

	return queryResponse, nil
}

// DoArrowBatches executes a query and returns raw Arrow RecordBatches for Parquet writing.
func DoArrowBatches[R any](ctx context.Context, c *Client, url string, method string, payload R) (*arrowhs.ArrowResponse, error) {
	reqPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal envio payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(reqPayload)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new request")
	}

	c.addRequestHeaders(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to perform request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseData, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(responseData))
	}

	responseData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}
	responseSize := uint64(len(responseData))

	return arrowhs.ReadArrowBatches(io.NopCloser(bytes.NewReader(responseData)), responseSize)
}

// GetArrowBatches executes a query with retries and returns raw Arrow RecordBatches.
func (c *Client) GetArrowBatches(ctx context.Context, query *types.Query) (*arrowhs.ArrowResponse, error) {
	base := c.opts.RetryBaseMs

	c.opts.RetryBackoffMs = time.Duration(100)
	c.opts.MaxNumRetries = 3

	var lastErr error
	for i := 0; i < c.opts.MaxNumRetries+1; i++ {
		response, err := DoArrowBatches[*types.Query](ctx, c, c.GeUrlFromNodeAndPath(c.opts, "query", "arrow-ipc"), http.MethodPost, query)
		if err == nil {
			return response, nil
		}
		lastErr = err

		baseMs := base * time.Millisecond

		jitter := time.Duration(rand.Int63n(int64(c.opts.RetryBackoffMs))) * time.Millisecond

		select {
		case <-time.After(baseMs + jitter):
			base = min(base+c.opts.RetryBackoffMs, c.opts.RetryCeilingMs)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, errors.Wrapf(lastErr, "failed to get arrow batches after retries: %d", c.opts.MaxNumRetries)
}

// CollectParquet streams data from HyperSync and writes it to Parquet files.
// The path should be a directory where parquet files will be created.
// Creates: blocks.parquet, transactions.parquet, logs.parquet, traces.parquet
func (c *Client) CollectParquet(ctx context.Context, query *types.Query, path string, config *parquetpkg.CollectConfig) error {
	if config == nil {
		config = parquetpkg.DefaultCollectConfig()
	}

	stream, err := c.ArrowStream(ctx, query, config.StreamOptions)
	if err != nil {
		return errors.Wrap(err, "failed to create arrow stream for parquet collection")
	}
	defer stream.Unsubscribe()

	return parquetpkg.Collect(stream, query.ToBlock.Uint64(), path, config)
}

// Collect fetches all data for a query and accumulates it into a single QueryResponse.
// It handles pagination automatically, fetching until the entire requested block range is covered.
func (c *Client) Collect(ctx context.Context, query *types.Query) (*types.QueryResponse, error) {
	toBlock := query.ToBlock
	currentQuery := *query

	var combinedResponse *types.QueryResponse
	var totalResponseSize uint64

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := c.GetArrow(ctx, &currentQuery)
		if err != nil {
			return nil, errors.Wrap(err, "failed to fetch data during collect")
		}

		totalResponseSize += resp.ResponseSize

		if combinedResponse == nil {
			combinedResponse = resp
		} else {
			combinedResponse.Data.Blocks = append(combinedResponse.Data.Blocks, resp.Data.Blocks...)
			combinedResponse.Data.Transactions = append(combinedResponse.Data.Transactions, resp.Data.Transactions...)
			combinedResponse.Data.Logs = append(combinedResponse.Data.Logs, resp.Data.Logs...)
			combinedResponse.Data.Traces = append(combinedResponse.Data.Traces, resp.Data.Traces...)
			combinedResponse.NextBlock = resp.NextBlock
			combinedResponse.ArchiveHeight = resp.ArchiveHeight
			if resp.RollbackGuard != nil {
				combinedResponse.RollbackGuard = resp.RollbackGuard
			}
		}

		if resp.NextBlock.Cmp(toBlock) >= 0 {
			break
		}

		currentQuery.FromBlock = resp.NextBlock
	}

	if combinedResponse != nil {
		combinedResponse.ResponseSize = totalResponseSize
	}
	return combinedResponse, nil
}

// CollectArrow fetches all data for a query and accumulates raw Arrow batches into a single ArrowResponse.
// It handles pagination automatically. The caller is responsible for calling Release() on the returned
// ArrowResponse.Batches when done to free Arrow memory.
func (c *Client) CollectArrow(ctx context.Context, query *types.Query) (*arrowhs.ArrowResponse, error) {
	toBlock := query.ToBlock
	currentQuery := *query

	var combinedResponse *arrowhs.ArrowResponse
	var totalResponseSize uint64

	for {
		select {
		case <-ctx.Done():
			if combinedResponse != nil {
				combinedResponse.Batches.Release()
			}
			return nil, ctx.Err()
		default:
		}

		resp, err := c.GetArrowBatches(ctx, &currentQuery)
		if err != nil {
			if combinedResponse != nil {
				combinedResponse.Batches.Release()
			}
			return nil, errors.Wrap(err, "failed to fetch arrow batches during collect")
		}

		totalResponseSize += resp.ResponseSize

		if combinedResponse == nil {
			combinedResponse = resp
		} else {
			combinedResponse.Batches.Blocks = append(combinedResponse.Batches.Blocks, resp.Batches.Blocks...)
			combinedResponse.Batches.Transactions = append(combinedResponse.Batches.Transactions, resp.Batches.Transactions...)
			combinedResponse.Batches.Logs = append(combinedResponse.Batches.Logs, resp.Batches.Logs...)
			combinedResponse.Batches.Traces = append(combinedResponse.Batches.Traces, resp.Batches.Traces...)
			combinedResponse.NextBlock = resp.NextBlock
			combinedResponse.ArchiveHeight = resp.ArchiveHeight
			if resp.RollbackGuard != nil {
				combinedResponse.RollbackGuard = resp.RollbackGuard
			}
		}

		if resp.NextBlock.Cmp(toBlock) >= 0 {
			break
		}

		currentQuery.FromBlock = resp.NextBlock
	}

	if combinedResponse != nil {
		combinedResponse.ResponseSize = totalResponseSize
	}
	return combinedResponse, nil
}
