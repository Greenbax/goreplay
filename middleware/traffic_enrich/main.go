package main

import (
	// "bufio"
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"zip/infra/traffic_enrich/logs"

	"github.com/buger/goreplay/proto"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/printer"
)

func main() {
	s3Loader := newS3Loader()
	for {
		buf := make([]byte, 0, 1024*1024)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(buf, 60*1024*1024) // initial 1MB, max 60MB.
		logs.Info("Traffic enrichment starts.")
		for scanner.Scan() {
			encoded := scanner.Bytes()
			buf := make([]byte, len(encoded)/2)
			hex.Decode(buf, encoded)

			t := time.Now()
			process(buf, s3Loader)
			DDClient.Histogram(
				"traffic_replay.latency",
				float64(time.Since(t))/1e9,
				[]string{"type:total"},
				1,
			)
		}
		logs.Error("Traffic enrichment stops. Something must be wrong: ", scanner.Err())
	}
}

func process(buf []byte, s3Loader *s3Loader) {
	// First byte indicate payload type, possible values:
	//  1 - Request
	//  2 - Response
	//  3 - ReplayedResponse
	payloadType := buf[0]
	headerSize := bytes.IndexByte(buf, '\n') + 1
	header := buf[:headerSize-1]

	// Header contains four values separated by space:
	// 1. request type
	// 2. request id
	// 3. request start time (or round-trip time for responses)
	// 4. duration in nano seconds
	// see https://github.com/dingxiong/goreplay/blob/d440b3dc8f2800b8147cd968f68aa10ec8b72e3b/input_raw.go#L101
	meta := bytes.Split(header, []byte(" "))
	if len(meta) != 4 {
		logs.Error("Bad header", meta)
		return
	}

	requestTimeNanoseconds, err := strconv.ParseInt(string(meta[2]), 10, 64)
	if err != nil {
		logs.Error("Fail to convert", string(meta[2]), "to int64")
		return
	}

	payload := buf[headerSize:]

	logs.Debug("Received payload:", string(buf))
	DDClient.Incr("traffic_replay.count", []string{"type:total"}, 1)
	switch payloadType {
	case '1': // Request
		url := proto.Path(payload)
		logs.Debug(string(url))
		if bytes.Equal(url, []byte("/graphql")) {
			p, err := GetGrapqhlPayload(payload)
			if err != nil {
				logs.Debug(err)
				return
			}
			if !p.IsQuery {
				logs.Debug("Ignore write traffic")
				return
			}
			if !rateLimitAllowed(p.Operation, AppSettings.RequestHourlyRateLimit) {
				logs.Debug("Drop request", p.Operation, "as it reach the limit", AppSettings.RequestHourlyRateLimit)
				return
			}
			newPayload := proto.SetHeader(
				payload,
				[]byte("Canonical-Resource"),
				[]byte(p.Operation),
			)
			buf = append(buf[:headerSize], newPayload...)

			// Emitting data back
			os.Stdout.Write(encode(buf))

			// save request to s3
			s3Loader.Enqueue(newPayload, requestTimeNanoseconds)

			DDClient.Incr("traffic_replay.count", []string{"type:request"}, 1)
		}
	case '2': // Original response
		DDClient.Incr("traffic_replay.count", []string{"type:original_response"}, 1)
	case '3': // Replayed response
		DDClient.Incr("traffic_replay.count", []string{"type:replayed_response"}, 1)
	default:
	}
}

func encode(buf []byte) []byte {
	dst := make([]byte, len(buf)*2+1)
	hex.Encode(dst, buf)
	dst[len(dst)-1] = '\n'

	return dst
}

type postData struct {
	Query     string                 `json:"query"`
	Operation string                 `json:"operationName"`
	Variables map[string]interface{} `json:"variables"`
}

type GraphQLPayLoad struct {
	postData
	IsQuery bool // is query or mutation
}

func GetGrapqhlPayload(payload []byte) (*GraphQLPayLoad, error) {
	var p postData
	if err := json.Unmarshal(proto.Body(payload), &p); err != nil {
		fmt.Fprintf(os.Stderr, "Fail to decode payload to json %v\n", string(payload))
		return nil, err
	}

	result := GraphQLPayLoad{postData: p}
	astDoc, err := parser.Parse(parser.ParseParams{Source: result.Query})
	if err != nil {
		return nil, err
	}
	logs.Debug(printer.Print(astDoc))

	for _, definition := range astDoc.Definitions {
		switch definition := definition.(type) {
		case *ast.OperationDefinition:
			if definition.Operation == "query" {
				result.IsQuery = true
				break
			}
		case *ast.FragmentDefinition:
		default:
			return nil, fmt.Errorf("GraphQL cannot execute a request containing a %v", definition.GetKind())
		}
	}

	return &result, nil
}
