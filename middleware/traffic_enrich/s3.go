package main

import (
	"fmt"
	"math/rand"
	"net/textproto"
	"os"
	"strconv"
	"encoding/json"
	"strings"
	"time"
	"zip/infra/traffic_enrich/logs"

	"github.com/buger/goreplay/proto"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type httpRequest struct {
	Path     []byte               `json:"path"`
	Method  []byte               `json:"method"`
	Headers textproto.MIMEHeader `json:"headers"`
	Body    []byte               `json:"body"`
}

type s3Loader struct {
	RequestsBuffer [][]byte
	batchLoadSize  int
	// client *s3.S3
	bucket string
	prefix string
	// uploader *s3manager.Uploader
}

func newS3Loader() *s3Loader {
	batchLoadSize := 1000
	batchLoadSizeStr, found := os.LookupEnv("BATCH_LOAD_SIZE")
	if found {
		var err error
		batchLoadSize, err = strconv.Atoi(batchLoadSizeStr)
		if err != nil {
			logs.Error("Fail to convert %s to integer.", batchLoadSizeStr)
			panic(err)
		}
	}
	loader := &s3Loader{RequestsBuffer: make([][]byte, 0), batchLoadSize: batchLoadSize}
	return loader
}

func (l *s3Loader) Enqueue(req []byte) {
	l.RequestsBuffer = append(l.RequestsBuffer, req)
	if len(l.RequestsBuffer) >= l.batchLoadSize {
		requestsCopied := make([][]byte, len(l.RequestsBuffer))
		copy(requestsCopied, l.RequestsBuffer)
		l.RequestsBuffer = l.RequestsBuffer[:0] // clear the slice
		go l.save(requestsCopied)
	}
}

func (l *s3Loader) getS3uploader() *s3manager.Uploader {
	session, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)
	if err != nil {
		logs.Fatal(err)
	}
	uploader := s3manager.NewUploader(session)
	return uploader

}

func (l *s3Loader) save(requests [][]byte) {
	requestStrs := make([]string, 0)
	for _, item := range(requests) {
		r := httpRequest{Path: proto.Path(item), Method: proto.Method(item), Headers: proto.GetHeaders(item), Body: proto.Body(item)}
		s, err := json.Marshal(r)
		if err != nil {
			logs.Error("Fail to marshal request %v", r)
			continue
		}
		requestStrs = append(requestStrs, string(s))
	}
	body := strings.Join(requestStrs, "\n")
	t := time.Now().Format("20060102150405")
	filename := fmt.Sprintf("%s/requests-%s-%d.txt", l.prefix, t, rand.Intn(1000))

	uploader := l.getS3uploader()
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(filename),
		Body:   strings.NewReader(body),
	})
	if err != nil {
		logs.Error("Unable to upload %q to %q, %v", filename, l.bucket, err)
	} else {
		logs.Info("Successfully uploaded %d to %q\n", filename, l.bucket)
	}
}
