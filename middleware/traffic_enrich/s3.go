package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/textproto"
	"strings"
	"time"
	"zip/infra/traffic_enrich/logs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/buger/goreplay/proto"
)

type httpRequest struct {
	Path    string               `json:"path"`
	Method  string               `json:"method"`
	Headers textproto.MIMEHeader `json:"headers"`
	Body    string               `json:"body"`
}

type s3Loader struct {
	RequestsBuffer [][]byte
}

func newS3Loader() *s3Loader {
	loader := &s3Loader{RequestsBuffer: make([][]byte, 0)}
	return loader
}

func (l *s3Loader) Enqueue(payload []byte) {
	l.RequestsBuffer = append(l.RequestsBuffer, payload)
	if len(l.RequestsBuffer) >= AppSettings.S3BatchLoadSize {
		requestsCopied := make([][]byte, len(l.RequestsBuffer))
		copy(requestsCopied, l.RequestsBuffer)
		l.RequestsBuffer = l.RequestsBuffer[:0] // clear the slice
		go upload(requestsCopied)
	}
}

func getS3Uploader() *s3manager.Uploader {
	session, err := session.NewSession(&aws.Config{
		Region:      aws.String(AppSettings.AwsRegion),
		Credentials: credentials.NewStaticCredentials(AppSettings.AwsAccessKeyId, AppSettings.AwsSecretAccessKey, ""),
	},
	)
	if err != nil {
		logs.Fatal(err)
	}
	uploader := s3manager.NewUploader(session)
	return uploader

}

func upload(requests [][]byte) {
	requestStrs := make([]string, 0)
	for _, payload := range requests {
		r := httpRequest{
			Path:    string(proto.Path(payload)),
			Method:  string(proto.Method(payload)),
			Headers: proto.ParseHeaders(payload),
			Body:    string(proto.Body(payload)),
		}
		s, err := json.Marshal(r)
		if err != nil {
			logs.Error("Fail to marshal request", r)
			continue
		}
		requestStrs = append(requestStrs, string(s))
	}
	body := strings.Join(requestStrs, "\n")
	t := time.Now().Format("20060102150405")
	filename := fmt.Sprintf("%s/requests-%s-%d.txt", AppSettings.S3BucketPrefix, t, rand.Intn(1000))

	uploader := getS3Uploader()
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(AppSettings.S3BucketName),
		Key:    aws.String(filename),
		Body:   strings.NewReader(body),
	})
	if err != nil {
		logs.Error("Unable to upload", filename, "to", AppSettings.S3BucketName, ". error is", err)
	} else {
		logs.Info("Successfully uploaded", filename, "to", AppSettings.S3BucketName)
	}
}
