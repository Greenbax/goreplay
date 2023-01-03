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
	Path        string               `json:"path"`
	Method      string               `json:"method"`
	Headers     textproto.MIMEHeader `json:"headers"`
	Body        string               `json:"body"`
	RequestTime int64                `json:"request_time"` // nano seconds
}

type s3Loader struct {
	RequestsBuffer []httpRequest
	AwsSession     *session.Session
}

func newS3Loader() *s3Loader {
	s, err := session.NewSession(&aws.Config{
		Region: aws.String(AppSettings.AwsRegion),
		Credentials: credentials.NewStaticCredentials(
			AppSettings.AwsAccessKeyId,
			AppSettings.AwsSecretAccessKey,
			"",
		),
	},
	)
	if err != nil {
		logs.Fatal(err)
	}
	loader := &s3Loader{RequestsBuffer: make([]httpRequest, 0), AwsSession: s}
	return loader
}

func (l *s3Loader) Enqueue(payload []byte, requestTimeNanoseconds int64) {
	r := httpRequest{
		Path:        string(proto.Path(payload)),
		Method:      string(proto.Method(payload)),
		Headers:     proto.ParseHeaders(payload),
		Body:        string(proto.Body(payload)),
		RequestTime: requestTimeNanoseconds,
	}
	l.RequestsBuffer = append(l.RequestsBuffer, r)
	if len(l.RequestsBuffer) >= AppSettings.S3BatchLoadSize {
		requestsCopied := make([]httpRequest, len(l.RequestsBuffer))
		copy(requestsCopied, l.RequestsBuffer)
		l.RequestsBuffer = l.RequestsBuffer[:0] // clear the slice
		go upload(l.AwsSession, requestsCopied)
	}
}

func getS3Uploader(s *session.Session) *s3manager.Uploader {
	uploader := s3manager.NewUploader(s)
	return uploader

}

func upload(s *session.Session, requests []httpRequest) {
	requestStrs := make([]string, 0)
	for _, r := range requests {
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

	uploader := getS3Uploader(s)
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
