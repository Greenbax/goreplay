package main

import (
	"os"
	"strconv"

	"zip/infra/traffic_enrich/logs"
)

// Settings are a set of configurations loaded from environment variables.
type Settings struct {
	AwsRegion          string
	AwsAccessKeyId     string
	AwsSecretAccessKey string
	S3BucketName       string
	S3BucketPrefix     string
	S3BatchLoadSize    int
	TrafficSampleRate  float32
}

func lookupEnvOrDefault(key string, defaultValue string) string {
	v, found := os.LookupEnv(key)
	if found {
		return v
	}
	return defaultValue
}

func lookupEnvOrExit(key string) string {
	v, found := os.LookupEnv(key)
	if !found {
		logs.Fatal("Environment variable", key, "does not exits.")
	}
	return v
}

func newSettings() *Settings {
	st := &Settings{}

	st.AwsRegion = lookupEnvOrDefault("AWS_REGION", "us-east-2")
	st.AwsAccessKeyId = lookupEnvOrExit("TRAFFIC_REPLAY_AWS_ACCESS_KEY_ID")
	st.AwsSecretAccessKey = lookupEnvOrExit("TRAFFIC_REPLAY_AWS_SECRET_ACCESS_KEY")

	st.S3BucketName = lookupEnvOrDefault("TRAFFIC_REPLAY_BUCKET_NAME", "zip-traffic-replay")
	st.S3BucketPrefix = lookupEnvOrDefault("TRAFFIC_REPLAY_BUCKET_PREFIX", "test")

	batchLoadSizeStr := lookupEnvOrDefault("BATCH_LOAD_SIZE", "1000")
	batchLoadSize, err := strconv.Atoi(batchLoadSizeStr)
	if err != nil {
		logs.Fatal("Fail to convert", batchLoadSize, "to integer.")
	}
	st.S3BatchLoadSize = batchLoadSize

	trafficSampleRateStr := lookupEnvOrDefault("TRAFFIC_SAMPLE_RATE", "1.0")
	trafficSampleRate, err := strconv.ParseFloat(trafficSampleRateStr, 32)
	if err != nil {
		logs.Fatal("Fail to convert", trafficSampleRateStr, "to float32.")
	}
	st.TrafficSampleRate = float32(trafficSampleRate)

	return st
}

var AppSettings *Settings

func init() {
	AppSettings = newSettings()
}
