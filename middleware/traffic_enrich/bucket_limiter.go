package main

import (
    "time"
)
// A simple rate limit algorithm.
// Using a bucket to make sure every request can have at most x requests per hour.

var requestHourlyCounts map[string]int = make(map[string]int)
var theHour= -1

func rateLimitAllowed(resource string, rateLimit int) bool {
	hour := time.Now().Hour()
	if theHour != hour {
		theHour = hour
		requestHourlyCounts = make(map[string]int)
	}
	count := requestHourlyCounts[resource]
	if count > rateLimit {
		return false
	}
	requestHourlyCounts[resource] = count + 1
	return true
}
