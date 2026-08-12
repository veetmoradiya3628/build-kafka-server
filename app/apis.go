package main

type APIInfo struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

var apiVersions = []APIInfo{
	{APIKey: 18, MinVersion: 0, MaxVersion: 4}, // APIVersions
	{APIKey: 75, MinVersion: 0, MaxVersion: 0}, // DescribeTopicPartitions
}
