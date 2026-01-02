package config

import "time"

type TopicConfig struct {
	MaxMessage int
	MaxBytes   int64
	TimeToAck  time.Duration
	TTL        time.Duration
}
