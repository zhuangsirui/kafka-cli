package main

import (
	"kafka-cli/client"

	"github.com/Shopify/sarama"
)

var (
	_cli *client.Client
)

func init() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	_cli = client.New(config)
}
