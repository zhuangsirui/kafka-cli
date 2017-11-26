package main

import (
	"fmt"
	"kafka-cli/client"

	"github.com/Shopify/sarama"
)

type state struct {
	cli       *client.Client
	topic     *string
	partition *int32
}

var _state = new(state)

func init() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	_state.cli = client.New(config)
}

func (s *state) String() (out string) {
	var connected, state, topic, partition string
	if s.cli.Connected() {
		connected = "connected"
	} else {
		connected = "disconnected"
	}
	if s.topic != nil {
		topic = *s.topic
	}
	if s.partition != nil {
		partition = fmt.Sprintf("%d", *s.partition)
	}
	if topic != "" || partition != "" {
		state = fmt.Sprintf(" [%s@%s]", topic, partition)
	}
	return connected + state
}

func (s *state) setTopic(topic string) {
	s.topic = &topic
}

func (s *state) setPartition(partition int32) {
	s.partition = &partition
}
