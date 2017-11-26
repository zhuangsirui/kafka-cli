package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
)

type packer struct {
	fields        []string
	format        string
	isBinaryKey   bool
	isBinaryValue bool

	_message *message
}

func newPacker(fields []string, format string, isBinaryKey, isBinaryValue bool) (*packer, error) {
	switch format {
	case "json":
		// ignore
	default:
		return nil, fmt.Errorf("arg fmt not supported")
	}
	return &packer{
		fields:        fields,
		format:        format,
		isBinaryKey:   isBinaryKey,
		isBinaryValue: isBinaryValue,
	}, nil
}

func (p *packer) pack(msg *sarama.ConsumerMessage) ([]byte, error) {
	if p._message == nil {
		p._message = new(message)
	}
	p._message.reset()
	for _, field := range p.fields {
		switch field {
		case "key":
			p._message.addKey(msg.Key, p.isBinaryKey)
		case "value":
			p._message.addValue(msg.Value, p.isBinaryValue)
		case "offset":
			p._message.addOffset(msg.Offset)
		case "timestamp":
			p._message.addTimestamp(msg.Timestamp)
		}
	}
	out, err := p.fmt()
	return out, err
}

func (p *packer) fmt() (out []byte, err error) {
	switch p.format {
	case "json":
		out, err = json.Marshal(p._message)
	}
	return
}
