package client

import (
	"fmt"

	"github.com/Shopify/sarama"
)

const pkgName = "kafka-cli/client"

type Client struct {
	config      *sarama.Config
	brokerAddrs []string

	client sarama.Client

	syncProducer sarama.SyncProducer
	consumer     sarama.Consumer
}

func New(config *sarama.Config) *Client {
	return &Client{
		config: config,
	}
}

func (c Client) States() (states []string) {
	states = append(states, fmt.Sprintf("connected: %t", c.Connected()))
	states = append(states, fmt.Sprintf("broker addrs: %v", c.BrokerAddrs()))
	return
}

func (c Client) Connected() bool {
	return c.client != nil && !c.client.Closed()
}

func (c Client) BrokerAddrs() []string {
	return c.brokerAddrs
}

func (c *Client) SetConfig(config *sarama.Config) {
	c.config = config
}

func (c *Client) Connect(brokerAddrs []string) (err error) {
	if brokerAddrs != nil {
		c.brokerAddrs = brokerAddrs
	}
	c.client, err = sarama.NewClient(c.brokerAddrs, c.config)
	return
}

func (c *Client) Disconnect() (errs []error) {
	if c.client != nil {
		errs = append(errs, c.client.Close())
	}
	if c.consumer != nil {
		errs = append(errs, c.consumer.Close())
	}
	if c.syncProducer != nil {
		errs = append(errs, c.syncProducer.Close())
	}
	return
}

func (c *Client) Reset() {
	c.Disconnect()
	c.brokerAddrs = nil
	c.client = nil
}

func (c *Client) Topics() (topics []string, err error) {
	if c.client != nil {
		topics, err = c.client.Topics()
	}
	return
}

func (c *Client) Partitions(topic string) (partitions []int32, err error) {
	if c.client != nil {
		partitions, err = c.client.Partitions(topic)
	}
	return
}

func (c *Client) Offset(topic string, partition int32, time int64) (offset int64, err error) {
	if c.client != nil {
		offset, err = c.client.GetOffset(topic, partition, time)
	}
	return
}

func (c *Client) Produce(topic string, partition int32, key, value []byte) (offset int64, err error) {
	if c.client == nil {
		return
	}
	var producer sarama.SyncProducer
	if producer, err = c.getSyncProducer(); err != nil {
		return
	}
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
	}
	if partition, offset, err = producer.SendMessage(msg); err != nil {
		err = fmt.Errorf("%s: send message failed: %s", pkgName, err)
	}
	return
}

func (c *Client) getSyncProducer() (producer sarama.SyncProducer, err error) {
	if c.client == nil {
		err = fmt.Errorf("%s: kafka brokers not connected", pkgName)
		return
	}
	if producer = c.syncProducer; producer == nil {
		if producer, err = sarama.NewSyncProducerFromClient(c.client); err != nil {
			err = fmt.Errorf("%s: new sync producer failed: %s", pkgName, err)
			return
		}
	}
	return
}

func (c *Client) PartitionConsumer(topic string, partition int32, offset int64) (partitionConsumer sarama.PartitionConsumer, err error) {
	var consumer sarama.Consumer
	if consumer, err = c.getConsumer(); err != nil {
		return
	}
	if partitionConsumer, err = consumer.ConsumePartition(topic, partition, offset); err != nil {
		err = fmt.Errorf("%s: get partition consumer failed: %s", pkgName, err)
	}
	return
}

func (c *Client) getConsumer() (consumer sarama.Consumer, err error) {
	if c.client == nil {
		err = fmt.Errorf("%s: kafka brokers not connected", pkgName)
		return
	}
	if consumer = c.consumer; consumer == nil {
		if consumer, err = sarama.NewConsumerFromClient(c.client); err != nil {
			err = fmt.Errorf("%s: new consumer failed: %s", pkgName, err)
		}
	}
	return
}
