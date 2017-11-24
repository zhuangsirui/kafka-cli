package main

import (
	"fmt"
	"time"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name:   "consume",
		Usage:  "consume topic",
		Action: handleConsume,
		Flags: []cli.Flag{
			cli.Int64Flag{
				Name:  "partition, p",
				Usage: "partition id",
			},
			cli.StringFlag{
				Name:  "topic, t",
				Usage: "topic name",
			},
		},
	})
}

func handleConsume(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	var (
		partition = int32(c.Int64("partition"))
		offset    = c.Int64("offset")
		topic     = c.String("topic")
	)
	consumer, err := _cli.PartitionConsumer(topic, partition, offset)
	if err != nil {
		fmt.Printf("get partition consumer failed:\n%s\n", err)
		return nil
	}
	defer consumer.Close()
	var (
		resetSeconds = 5
		ticker       = time.NewTicker(time.Second)
		stoppedChan  = make(chan struct{})
	)
	go func() {
	FetchLoop:
		for {
			select {
			case msg := <-consumer.Messages():
				fmt.Println("message:", msg)
				resetSeconds = 5
			case err := <-consumer.Errors():
				fmt.Println("error:", err)
				resetSeconds = 5
			case <-ticker.C:
				resetSeconds--
				fmt.Printf("%d seconds remain to abort\n", resetSeconds)
				if resetSeconds <= 0 {
					close(stoppedChan)
					break FetchLoop
				}
			}
		}
	}()
	<-stoppedChan
	ticker.Stop()
	return nil
}
