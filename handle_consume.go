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
			cli.StringFlag{
				Name:  "topic, t",
				Usage: "topic name",
			},
			cli.Int64Flag{
				Name:  "partition, p",
				Usage: "partition id",
			},
			cli.Int64Flag{
				Name:  "offset, o",
				Usage: "set the offset where to start to consume, -1 for newest and -2 for oldest",
			},
			cli.StringFlag{
				Name:  "output",
				Usage: "assign a file to print, empty means stdout",
			},
			cli.StringSliceFlag{
				Name:  "fields, f",
				Usage: "select fields of message, the available fields are: key, value, offset, timestamp",
				Value: &cli.StringSlice{"key", "value", "offset"},
			},
			cli.StringFlag{
				Name:  "fmt",
				Usage: "how to show message, as json or csv(not implemented)",
				Value: "json",
			},
			cli.StringFlag{
				Name:  "fmt.key",
				Usage: "how to show message's key, as string or bytes",
				Value: "string",
			},
			cli.StringFlag{
				Name:  "fmt.value",
				Usage: "how to show message's value, as string or bytes",
				Value: "string",
			},
		},
	})
}

const countdown = 1

func handleConsume(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	packer, err := newPacker(
		c.StringSlice("fields"),
		c.String("fmt"),
		c.Bool("isBinaryKey"),
		c.Bool("isBinaryValue"),
	)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	consumer, err := _cli.PartitionConsumer(
		c.String("topic"),
		int32(c.Int64("partition")),
		c.Int64("offset"),
	)
	if err != nil {
		fmt.Printf("get partition consumer failed:\n%s\n", err)
		return nil
	}
	defer consumer.Close()
	writer, err := newWriter(c.String("output"))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer writer.close()
	var (
		counter     = countdown
		ticker      = time.NewTicker(time.Second)
		stoppedChan = make(chan struct{})
	)
	go func() {
	FetchLoop:
		for {
			select {
			case msg := <-consumer.Messages():
				data, err := packer.pack(msg)
				if err != nil {
					close(stoppedChan)
					break FetchLoop
				}
				writer.write(data)
				writer.write([]byte("\n"))
				counter = countdown
			case err := <-consumer.Errors():
				fmt.Println("error:", err)
			case <-ticker.C:
				counter--
				fmt.Printf("countdown: %d seconds remain to abort\n", counter)
				if counter <= 0 {
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
