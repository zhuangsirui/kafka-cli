package main

import (
	"fmt"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name:   "produce",
		Usage:  "produce message",
		Action: handleProduce,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "key, k",
				Usage: "key for message",
			},
			cli.StringFlag{
				Name:  "value, v",
				Usage: "value for message",
			},
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

func handleProduce(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	var (
		key       = c.String("key")
		value     = c.String("value")
		topic     = c.String("topic")
		partition = int32(c.Int64("partition"))
	)
	offset, err := _cli.Produce(topic, partition, []byte(key), []byte(value))
	if err != nil {
		fmt.Printf("produce failed:\n%s\n", err)
		return nil
	}
	fmt.Printf("produce successful, offset %d\n", offset)
	return nil
}
