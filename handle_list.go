package main

import (
	"fmt"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name: "list",
		Subcommands: []cli.Command{
			{
				Name:   "topics",
				Usage:  "list topics in kafka",
				Action: handleListTopics,
			},
			{
				Name:   "partitions",
				Usage:  "list topic partitions",
				Action: handleListPartitions,
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "topic, t",
						Usage: "topic name",
					},
				},
			},
		},
	})
}

func handleListTopics(c *cli.Context) error {
	if ok := checkConnect(c.GlobalString("addrs")); !ok {
		return nil
	}
	topics, err := _state.cli.Topics()
	if err != nil {
		fmt.Printf("list topics failed:\n%s\n", err)
		return nil
	}
	for _, topic := range topics {
		fmt.Println(topic)
	}
	return nil
}

func handleListPartitions(c *cli.Context) error {
	if ok := checkConnect(c.GlobalString("addrs")); !ok {
		return nil
	}
	topic := c.String("topic")
	if topic == "" && _state.topic != nil {
		topic = *_state.topic
	}
	partitions, err := _state.cli.Partitions(topic)
	if err != nil {
		fmt.Printf("list topic's partitions failed:\n%s\n", err)
		return nil
	}
	fmt.Println(partitions)
	return nil
}
