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
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	topics, err := _cli.Topics()
	if err != nil {
		fmt.Printf("list topics failed:\n%s\n", err)
		return nil
	}
	fmt.Println(topics)
	return nil
}

func handleListPartitions(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	topic := c.String("topic")
	partitions, err := _cli.Partitions(topic)
	if err != nil {
		fmt.Printf("list topic's partitions failed:\n%s\n", err)
		return nil
	}
	fmt.Println(partitions)
	return nil
}
