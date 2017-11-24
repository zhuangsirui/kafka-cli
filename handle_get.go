package main

import (
	"fmt"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name: "get",
		Subcommands: []cli.Command{
			{
				Name:   "offset",
				Usage:  "get offset on a topic's partition",
				Action: handleGetOffset,
				Flags: []cli.Flag{
					cli.Int64Flag{
						Name:  "partition, p",
						Usage: "partition id"},
					cli.StringFlag{
						Name:  "topic, t",
						Usage: "topic name",
					},
					cli.Int64Flag{
						Name:  "offset, o",
						Usage: "offset, use -1 for newest and -2 for oldest",
					},
				},
			},
		},
	})
}

func handleGetOffset(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	var (
		topic     = c.String("topic")
		partition = int32(c.Int64("partition"))
		offset    = c.Int64("offset")
	)
	offset, err := _cli.Offset(topic, partition, offset)
	if err != nil {
		fmt.Printf("get offset failed:\n%s\n", err)
		return nil
	}
	fmt.Println(offset)
	return nil
}
