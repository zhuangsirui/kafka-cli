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
						Name:  "time",
						Usage: "timestamp, use -1 for newest and -2 for oldest",
						Value: -1,
					},
				},
			},
		},
	})
}

func handleGetOffset(c *cli.Context) error {
	if ok := checkConnect(c.GlobalString("addrs")); !ok {
		return nil
	}
	var (
		topic     = c.String("topic")
		partition = int32(c.Int64("partition"))
		time      = c.Int64("time")
	)
	offset, err := _state.cli.Offset(topic, partition, time)
	if err != nil {
		fmt.Printf("get offset failed:\n%s\n", err)
		return nil
	}
	fmt.Println(offset)
	return nil
}
