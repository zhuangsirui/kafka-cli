package main

import (
	"fmt"
	"strconv"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name: "set",
		Subcommands: []cli.Command{
			{
				Name:   "topic",
				Usage:  "set topic for cli",
				Action: handleSetTopic,
			},
			{
				Name:   "partition",
				Usage:  "set partition for cli",
				Action: handleSetPartition,
			},
		},
	})
}

func handleSetTopic(c *cli.Context) error {
	if c.NArg() != 1 {
		return nil
	}
	_state.setTopic(c.Args().Get(0))
	return nil
}

func handleSetPartition(c *cli.Context) error {
	if c.NArg() != 1 {
		return nil
	}
	partition, err := strconv.ParseInt(c.Args().Get(0), 10, 32)
	if err != nil {
		fmt.Println("partition invalid:", err)
		return nil
	}
	_state.setPartition(int32(partition))
	return nil
}
