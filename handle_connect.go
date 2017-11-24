package main

import (
	"fmt"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name:   "connect",
		Usage:  "connect to a kafka cluster",
		Action: handleConnect,
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name: "addrs",
			},
		},
	})
}

func handleConnect(c *cli.Context) error {
	_cli.Reset()
	addrs := c.StringSlice("addrs")
	fmt.Println("connecting", addrs, "...")
	if err := _cli.Connect(addrs); err != nil {
		fmt.Println("connect error:", err)
	}
	return nil
}
