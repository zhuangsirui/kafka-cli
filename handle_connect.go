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
			cli.StringFlag{
				Name: "addrs",
			},
		},
	})
}

func handleConnect(c *cli.Context) error {
	_state.cli.Reset()
	addrs := parseAddrs(c.String("addrs"))
	if len(addrs) == 0 {
		fmt.Println("must provide at least one broker addr")
		return nil
	}
	fmt.Println("connecting...", addrs)
	if err := connect(addrs); err != nil {
		fmt.Println("connect error:", err)
	}
	return nil
}
