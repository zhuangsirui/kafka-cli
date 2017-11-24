package main

import (
	"fmt"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name:   "info",
		Usage:  "show kafka-cli info",
		Action: handleInfo,
	})
}

func handleInfo(c *cli.Context) error {
	for _, state := range _cli.States() {
		fmt.Println(state)
	}
	return nil
}
