package main

import (
	"fmt"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name:   "disconnect",
		Usage:  "disconnect from kafka cluster",
		Action: handleDisconnect,
	})
}

func handleDisconnect(c *cli.Context) error {
	if err := _state.cli.Disconnect(); err != nil {
		fmt.Println("disconnect error:", err)
	}
	return nil
}
