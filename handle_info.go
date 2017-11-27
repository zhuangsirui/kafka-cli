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
	for _, state := range _state.cli.States() {
		fmt.Println(state)
	}
	if _state.topic != nil {
		fmt.Println("topic:", *_state.topic)
	}
	if _state.partition != nil {
		fmt.Println("partition:", *_state.partition)
	}
	return nil
}
