package main

import (
	"fmt"
	"strings"
)

func connected() bool {
	return _state.cli.Connected()
}

func connect(addrs []string) error {
	return _state.cli.Connect(addrs)
}

func parseAddrs(flag string) (addrs []string) {
	for _, addr := range strings.Split(flag, ",") {
		addrs = append(addrs, addr)
	}
	return
}

func checkConnect(flagAddrs string) bool {
	if connected() {
		return true
	}
	err := connect(parseAddrs(flagAddrs))
	if err == nil {
		return true
	}
	fmt.Println("connect error:", err)
	return false
}
