package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/peterh/liner"
)

var historyPath = filepath.Join(os.TempDir(), ".kafka-cli-history")

func main() {
	line := liner.NewLiner()
	defer func() {
		if f, err := os.Create(historyPath); err != nil {
			log.Print("Error reading line: ", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
		line.Close()
	}()
	line.SetCtrlCAborts(false)
	line.SetCompleter(func(line string) (c []string) {
		return
	})
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
	prompt, finalPrompt := ">", ""
	for {
		if _cli.Connected() {
			finalPrompt = fmt.Sprintf("connected %s ", prompt)
		} else {
			finalPrompt = fmt.Sprintf("unconnected %s ", prompt)
		}
		if input, err := line.Prompt(finalPrompt); err == nil {
			handleInput(input)
			line.AppendHistory(input)
		} else if err == liner.ErrPromptAborted {
			fmt.Print("\n")
			break
		} else {
			fmt.Print("\n")
			break
		}
	}
}

func handleInput(input string) {
	cmd, args := parseInput(input)
	switch cmd {
	case "connect":
		if checkArgs(args, mulitArgs, "connect <kafka-broker-addrs>") {
			handleConnect(args)
		}
	case "disconnect":
		if checkArgs(args, 0, "disconnect") {
			handleDisconnect(args)
		}
	case "info":
		if checkArgs(args, 0, "info") {
			handleInfo(args)
		}
	case "produce":
		if checkArgs(args, mulitArgs, "produce what ever you want") {
			handleProduce(args)
		}
	case "list-topics":
		if checkArgs(args, 0, "list-topics") {
			handleListTopics(args)
		}
	case "list-topic-partitions":
		if checkArgs(args, 1, "list-topic-partitions") {
			handleListPartitions(args)
		}
	case "get-partition-offset":
		if checkArgs(args, 2, "get-partition-offset topic partition") {
			handleListOffsets(args)
		}
	case "consume":
		if checkArgs(args, 3, "consume topic partition offset") {
			handleConsume(args)
		}
	}
}

func parseInput(input string) (string, []string) {
	args := strings.Split(input, " ")
	if len(args) >= 1 {
		return args[0], args[1:]
	} else {
		return "", nil
	}
}

func handleConnect(args []string) {
	_cli.Reset()
	if err := _cli.Connect(args); err != nil {
		fmt.Println("connect error:", err)
	}
}

func handleDisconnect(args []string) {
	if err := _cli.Disconnect(); err != nil {
		fmt.Println("disconnect error:", err)
	}
}

func handleInfo(args []string) {
	for _, state := range _cli.States() {
		fmt.Println(state)
	}
}

func handleProduce(args []string) {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return
	}
	offset, err := _cli.Produce("test", 0, []byte(args[0]), []byte(args[1]))
	if err != nil {
		fmt.Printf("produce failed:\n%s\n", err)
		return
	}
	fmt.Printf("produce successful, offset %d\n", offset)
}

func handleListTopics(args []string) {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return
	}
	topics, err := _cli.Topics()
	if err != nil {
		fmt.Printf("list topics failed:\n%s\n", err)
		return
	}
	fmt.Println(topics)
}

func handleListPartitions(args []string) {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return
	}
	partitions, err := _cli.Partitions(args[0])
	if err != nil {
		fmt.Printf("list topic's partitions failed:\n%s\n", err)
		return
	}
	fmt.Println(partitions)
}

func handleListOffsets(args []string) {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return
	}
	partition, err := strconv.ParseInt(args[1], 10, 32)
	if err != nil {
		fmt.Println("partition invalid")
		return
	}
	offset, err := _cli.Offset(args[0], int32(partition), sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("get offset failed:\n%s\n", err)
		return
	}
	fmt.Println(offset)
}

func handleConsume(args []string) {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return
	}
	var (
		partition, offset int64
		consumer          sarama.PartitionConsumer
		err               error
	)
	if partition, err = strconv.ParseInt(args[1], 10, 32); err != nil {
		fmt.Printf("partition `%s` invalid\n", args[1])
		return
	}
	if offset, err = strconv.ParseInt(args[2], 10, 32); err != nil {
		fmt.Println("offset invalid")
		return
	}
	if consumer, err = _cli.PartitionConsumer(args[0], int32(partition), offset); err != nil {
		fmt.Printf("get partition consumer failed:\n%s\n", err)
		return
	}
	defer consumer.Close()
	var (
		resetSeconds = 5
		ticker       = time.NewTicker(time.Second)
		stoppedChan  = make(chan struct{})
	)
	go func() {
	FetchLoop:
		for {
			select {
			case msg := <-consumer.Messages():
				fmt.Println("message:", msg)
				resetSeconds = 5
			case err := <-consumer.Errors():
				fmt.Println("error:", err)
				resetSeconds = 5
			case <-ticker.C:
				resetSeconds--
				fmt.Printf("%d seconds remain to abort\n", resetSeconds)
				if resetSeconds <= 0 {
					close(stoppedChan)
					break FetchLoop
				}
			}
		}
	}()
	<-stoppedChan
	ticker.Stop()
}

const mulitArgs = -1

func checkArgs(args []string, count int, notice string) bool {
	switch count {
	case mulitArgs:
		if len(args) >= 1 {
			return true
		} else {
			fmt.Println(notice)
			return false
		}
	default:
		if len(args) == count {
			return true
		} else {
			fmt.Println(notice)
			return false
		}
	}
}
