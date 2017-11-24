package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/cosiner/argv"
	"github.com/peterh/liner"
	"github.com/urfave/cli"
)

var (
	app     = cli.NewApp()
	history = filepath.Join(os.TempDir(), ".kafka-cli-history")
)

func setApp() {
	app.Name = "kafka-cli"
	app.Version = "0.0.1"
	if time.Now().Unix()%2 == 0 {
		app.Usage = "为众人抱薪者，已困顿于荆棘。为自由开路者，已冻毙于风雪。"
	} else {
		app.Usage = "为众人抱薪者，不可使其冻毙于风雪。为自由开路者，不可使其困顿于荆棘。"
	}
	app.Flags = []cli.Flag{}
	app.Commands = []cli.Command{
		{
			Name:   "connect",
			Usage:  "connect to a kafka cluster",
			Action: handleConnect,
			Flags: []cli.Flag{
				cli.StringSliceFlag{
					Name: "addrs",
				},
			},
		},
		{
			Name:   "disconnect",
			Usage:  "disconnect from kafka cluster",
			Action: handleDisconnect,
		},
		{
			Name:   "info",
			Usage:  "show kafka-cli info",
			Action: handleInfo,
		},
		{
			Name:   "produce",
			Usage:  "produce message",
			Action: handleProduce,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "key, k",
					Usage: "key for message",
				},
				cli.StringFlag{
					Name:  "value, v",
					Usage: "value for message",
				},
				cli.Int64Flag{
					Name:  "partition, p",
					Usage: "partition id",
				},
				cli.StringFlag{
					Name:  "topic, t",
					Usage: "topic name",
				},
			},
		},
		{
			Name:   "consume",
			Usage:  "consume topic",
			Action: handleConsume,
			Flags: []cli.Flag{
				cli.Int64Flag{
					Name:  "partition, p",
					Usage: "partition id",
				},
				cli.StringFlag{
					Name:  "topic, t",
					Usage: "topic name",
				},
			},
		},
		{
			Name: "list",
			Subcommands: []cli.Command{
				{
					Name:   "topics",
					Usage:  "list topics in kafka",
					Action: handleListTopics,
				},
				{
					Name:   "partitions",
					Usage:  "list topic partitions",
					Action: handleListPartitions,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "topic, t",
							Usage: "topic name",
						},
					},
				},
			},
		},
		{
			Name: "get",
			Subcommands: []cli.Command{
				{
					Name:   "offset",
					Usage:  "get offset on a topic's partition",
					Action: handleGetOffset,
					Flags: []cli.Flag{
						cli.Int64Flag{
							Name:  "partition, p",
							Usage: "partition id",
						},
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
		},
	}
}

func main() {
	setApp()
	line := liner.NewLiner()
	defer func() {
		if f, err := os.Create(history); err != nil {
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
	if f, err := os.Open(history); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
	prompt, finalPrompt := ">", ""
	for {
		if _cli.Connected() {
			finalPrompt = fmt.Sprintf("connected %s ", prompt)
		} else {
			finalPrompt = fmt.Sprintf("disconnected %s ", prompt)
		}
		if input, err := line.Prompt(finalPrompt); err == nil {
			args, _ := argv.Argv([]rune(input), nil, argv.Run)
			if len(args) == 0 {
				continue
			}
			app.Run(append([]string{"kafka-cli"}, args[0]...))
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

func handleConnect(c *cli.Context) error {
	_cli.Reset()
	addrs := c.StringSlice("addrs")
	fmt.Println("connecting", addrs, "...")
	if err := _cli.Connect(addrs); err != nil {
		fmt.Println("connect error:", err)
	}
	return nil
}

func handleDisconnect(c *cli.Context) error {
	if err := _cli.Disconnect(); err != nil {
		fmt.Println("disconnect error:", err)
	}
	return nil
}

func handleInfo(c *cli.Context) error {
	for _, state := range _cli.States() {
		fmt.Println(state)
	}
	return nil
}

func handleProduce(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	var (
		key       = c.String("key")
		value     = c.String("value")
		topic     = c.String("topic")
		partition = int32(c.Int64("partition"))
	)
	offset, err := _cli.Produce(topic, partition, []byte(key), []byte(value))
	if err != nil {
		fmt.Printf("produce failed:\n%s\n", err)
		return nil
	}
	fmt.Printf("produce successful, offset %d\n", offset)
	return nil
}

func handleListTopics(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	topics, err := _cli.Topics()
	if err != nil {
		fmt.Printf("list topics failed:\n%s\n", err)
		return nil
	}
	fmt.Println(topics)
	return nil
}

func handleListPartitions(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	topic := c.String("topic")
	partitions, err := _cli.Partitions(topic)
	if err != nil {
		fmt.Printf("list topic's partitions failed:\n%s\n", err)
		return nil
	}
	fmt.Println(partitions)
	return nil
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

func handleConsume(c *cli.Context) error {
	if !_cli.Connected() {
		fmt.Println("no available connection")
		return nil
	}
	var (
		partition = int32(c.Int64("partition"))
		offset    = c.Int64("offset")
		topic     = c.String("topic")
	)
	consumer, err := _cli.PartitionConsumer(topic, partition, offset)
	if err != nil {
		fmt.Printf("get partition consumer failed:\n%s\n", err)
		return nil
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
	return nil
}
