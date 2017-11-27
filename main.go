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

func init() {
	app.Name = "kafka-cli"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "addrs",
			Usage: "set default addrs",
		},
	}
	if time.Now().Unix()%2 == 0 {
		app.Usage = "为众人抱薪者，已困顿于荆棘。为自由开路者，已冻毙于风雪。"
	} else {
		app.Usage = "为众人抱薪者，不可使其冻毙于风雪。为自由开路者，不可使其困顿于荆棘。"
	}
}

func main() {
	if len(os.Args) > 1 {
		app.Run(os.Args)
		return
	}
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
	for {
		if input, err := line.Prompt(_state.String() + " > "); err == nil {
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
