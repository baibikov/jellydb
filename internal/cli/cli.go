package cli

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/pkg/errors"
)

type Cli struct {
	conn net.Conn
}

func (c Cli) Close() error {
	return c.conn.Close()
}

type Config struct {
	Addr string
}

func New(config *Config) (*Cli, error) {
	if config == nil {
		return nil, errors.New("config has not be empty")
	}

	if config.Addr == "" {
		return nil, errors.New("config addr has not be empty")
	}

	conn, err := net.Dial("tcp", config.Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "connect by tcp protocol to address %s", config.Addr)
	}

	return &Cli{conn: conn}, nil
}

const foreword = `JellyDB (message broker database) СLI 🤟
----------------------
(sys)
-help:  Navigating existing Commands
exit:  Exit from CLI
clear: Carriage cleaning
`

const commands = `commands:
(sys)
-help:  Navigating existing Commands
exit:  Exit from CLI
clear: Carriage cleaning

(store)
SET: Adding an entry to the read queue, as soon as the entry
example:
> SET my_super_important SOME_VALUE_1

GET [N]: Getting uncommitted messages from the batch queue and n is batch elements
example:
> GET my_super_important 2
> SOME_VALUE_1
> SOME_VALUE_2

COM [N]: Commenting on a batch of messages
example:
> COMMIT my_super_important 2

S_ERR: syntax error, displayed if you made a mistake while writing the request
E_ERR: system error, the error indicates that you encountered a problem while executing the request
`

const (
	helpCommand  = "-help"
	exitCommand  = "exit"
	clearCommand = "clear"
)

const (
	setCommand    = "SET"
	getCommand    = "GET"
	commitCommand = "COM"
)

const (
	undefinedCommand = "undefined command"
	escapeInfo       = "\033[H\033[2J"
	lineBreak        = "\n"
)

func (c *Cli) Run(ctx context.Context) {
	fmt.Print(foreword, lineBreak)
	reader := bufio.NewReader(os.Stdin)

cliLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')

		tape := strings.Trim(strings.TrimSpace(text), lineBreak)

		switch tape {
		case helpCommand:
			fmt.Print(commands, lineBreak)
		case exitCommand:
			fmt.Println("👋 buy")
			break cliLoop
		case clearCommand:
			fmt.Print(escapeInfo, lineBreak)
		case "":
			continue
		default:
			tree := commandTree(tape)
			if len(tree) == 0 {
				fmt.Printf(`💨 %s "%s"%s`, undefinedCommand, tape, lineBreak)
				continue
			}

			payload, err := c.execCommand(tree)
			if err != nil {
				fmt.Printf("🚫 %v %s", err, lineBreak)
				continue
			}

			for _, pp := range payload {
				fmt.Println(pp)
			}
		}
	}
}

const (
	commandIndex = 0
)

func commandTree(tape string) []string {
	sep := strings.Split(tape, " ")
	if len(sep) == 0 {
		return nil
	}

	com := sep[commandIndex]
	if !isStoreCommand(com) {
		return nil
	}

	res := append([]string(nil), com)
	return append(res, sep[1:]...)
}

func (c *Cli) execCommand(tree []string) ([]string, error) {
	return newCommand(tree[0], c.conn, tree[1:])
}

func isStoreCommand(s string) bool {
	return s == setCommand || s == getCommand || s == commitCommand
}
