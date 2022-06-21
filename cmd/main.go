package main

import (
	"fmt"
	"os"

	p "github.com/Kaylthomas777/polygonWebSocketAggregator/internal/polygonAggregator"
)

func main() {
	cmdArgs := os.Args
	if len(cmdArgs) != 2{
		fmt.Println("Please provide a single Crypto Symbol")
	} else {
		p.Orchestrate(cmdArgs[1] + "-USD")
	}
}