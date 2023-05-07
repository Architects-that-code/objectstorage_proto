package main

import (
	"fmt"

	"bufio"
	"myworkspace/core"
	"myworkspace/delta"
	"myworkspace/maker"
	"myworkspace/preflight"
	"myworkspace/reader"
	"myworkspace/renamer"
	"myworkspace/stuff"

	"os"
	"strconv"
)

func main() {
	fmt.Println("____")

	fmt.Println("Select an option:")
	fmt.Println("1. GetReader")
	fmt.Println("2. GetRenamer")
	fmt.Println("3. GetDelta")
	fmt.Println("4. GetMaker")
	fmt.Println("5. GetPreflight")
	fmt.Println("6. CheckPath")

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter your choice: ")
	input, _ := reader.ReadString('\n')
	choice, err := strconv.Atoi(input[:len(input)-1])
	if err != nil {
		fmt.Println("Invalid choice")
		return
	}
	switch choice {
	case 1:
		connobj := core.GetConnections()
		getReader(connobj)
	case 2:
		connobj := core.GetConnections()
		getRenamer(connobj)
	case 3:
		connobj := core.GetConnections()
		getDelta(connobj)
	case 4:
		connobj := core.GetConnections()
		getMaker(connobj)
	case 5:
		connobj := core.GetConnections()
		getPreflight(connobj)
	case 6:
		connobj := core.GetConnections()
		getPaths(connobj)
	default:
		fmt.Println("Invalid choice")
	}
}

func getPaths(connobj core.ConnectionObj) {
	stuff.GetObjectStoragePath(connobj)
}

func getPreflight(connobj core.ConnectionObj) {
	preflight.GetPreflight(connobj)
}

func getReader(connobj core.ConnectionObj) {
	reader.GetReader(connobj)
}

func getRenamer(connobj core.ConnectionObj) {
	renamer.GetRenamer(connobj)
}
func getDelta(connobj core.ConnectionObj) {
	delta.GetDelta(connobj)
}

func getMaker(connobj core.ConnectionObj) {
	maker.GetMaker(connobj)
}
