package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"oci-toolkit-object-storage/core"
	"oci-toolkit-object-storage/delta"
	"oci-toolkit-object-storage/maker"
	"oci-toolkit-object-storage/preflight"
	"oci-toolkit-object-storage/reader"
	"oci-toolkit-object-storage/renamer"
	"oci-toolkit-object-storage/stuff"
	"oci-toolkit-object-storage/swapper"

	utils "oci-toolkit-object-storage/util"

	"os"
	"strconv"
)

func main() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	utils.PrintBanner()
	fmt.Println("____")

	fmt.Println("Select an option:")
	fmt.Println("1. GetReader: I might be slow, but I will get you all the files in your bucket")
	fmt.Println("2. GetRenamer do not use unless you know what you are doing (ie, read the code)")
	fmt.Println("3. GetDelta: find files that are in source but not in target and allow to touch to sync")
	fmt.Println("4. GetMaker: CAUTION: this will create LOTS of files in your source bucket")
	fmt.Println("5. GetPreflight")
	fmt.Println("6. CheckPath")
	fmt.Println("7. GetSizes: FASTEST way to get sizes of all files in a bucket and check for replication policies")
	fmt.Println("8. GetSingleReader: read all from single bucket")
	fmt.Println("9. SWAPPING: Change bucket from source to target")

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
	case 7:
		connobj := core.GetConnections()
		getSizes(connobj)
	case 8:
		connobj := core.GetConnections()
		getSingleReader(connobj)
	case 9:
		connobj := core.GetConnections()
		getSwapper(connobj)
	default:
		fmt.Println("Invalid choice")
	}
}

func getSwapper(connobj core.ConnectionObj) {
	swapper.GetSwapper(connobj)
}

func getSingleReader(connobj core.ConnectionObj) {
	reader.GetSourceOnlyReader(connobj)
}

func getSizes(connobj core.ConnectionObj) {
	// GetSizes retrieves the sizes of the objects in the connection object.
	reader.GetSizes(connobj)
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
