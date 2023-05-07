package reader

import (
	"fmt"
	"sync"

	"myworkspace/core"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// This function returns a list of all objects in a given bucket

func GetReader(connobj core.ConnectionObj) {

	GetSizes(connobj)

	var wg sync.WaitGroup
	wg.Add(2)

	sourceObjectsCh := make(chan []objectstorage.ObjectSummary)
	targetObjectsCh := make(chan []objectstorage.ObjectSummary)
	errCh := make(chan error, 2)

	go core.ListObjectsInBucket(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient, &wg, sourceObjectsCh, errCh)
	go core.ListObjectsInBucket(connobj.NameSpace, connobj.Config.Target.Bucketname, connobj.TargetClient, &wg, targetObjectsCh, errCh)

	go func() {
		wg.Wait()
		close(sourceObjectsCh)
		close(targetObjectsCh)
		close(errCh)
	}()

	var sourceObjects, targetObjects []objectstorage.ObjectSummary

	for {
		select {
		case obj, ok := <-sourceObjectsCh:
			if !ok {
				sourceObjectsCh = nil
				break
			}
			sourceObjects = append(sourceObjects, obj...)
		case obj, ok := <-targetObjectsCh:
			if !ok {
				targetObjectsCh = nil
				break
			}
			targetObjects = append(targetObjects, obj...)
		case err := <-errCh:
			fmt.Println("Error listing objects in bucket:", err)
		}
		if sourceObjectsCh == nil && targetObjectsCh == nil {
			break
		}
	}

	fmt.Println("Found Source ", len(sourceObjects), "objects")
	fmt.Println("Found Target ", len(targetObjects), "objects")

}
