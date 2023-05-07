package reader

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"myworkspace/core"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// This function returns a list of all objects in a given bucket
func ListObjectsInBucket(namespace, bucketName string, objectStorageClient objectstorage.ObjectStorageClient, wg *sync.WaitGroup, objSums chan<- []objectstorage.ObjectSummary, errCh chan<- error) {
	defer wg.Done()
	fmt.Printf("getting data from: bucket: %v in  %v \n", bucketName, objectStorageClient.Host)

	defaultRetryPolicy := common.DefaultRetryPolicy()
	var objects []objectstorage.ObjectSummary
	fields := "name,size,timeCreated,timeModified,storageTier"

	listObjectsRequest := objectstorage.ListObjectsRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketName,
		Fields:        &fields,
	}

	listObjectsRequest.RequestMetadata = common.RequestMetadata{
		RetryPolicy: &defaultRetryPolicy,
	}
	ctx := context.Background()

	// Create a ticker that prints a status message every 10 seconds
	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-statusTicker.C:
			log.Printf("Retrieved %d objects so far from bucket %v", len(objects), bucketName)
		default:
			// Continue with the loop
		}

		listObjectsResponse, err := objectStorageClient.ListObjects(ctx, listObjectsRequest)
		if err != nil {
			errCh <- err
			return
		}
		objects = append(objects, listObjectsResponse.ListObjects.Objects...)

		if listObjectsResponse.ListObjects.NextStartWith != nil {
			//log.Printf("from bucket %v, next start with: %v", bucketName, *listObjectsResponse.ListObjects.NextStartWith)
			listObjectsRequest.Start = listObjectsResponse.ListObjects.NextStartWith
		} else {
			break
		}
	}
	objSums <- objects
}

func GetReader(connobj core.ConnectionObj) {

	GetSizes(connobj)

	var wg sync.WaitGroup
	wg.Add(2)

	sourceObjectsCh := make(chan []objectstorage.ObjectSummary)
	targetObjectsCh := make(chan []objectstorage.ObjectSummary)
	errCh := make(chan error, 2)

	go ListObjectsInBucket(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient, &wg, sourceObjectsCh, errCh)
	go ListObjectsInBucket(connobj.NameSpace, connobj.Config.Target.Bucketname, connobj.TargetClient, &wg, targetObjectsCh, errCh)

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
