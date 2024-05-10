package renamer

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"oci-toolkit-object-storage/core"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const maxWorkers = 10 // Maximum number of concurrent goroutines

func GetRenamer(connobj core.ConnectionObj) {
	maxWorkers := connobj.Config.RenamerMaxWorker

	log.Printf("start GetRenamer:maxWorkers:%v", maxWorkers)

	bucketName := connobj.Config.Source.Bucketname
	namespace := connobj.NameSpace

	response, _ := ListObjectsInBucket(namespace, bucketName, connobj.SourceClient)
	newSubfolderPath := "files"

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create a channel to receive errors from goroutines
	errCh := make(chan error)

	// Create a buffered channel to control the number of simultaneous workers
	workerCh := make(chan struct{}, maxWorkers)

	// Iterate over object summaries and spawn goroutines with limited concurrency
	for _, objectSummary := range response {
		// Increment the wait group counter
		wg.Add(1)

		// Acquire a worker slot from the channel
		workerCh <- struct{}{}

		go func(obj objectstorage.ObjectSummary) {
			defer func() {
				// Release the worker slot back to the channel
				<-workerCh

				// Notify the wait group that the goroutine has finished
				wg.Done()
			}()

			newKey := newSubfolderPath + *obj.Name
			log.Printf("copying %v ==> %v \n", *obj.Name, newKey)

			err := copyObj(namespace, bucketName, obj, newKey, connobj, connobj.SourceClient)
			if err != nil {
				errCh <- err
			}
		}(objectSummary)
	}

	// Start a goroutine to close the error channel when all goroutines are done
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// Collect errors from the error channel
	for err := range errCh {
		fmt.Println("Error:", err)
	}

	fmt.Println("All requests completed.")
}

func deleteaftercopy(ns string, bucketName string, objSum objectstorage.ObjectSummary, connobj core.ConnectionObj) {
	log.Printf("deleting %v \n", *objSum.Name)
	deleteRequest := objectstorage.DeleteObjectRequest{
		NamespaceName: &ns,
		BucketName:    &bucketName,
		ObjectName:    objSum.Name,
	}
	_, err := connobj.SourceClient.DeleteObject(context.Background(), deleteRequest)
	if err != nil {
		fmt.Println("Error Deleting Object: ABORT", err)
		os.Exit(1)
	}
}

func copyObj(ns string, bucketName string, objSum objectstorage.ObjectSummary, newKey string, connobj core.ConnectionObj, client objectstorage.ObjectStorageClient) error {
	defaultRetryPolicy := common.DefaultRetryPolicy()
	coR := objectstorage.CopyObjectRequest{
		NamespaceName: &ns,
		BucketName:    &bucketName,
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      objSum.Name,
			DestinationRegion:     &connobj.Config.Source.Region,
			DestinationNamespace:  &ns,
			DestinationBucket:     &connobj.Config.Source.Bucketname,
			DestinationObjectName: &newKey,
		},
	}
	coR.RequestMetadata = common.RequestMetadata{
		RetryPolicy: &defaultRetryPolicy,
	}

	_, err := client.CopyObject(context.Background(), coR)
	if err != nil {
		fmt.Println("Error Copying Object:", err)
		return err
	}

	fmt.Println("Copied Object:", *objSum.Name+" to "+newKey)
	deleteaftercopy(ns, bucketName, objSum, connobj)
	return nil
}

func ListObjectsInBucket(namespace string, bucketName string, objectStorageClient objectstorage.ObjectStorageClient) ([]objectstorage.ObjectSummary, error) {
	fmt.Printf("getting data from:  %v \n", objectStorageClient.Host)
	var objSums []objectstorage.ObjectSummary
	fields := "name,size,timeCreated,timeModified,storageTier"
	listObjectsRequest := objectstorage.ListObjectsRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketName,
		Fields:        &fields,
	}
	ctx := context.Background()
	for {
		listObjectsResponse, err := objectStorageClient.ListObjects(ctx, listObjectsRequest)
		if err != nil {
			return nil, err
		}
		for _, objectSummary := range listObjectsResponse.ListObjects.Objects {
			objSums = append(objSums, objectSummary)
		}
		if listObjectsResponse.ListObjects.NextStartWith != nil {
			listObjectsRequest.Start = listObjectsResponse.ListObjects.NextStartWith
		} else {
			break
		}
	}
	return objSums, nil
}
