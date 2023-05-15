package reader

import (
	"context"
	"fmt"
	"log"
	"myworkspace/core"
	"time"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// This function returns a list of all objects in a given bucket

func GetSourceOnlyReader(connobj core.ConnectionObj) {

	GetObjectCount(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient)
	objects, _ := ListObjectsInBucketSIMPLE(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient)
	//objects := ListObjectsInSingleBucket(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient)
	if objects != nil {
		log.Printf("number of objects in bucket %s is %d", connobj.Config.Source.Bucketname, len(objects))
	}
}

func ListObjectsInSingleBucket(namespace, bucketName string, objectStorageClient objectstorage.ObjectStorageClient) []objectstorage.ObjectSummary {

	fmt.Printf("getting data from: bucket: %v in  %v \n", bucketName, objectStorageClient.Host)

	var objects []objectstorage.ObjectSummary
	fields := "name,size,timeCreated,timeModified,storageTier"

	listObjectsRequest := objectstorage.ListObjectsRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketName,
		Fields:        &fields,
	}

	ctx := context.Background()

	var numObjectsRetrieved int
	lastStatusTime := time.Now()

	// Create a ticker that prints a status message every 10 seconds
	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-statusTicker.C:
			timeDelta := time.Since(lastStatusTime)
			objectsPerSecond := float64(len(objects)-numObjectsRetrieved) / timeDelta.Seconds()

			log.Printf("Retrieved %d objects so far from bucket %v (%.2f objects/sec)", len(objects), bucketName, objectsPerSecond)

			// Update the variables to keep track of the number of objects retrieved and the time of the last status message
			numObjectsRetrieved = len(objects)
			lastStatusTime = time.Now()

		default:
			// Continue with the loop
		}

		listObjectsResponse, err := objectStorageClient.ListObjects(ctx, listObjectsRequest)
		if err != nil {
			fmt.Printf("error in list objects: %v", err)
			return nil
		}
		objects = append(objects, listObjectsResponse.ListObjects.Objects...)

		if listObjectsResponse.ListObjects.NextStartWith != nil {
			//log.Printf("from bucket %v, next start with: %v", bucketName, *listObjectsResponse.ListObjects.NextStartWith)
			listObjectsRequest.Start = listObjectsResponse.ListObjects.NextStartWith
		} else {
			break
		}
	}
	return objects
}

func ListObjectsInBucketSIMPLE(namespace, bucketName string, objectStorageClient objectstorage.ObjectStorageClient) ([]objectstorage.ObjectSummary, error) {
	fmt.Printf("getting data from: bucket: %v in  %v \n", bucketName, objectStorageClient.Host)
	var objSums []objectstorage.ObjectSummary
	fields := "name,size,timeCreated,timeModified,storageTier"
	/**
	var limit = 1000
	Limit:         common.Int(limit),
	**/
	listObjectsRequest := objectstorage.ListObjectsRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketName,
		Fields:        &fields,
	}
	ctx := context.Background()
	var ctr = 1
	for {
		log.Printf("call %d", ctr)
		listObjectsResponse, err := objectStorageClient.ListObjects(ctx, listObjectsRequest)
		if err != nil {
			return nil, err
		}

		//objSums = append(objSums, listObjectsResponse.ListObjects.Objects...)
		//log.Printf("num retrieved so far: %d", len(objSums))

		if listObjectsResponse.ListObjects.NextStartWith != nil {
			//log.Printf("call next: %s", *listObjectsResponse.ListObjects.NextStartWith)
			listObjectsRequest.Start = listObjectsResponse.ListObjects.NextStartWith
		} else {
			break
		}
		ctr++
	}
	return objSums, nil
}
