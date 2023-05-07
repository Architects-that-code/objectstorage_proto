package reader

import (
	"context"
	"myworkspace/core"
	"strconv"

	"log"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func GetSizes(connobj core.ConnectionObj) {

	getObjectCount(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient)

	getObjectCount(connobj.NameSpace, connobj.Config.Target.Bucketname, connobj.TargetClient)

}

func getObjectCount(namespace, bucketName string, objectStorageClient objectstorage.ObjectStorageClient) {
	// Create a context for the API call
	ctx := context.Background()

	// Create the request to get the bucket metadata
	req := objectstorage.GetBucketRequest{
		NamespaceName:   &namespace,
		BucketName:      &bucketName,
		Fields:          []objectstorage.GetBucketFieldsEnum{objectstorage.GetBucketFieldsApproximatecount},
		RequestMetadata: common.RequestMetadata{},
	}

	// Call the API to get the bucket metadata
	res, err := objectStorageClient.GetBucket(ctx, req)
	if err != nil {
		log.Fatalf("Error getting bucket: %v\n", err)
	}
	//log.Printf("res: %v\n", res)

	// Get the object count from the bucket metadata
	objectCount := res.Bucket.ApproximateCount

	log.Printf("bucket %v in region %v has approximately %s objects\n", bucketName, objectStorageClient.Endpoint(), strconv.FormatInt(int64(*objectCount), 10))

}
