package preflight

import (
	"context"
	"fmt"
	"log"
	"time"

	"oci-toolkit-object-storage/core"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func GetPreflight(connobj core.ConnectionObj) {
	log.Println("startconst")

	testReadLimits(connobj.SourceClient, connobj.Config.Source.Bucketname, connobj.NameSpace)

}

func testReadLimits(client objectstorage.ObjectStorageClient, bucketName string, namespace string) {
	fmt.Println("Testing read limits...")

	// Get the current time
	startTime := time.Now()

	// Perform a series of list objects requests
	for i := 1; i <= 10; i++ {
		fmt.Printf("ListObjects request #%d\n", i)

		request := objectstorage.ListObjectsRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(bucketName),
		}
		_, err := client.ListObjects(context.Background(), request)
		if err != nil {
			fmt.Println("Error listing objects:", err)
			continue
		}
	}

	// Calculate the elapsed time
	elapsedTime := time.Since(startTime)
	fmt.Println("Elapsed time:", elapsedTime)
}
