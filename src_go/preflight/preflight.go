package preflight

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"oci-toolkit-object-storage/core"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// GetPreflight runs a test to check read limits on the source bucket by performing multiple list objects requests.
func GetPreflight(connobj core.ConnectionObj) {
	log.Println("startconst")

	testReadLimits(connobj.SourceClient, connobj.Config.Source.Bucketname, connobj.NameSpace)

}

// testReadLimits performs a series of list objects requests to test read limits and measures the elapsed time.
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

// TestConcurrentObjectReads tests concurrent reads of a single object.
// It first creates or overwrites the object with random data of the specified size,
// then spawns the specified number of goroutines to read the object and measures total time.
func TestConcurrentObjectReads(connobj core.ConnectionObj, objectName string, size int, concurrency int) {
	// Create or overwrite the test object
	log.Printf("Creating test object %s of size %d bytes", objectName, size)
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		log.Fatalf("Failed to generate random data: %v", err)
	}

	putReq := objectstorage.PutObjectRequest{
		NamespaceName: &connobj.NameSpace,
		BucketName:    &connobj.Config.Source.Bucketname,
		ObjectName:    &objectName,
		ContentLength: common.Int64(int64(size)),
		PutObjectBody: ioutil.NopCloser(bytes.NewReader(data)),
	}
	_, err = connobj.SourceClient.PutObject(context.Background(), putReq)
	if err != nil {
		log.Fatalf("Failed to create test object: %v", err)
	}
	log.Println("Test object created successfully")

	log.Printf("Testing concurrent reads of object %s (%d bytes) with %d goroutines", objectName, size, concurrency)

	ctx := context.Background()
	startTime := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			req := objectstorage.GetObjectRequest{
				NamespaceName: &connobj.NameSpace,
				BucketName:    &connobj.Config.Source.Bucketname,
				ObjectName:    &objectName,
			}

			resp, err := connobj.SourceClient.GetObject(ctx, req)
			if err != nil {
				errCh <- fmt.Errorf("error reading object: %v", err)
				return
			}
			defer resp.Content.Close()

			_, err = ioutil.ReadAll(resp.Content)
			if err != nil {
				errCh <- fmt.Errorf("error reading content: %v", err)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		log.Println(err)
	}

	elapsed := time.Since(startTime)
	log.Printf("Concurrent read test completed in %v", elapsed)
}
