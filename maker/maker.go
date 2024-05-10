package maker

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"

	"log"
	"math/big"
	"oci-toolkit-object-storage/core"
	"sync"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/example/helpers"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func getNamespace(ctx context.Context, c objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	r, err := c.GetNamespace(ctx, request)
	helpers.FatalIfError(err)
	fmt.Println("getting namespace")
	return *r.Value
}

var (
	numFiles    = 5000
	minFileSize = 1024 * 1         // 1KB
	maxFileSize = 1024 * 1024 * 10 // 10MB
)

var maxWorkers = 100 // Maximum number of concurrent goroutines

func GetMaker(connobj core.ConnectionObj) {
	numFiles = connobj.Config.MakerNumFiles
	maxWorkers = connobj.Config.RenamerMaxWorker
	log.Printf("orig maxFileSize:%v", maxFileSize)
	maxFileSize = connobj.Config.MakerMaxFileSize

	log.Printf("start GetMaker:numFiles:%v and maxWorkers:%v and maxFileSize:%v ", numFiles, maxWorkers, maxFileSize)
	bucketName := connobj.Config.Source.Bucketname
	namespace := connobj.NameSpace

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create a channel to receive errors from goroutines
	errCh := make(chan error)

	// Create a buffered channel to control the number of simultaneous workers
	workerCh := make(chan struct{}, maxWorkers)

	// Iterate over object summaries and spawn goroutines with limited concurrency

	for i := 0; i < numFiles; i++ {
		wg.Add(1)
		workerCh <- struct{}{}
		go func() {
			defer func() {
				// Release the worker slot back to the channel
				<-workerCh

				// Notify the wait group that the goroutine has finished
				wg.Done()
			}()

			shouldReturn := makeFile(namespace, bucketName, connobj.SourceClient)
			if shouldReturn {
				return
			}
		}()
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

func makeFile(namespace string, bucketName string, objectStorageClient objectstorage.ObjectStorageClient) bool {

	fileSizeRange := big.NewInt(int64(maxFileSize - minFileSize + 1))

	fileSize, err := rand.Int(rand.Reader, fileSizeRange)
	if err != nil {
		log.Println("Error generating random file size:", err)
		return true
	}
	//randomSize := int(fileSize.Int64()) + minFileSize

	fileData := make([]byte, int(fileSize.Int64()))
	if _, err := rand.Read(fileData); err != nil {
		log.Println("Error generating random file contents:", err)
		return true
	}

	now := time.Now()
	nano := now.UnixNano()
	minute := nano / 1000000000 // Divide by 1 billion to get seconds, then by 60 to get minutes
	filename := fmt.Sprintf("files/%d/%02d/%02d/%02d/%02d/%d.txt", now.Year(), now.Month(), now.Day(), now.Hour(), minute, nano)
	if err := putObject(namespace, bucketName, filename, fileData, objectStorageClient); err != nil {
		log.Printf("Error uploading file %s to bucket: %v\n", filename, err)
		return true
	}

	log.Printf("Uploaded file %s with size %d to bucket %s\n", filename, fileSize, bucketName)
	return false
}

func putObject(namespace, bucketName, objectName string, contents []byte, objectStorageClient objectstorage.ObjectStorageClient) error {
	ctx := context.Background()
	defaultRetryPolicy := common.DefaultRetryPolicy()
	req := objectstorage.PutObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(bucketName),
		ObjectName:    common.String(objectName),
		ContentLength: common.Int64(int64(len(contents))),
		ContentType:   common.String("application/octet-stream"),
		PutObjectBody: ioutil.NopCloser(bytes.NewReader(contents)),
	}
	req.RequestMetadata.RetryPolicy = &defaultRetryPolicy

	_, err := objectStorageClient.PutObject(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to put object: %v", err)
	}
	return nil
}
