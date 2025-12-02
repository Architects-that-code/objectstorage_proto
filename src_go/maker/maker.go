package maker

// Package maker provides functionality to generate and upload random files to an Object Storage bucket.
// It supports concurrent file creation with configurable limits on number of files and concurrency.

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

// getNamespace retrieves the Object Storage namespace for the given client.
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

// GetMaker generates and uploads a specified number of random files to the source bucket.
// It uses concurrency limited by configuration and logs progress.
// GetMaker generates and uploads a specified number of random files to the source bucket.
// It uses concurrency limited by configuration and logs progress.
// GetMaker generates and uploads a specified number of random files to the source bucket.
// It checks if the bucket exists and creates it if necessary, then proceeds with file creation.
// GetMaker generates and uploads a specified number of random files to the source bucket.
// It checks if the bucket exists and creates it if necessary, then proceeds with file creation.
func GetMaker(connobj core.ConnectionObj) {
	// Check if bucket exists, create if not
	ctx := context.Background()
	getReq := objectstorage.GetBucketRequest{
		NamespaceName: &connobj.NameSpace,
		BucketName:    &connobj.Config.Source.Bucketname,
	}
	_, err := connobj.SourceClient.GetBucket(ctx, getReq)
	if err != nil {
		if serviceErr, ok := common.IsServiceError(err); ok && serviceErr.GetHTTPStatusCode() == 404 {
			if connobj.Config.Source.CompartmentId == "" {
				connobj.Config.Source.CompartmentId = core.GetTenancyOCID(connobj.Config)
				log.Printf("Using tenancy OCID as compartment ID: %s", connobj.Config.Source.CompartmentId)
			}
			log.Printf("Bucket %s does not exist. Creating it.", connobj.Config.Source.Bucketname)
			createReq := objectstorage.CreateBucketRequest{
				NamespaceName: &connobj.NameSpace,
				CreateBucketDetails: objectstorage.CreateBucketDetails{
					Name:             &connobj.Config.Source.Bucketname,
					CompartmentId:    &connobj.Config.Source.CompartmentId,
					PublicAccessType: objectstorage.CreateBucketDetailsPublicAccessTypeNopublicaccess,
				},
			}
			_, createErr := connobj.SourceClient.CreateBucket(ctx, createReq)
			if createErr != nil {
				log.Fatalf("Failed to create bucket: %v", createErr)
			}
			log.Printf("Bucket %s created successfully.", connobj.Config.Source.Bucketname)
		} else {
			log.Fatalf("Error checking bucket: %v", err)
		}
	}
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

// makeFile generates a single random file and uploads it to the bucket.
// Returns true if an error occurred, false otherwise.
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

// putObject uploads the provided contents to the specified object in the bucket.
// It adds metadata and uses retry policy for reliability.
func putObject(namespace, bucketName, objectName string, contents []byte, objectStorageClient objectstorage.ObjectStorageClient) error {
	ctx := context.Background()
	defaultRetryPolicy := common.DefaultRetryPolicy()

	// Generate some metadata for the object
	metadata := map[string]string{
		"created-by": "objectstorage_proto",
		"file-type":  "txt",
		"timestamp":  time.Now().Format(time.RFC3339),
	}

	req := objectstorage.PutObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(bucketName),
		ObjectName:    common.String(objectName),
		ContentLength: common.Int64(int64(len(contents))),
		ContentType:   common.String("application/octet-stream"),
		PutObjectBody: ioutil.NopCloser(bytes.NewReader(contents)),
		OpcMeta:       metadata,
	}
	req.RequestMetadata.RetryPolicy = &defaultRetryPolicy

	_, err := objectStorageClient.PutObject(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to put object: %v", err)
	}
	return nil
}
