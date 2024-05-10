package stuff

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"oci-toolkit-object-storage/core"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func GetObjectStoragePath(connobj core.ConnectionObj) (string, error) {
	// Create a new Object Storage client

	client := connobj.SourceClient
	region := connobj.Config.Source.Region
	bucketName := connobj.Config.Source.Bucketname
	testObjName := "test"
	// Get the Object Storage namespace using the client

	namespace := connobj.NameSpace
	// Check if the Object Storage is using a service gateway or internet
	endpoint := fmt.Sprintf("%s/objectstorage/%s/%s", region, namespace, bucketName)

	log.Printf("endpoint:%v", endpoint)

	putObject(namespace, bucketName, testObjName, []byte(testObjName), client)

	request := objectstorage.HeadObjectRequest{
		ObjectName:    common.String(testObjName),
		NamespaceName: common.String(namespace),
		BucketName:    common.String(bucketName),
	}

	hoR, err := client.HeadObject(context.Background(), request)
	if err != nil {
		log.Printf("error:%v", err)
		if serviceError, ok := common.IsServiceError(err); ok && serviceError.GetHTTPStatusCode() == 301 {
			log.Printf("301 error so using service gateway")
			log.Printf("https://%s.objectstorage.%s.oraclecloud.com", namespace, region)
			return fmt.Sprintf("https://%s.objectstorage.%s.oraclecloud.com", namespace, region), nil
		} else {
			return "", err
		}
	} else {
		log.Printf("no error so using internet")
		log.Printf("hoR %v", hoR)
		log.Printf("https://objectstorage.%s.oraclecloud.com", region)
		return fmt.Sprintf("https://objectstorage.%s.oraclecloud.com", region), nil
	}
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
