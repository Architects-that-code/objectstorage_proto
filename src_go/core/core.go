package core

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/example/helpers"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"gopkg.in/yaml.v2"
)

// create package that will be imported from all other package to handle reading config file and returning config struct
// expose functions to other packages to get config struct

// getConfig reads and parses the configuration from deltaconfig.yaml.
// It returns the parsed Config struct or an error if reading or unmarshaling fails.
// getConfig reads and parses the configuration from deltaconfig.yaml.
// It returns the parsed Config struct or an error if reading or unmarshaling fails.
func getConfig() (error, Config) {
	log.Println("Loading config from deltaconfig.yaml")
	data, err := ioutil.ReadFile("deltaconfig.yaml")
	if err != nil {
		// handle error
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		// handle error
	}
	return err, config
}

// Config represents the application configuration loaded from YAML.
// It includes settings for source and target buckets, regions, and various operational parameters.
type Config struct {
	Source struct {
		Profilename   string `yaml:"profilename"`
		Bucketname    string `yaml:"bucketname"`
		Region        string `yaml:"region"`
		CompartmentId string `yaml:"compartment_id"`
	} `yaml:"source"`
	Target struct {
		Profilename string `yaml:"profilename"`
		Bucketname  string `yaml:"bucketname"`
		Region      string `yaml:"region"`
	} `yaml:"target"`
	HomeRegion           string `yaml:"home_region"`
	DeltaUpdate          bool   `yaml:"deltaupdate"`
	BatchSize            int    `yaml:"batchsize"`
	Limit                int    `yaml:"limit"`
	ProgressInterval     int    `yaml:"progressinterval"`
	MaxConcurrency       int    `yaml:"maxconcurrency"`
	ForceSourceDelete    bool   `yaml:"force_source_delete"`
	ForceSourceRefresh   bool   `yaml:"force_source_refresh"`
	ConfigPath           string `yaml:"configpath"`
	UseInstancePrincipal bool   `yaml:"useinstanceprincipal"`
	RenamerMaxWorker     int    `yaml:"renamer-maxworker"`
	MakerNumFiles        int    `yaml:"maker-numfile"`
	MakerMaxFileSize     int    `yaml:"maker-maxfilesize"`
}

// getSourceClient creates and returns an ObjectStorageClient for the source bucket.
// It uses either instance principal or custom profile configuration based on settings.
func getSourceClient(config Config, err error) objectstorage.ObjectStorageClient {
	var _source_configProvider common.ConfigurationProvider

	if config.UseInstancePrincipal {
		log.Printf("using instanceprincipal")
		_source_configProvider, _ = auth.InstancePrincipalConfigurationProvider()
	} else {
		log.Printf("NOT using instanceprincipal")
		_source_configProvider = common.CustomProfileConfigProvider(config.ConfigPath, config.Source.Profilename)

	}
	_source_objectStorageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(_source_configProvider)

	if err != nil {
		fmt.Println("Error creating Object Storage client:", err)
		os.Exit(1)
	}
	return _source_objectStorageClient
}

// getTargetClient creates and returns an ObjectStorageClient for the target bucket.
// It uses either instance principal or custom profile configuration based on settings.
func getTargetClient(config Config, err error) objectstorage.ObjectStorageClient {
	var _target_configProvider common.ConfigurationProvider

	if config.UseInstancePrincipal {
		log.Printf("using instanceprincipal")
		_target_configProvider, _ = auth.InstancePrincipalConfigurationProvider()
	} else {
		log.Printf("NOT using instanceprincipal")
		_target_configProvider = common.CustomProfileConfigProvider(config.ConfigPath, config.Target.Profilename)

	}

	_target_objectStorageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(_target_configProvider)
	if err != nil {
		fmt.Println("Error creating Object Storage client:", err)
		os.Exit(1)
	}
	return _target_objectStorageClient
}

// ConnectionObj holds the clients and configuration for source and target Object Storage connections.
type ConnectionObj struct {
	SourceClient objectstorage.ObjectStorageClient
	TargetClient objectstorage.ObjectStorageClient
	Config       Config
	NameSpace    string
}

// GetConnections initializes and returns a ConnectionObj with source and target clients.
// It loads the config and sets up clients, exiting on errors.
func GetConnections() ConnectionObj {
	err, config := getConfig()
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}

	sourceClient := getSourceClient(config, err)
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}

	targetClient := getTargetClient(config, err)
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}

	namespace := getnamespace(context.Background(), sourceClient)

	connObj := ConnectionObj{Config: config, SourceClient: sourceClient, TargetClient: targetClient, NameSpace: namespace}
	return connObj
}

// getnamespace retrieves the Object Storage namespace for the given client.
func getnamespace(ctx context.Context, c objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	r, err := c.GetNamespace(ctx, request)
	helpers.FatalIfError(err)
	fmt.Println("getting namespace")
	return *r.Value
}

// GetObjectCount returns the approximate number of objects in the specified bucket as a string.
func GetObjectCount(namespace, bucketName string, objectStorageClient objectstorage.ObjectStorageClient) string {
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
	var size = *objectCount

	log.Printf("bucket %v in region %v has approximately %s objects\n", bucketName, objectStorageClient.Endpoint(), strconv.FormatInt(int64(*objectCount), 10))
	return strconv.Itoa(int(size))
}

// ListObjectsInBucket lists all objects in the bucket asynchronously, sending results to objSums channel.
// It handles pagination and provides progress updates.
func ListObjectsInBucket(namespace, bucketName string, objectStorageClient objectstorage.ObjectStorageClient, wg *sync.WaitGroup, objSums chan<- []objectstorage.ObjectSummary, errCh chan<- error) {
	approxsize := GetObjectCount(namespace, bucketName, objectStorageClient)
	fmt.Printf("##### approx size of bucket %v is %v \n", bucketName, approxsize)

	defer wg.Done()
	fmt.Printf("getting data from: bucket: %v in  %v \n", bucketName, objectStorageClient.Host)

	defaultRetryPolicy := common.DefaultRetryPolicy()
	//var objects []objectstorage.ObjectSummary
	size, err := strconv.Atoi(approxsize)
	if err != nil {
		// Handle the error here, for example:
		log.Fatal(err)
	}
	fmt.Printf("size: %v\n", size)
	//var objects []objectstorage.ObjectSummary
	objects := make([]objectstorage.ObjectSummary, size/2)
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
			log.Printf("Retrieved %d objects so far from bucket %v", len(objects)-(size/2), bucketName)
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

// GetTenancyOCID retrieves the tenancy OCID from the configuration provider for the source.
func GetTenancyOCID(config Config) string {
	var provider common.ConfigurationProvider
	if config.UseInstancePrincipal {
		provider, _ = auth.InstancePrincipalConfigurationProvider()
	} else {
		provider = common.CustomProfileConfigProvider(config.ConfigPath, config.Source.Profilename)
	}
	tenancy, err := provider.TenancyOCID()
	if err != nil {
		log.Fatalf("Failed to get tenancy OCID: %v", err)
	}
	return tenancy
}
