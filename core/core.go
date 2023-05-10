package core

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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

func getConfig() (error, Config) {
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

type Config struct {
	Source struct {
		Profilename string `yaml:"profilename"`
		Bucketname  string `yaml:"bucketname"`
		Region      string `yaml:"region"`
	} `yaml:"source"`
	Target struct {
		Profilename string `yaml:"profilename"`
		Bucketname  string `yaml:"bucketname"`
		Region      string `yaml:"region"`
	} `yaml:"target"`
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

func getTargetClient(config Config, err error) objectstorage.ObjectStorageClient {
	_target_configProvider := common.CustomProfileConfigProvider(config.ConfigPath, config.Target.Profilename)
	_target_objectStorageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(_target_configProvider)
	if err != nil {
		fmt.Println("Error creating Object Storage client:", err)
		os.Exit(1)
	}
	return _target_objectStorageClient
}

type ConnectionObj struct {
	SourceClient objectstorage.ObjectStorageClient
	TargetClient objectstorage.ObjectStorageClient
	Config       Config
	NameSpace    string
}

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

func getnamespace(ctx context.Context, c objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	r, err := c.GetNamespace(ctx, request)
	helpers.FatalIfError(err)
	fmt.Println("getting namespace")
	return *r.Value
}
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
