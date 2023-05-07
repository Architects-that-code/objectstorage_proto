package core

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	

	"github.com/oracle/oci-go-sdk/v65/common"
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
	UseInstancePrincipal string `yaml:"useinstanceprincipal"`
	RenamerMaxWorker int `yaml:"renamer-maxworker"`
	MakerNumFiles int `yaml:"maker-numfile"`
	MakerMaxFileSize int `yaml:"maker-maxfilesize"`
}

func getSourceClient(config Config, err error) objectstorage.ObjectStorageClient {
	_source_configProvider := common.CustomProfileConfigProvider(config.ConfigPath, config.Source.Profilename)
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
func ListObjectsInBucket(namespace string, bucketName string, objectStorageClient objectstorage.ObjectStorageClient) ([]objectstorage.ObjectSummary, error) {
	fmt.Printf("getting data from bucket:%v in   %v \n",bucketName , objectStorageClient.Host)
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
