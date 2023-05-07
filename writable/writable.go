package writable

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/example/helpers"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

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
	DeltaUpdate      bool `yaml:"deltaupdate"`
	BatchSize        int  `yaml:"batchsize"`
	Limit            int  `yaml:"limit"`
	ProgressInterval int  `yaml:"progressinterval"`
	MaxConcurrency   int  `yaml:"maxconcurrency"`
}

func main() {
	var sourceHasDelta bool
	data, err := ioutil.ReadFile("deltaconfig.yaml")
	if err != nil {
		// handle error
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		// handle error
	}

	//fmt.Printf("%+v\n", config)
	_source_configProvider := common.CustomProfileConfigProvider("/Users/jscanlon/.oci/config", config.Source.Profilename)
	_source_objectStorageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(_source_configProvider)
	if err != nil {
		fmt.Println("Error creating Object Storage client:", err)
		os.Exit(1)
	}

	_target_configProvider := common.CustomProfileConfigProvider("/Users/jscanlon/.oci/config", config.Target.Profilename)

	_target_objectStorageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(_target_configProvider)
	if err != nil {
		fmt.Println("Error creating Object Storage client:", err)
		os.Exit(1)
	}

	namespace := getnamespace(context.Background(), _source_objectStorageClient)

	// now call the ListObjectsInBucket function in parallel
	sourceObjectsCh := make(chan []objectstorage.ObjectSummary)
	targetObjectsCh := make(chan []objectstorage.ObjectSummary)

	go func() {
		_source_objects, err := ListObjectsInBucket(namespace, config.Source.Bucketname, _source_objectStorageClient)
		if err != nil {
			fmt.Println("Error listing objects in bucket:", err)
			sourceObjectsCh <- nil
		} else {
			sourceObjectsCh <- _source_objects
		}
	}()

	go func() {
		_target_objects, err := ListObjectsInBucket(namespace, config.Target.Bucketname, _target_objectStorageClient)
		if err != nil {
			fmt.Println("Error listing objects in bucket:", err)
			targetObjectsCh <- nil
		} else {
			targetObjectsCh <- _target_objects
		}
	}()

	_source_objects := <-sourceObjectsCh
	_target_objects := <-targetObjectsCh

	if _source_objects == nil || _target_objects == nil {
		return
	}

}

// This function returns a list of all objects in a given bucket
func (client objectstorage.ObjectStorageClient) MakeBucketWritable(ctx context.Context, request objectstorage.MakeBucketWritableRequest) (response MakeBucketWritableResponse, err error) {
	var ociResponse common.OCIResponse
	policy := common.DefaultRetryPolicy()
	if client.RetryPolicy() != nil {
		policy = *client.RetryPolicy()
	}
	if request.RetryPolicy() != nil {
		policy = *request.RetryPolicy()
	}
	ociResponse, err = common.Retry(ctx, request, client.makeBucketWritable, policy)
	if err != nil {
		if ociResponse != nil {
			if httpResponse := ociResponse.HTTPResponse(); httpResponse != nil {
				opcRequestId := httpResponse.Header.Get("opc-request-id")
				response = MakeBucketWritableResponse{RawResponse: httpResponse, OpcRequestId: &opcRequestId}
			} else {
				response = MakeBucketWritableResponse{}
			}
		}
		return
	}
	if convertedResponse, ok := ociResponse.(MakeBucketWritableResponse); ok {
		response = convertedResponse
	} else {
		err = fmt.Errorf("failed to convert OCIResponse into MakeBucketWritableResponse")
	}
	return
}
func getnamespace(ctx context.Context, c objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	r, err := c.GetNamespace(ctx, request)
	helpers.FatalIfError(err)
	fmt.Println("getting namespace")
	return *r.Value
}
