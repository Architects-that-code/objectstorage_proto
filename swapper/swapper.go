package swapper

import (
	"log"

	"myworkspace/core"
	"myworkspace/reader"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func GetSwapper(connobj core.ConnectionObj) {

	//determine which is replication source and target
	// call getsize for source
	log.Println("Getting sizes for source buckets")
	s1 := reader.GetObjectCount(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient)
	log.Println("size of source bucket is: ", s1)

	hasReplPol(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient)

	log.Println("Getting sizes for target buckets")
	s2 := reader.GetObjectCount(connobj.NameSpace, connobj.Config.Target.Bucketname, connobj.TargetClient)
	log.Println("size of target bucket is: ", s2)

	hasReplPol(connobj.NameSpace, connobj.Config.Target.Bucketname, connobj.TargetClient)

	// call getsize for target

}

func hasReplPol(namespace string, bucketName string, client objectstorage.ObjectStorageClient) bool {
	policy, _ := reader.GetReplicationPolicy(namespace, bucketName, client)

	if policy.Name != nil {
		log.Println("bucket has replication policy")
		return true
	} else {
		log.Println("bucket does not have replication policy")
		return false
	}
}

//create struct that represents the source and target and who owns the replication policy
