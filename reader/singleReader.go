package reader

import (
	"log"
	"myworkspace/core"
)

// This function returns a list of all objects in a given bucket

func GetSourceOnlyReader(connobj core.ConnectionObj) {

	GetSizes(connobj)
	objects := core.ListObjectsInSingleBucket(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient)
	if objects != nil {
		log.Printf("number of objects in bucket %s is %d", connobj.Config.Source.Bucketname, len(objects))
	}
}
