package delta

import (
	"context"
	"fmt"

	"log"
	"sync"

	"myworkspace/core"
	"myworkspace/reader"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func GetDelta(connobj core.ConnectionObj) {
	// use common connections to get config

	reader.GetSizes(connobj)

	var wg sync.WaitGroup
	wg.Add(2)

	var sourceHasDelta bool

	config := connobj.Config

	namespace := connobj.NameSpace

	// now call the ListObjectsInBucket function in parallel
	sourceObjectsCh := make(chan []objectstorage.ObjectSummary)
	targetObjectsCh := make(chan []objectstorage.ObjectSummary)
	errCh := make(chan error, 2)

	go core.ListObjectsInBucket(connobj.NameSpace, connobj.Config.Source.Bucketname, connobj.SourceClient, &wg, sourceObjectsCh, errCh)
	go core.ListObjectsInBucket(connobj.NameSpace, connobj.Config.Target.Bucketname, connobj.TargetClient, &wg, targetObjectsCh, errCh)

	go func() {
		wg.Wait()
		close(sourceObjectsCh)
		close(targetObjectsCh)
		close(errCh)
	}()
	var sourceObjects, targetObjects []objectstorage.ObjectSummary
	///
	for {
		select {
		case obj, ok := <-sourceObjectsCh:
			if !ok {
				sourceObjectsCh = nil
				break
			}
			sourceObjects = append(sourceObjects, obj...)
		case obj, ok := <-targetObjectsCh:
			if !ok {
				targetObjectsCh = nil
				break
			}
			targetObjects = append(targetObjects, obj...)
		case err := <-errCh:
			fmt.Println("Error listing objects in bucket:", err)
		}
		if sourceObjectsCh == nil && targetObjectsCh == nil {
			break
		}
	}
	fmt.Println("Found Source ", len(sourceObjects), "objects")
	fmt.Println("Found Target ", len(targetObjects), "objects")

	////

	_source_objects := sourceObjects
	_target_objects := targetObjects

	if _source_objects == nil || _target_objects == nil {
		return
	}

	_source_foundObjects := make(map[string]*objectstorage.ObjectSummary)
	_target_foundObjects := make(map[string]*objectstorage.ObjectSummary)
	_sync_objts := make(map[string]*objectstorage.ObjectSummary)

	// Add all source into source found map
	for _, ObjectSummary := range _source_objects {
		ObjectSummary := ObjectSummary
		_key := ObjectSummary.Name
		//log.Println("source adding", *_key)
		_source_foundObjects[*_key] = &ObjectSummary
	}

	// Add all target into target found map
	for _, ObjectSummary := range _target_objects {
		ObjectSummary := ObjectSummary
		_key := ObjectSummary.Name
		//log.Println("target adding", *_key)
		_target_foundObjects[*_key] = &ObjectSummary
	}

	// now determine of there are objects in _source_objects that are not in _target_objects
	//log.Printf("%+v\n", _target_foundObjects)

	//var diff []objectstorage.ObjectSummary
	for index, ObjectSummary := range _source_foundObjects {
		ObjectSummary := ObjectSummary
		//_key := ObjectSummary.Name
		_, ok := _target_foundObjects[index]
		if ok {
			//log.Println("processing index found", index, *_key)
		} else {
			//log.Println("processing index not found", index, *_key)
			_sync_objts[index] = ObjectSummary
			sourceHasDelta = true
		}

	}
	log.Printf("comparing SOURCE bucket:%v with TARGET bucket:%v", config.Source.Bucketname, config.Target.Bucketname)
	log.Println("SOURCE Found", len(_source_foundObjects), "objects")
	log.Println("TargetNotOnSource Found", len(_target_foundObjects), "objects")
	log.Printf("sourceHasDelta: %+v\n", sourceHasDelta)
	log.Printf("config.DeltaUpdate: %+v\n", config.DeltaUpdate)
	log.Printf("config.ForceSourceDelete: %+v\n", config.ForceSourceDelete)
	log.Printf("config.ForceSourceRefresh: %+v\n", config.ForceSourceRefresh)
	log.Println("to be synced: ", len(_sync_objts), " objects")

	//

	if config.ForceSourceRefresh && !config.ForceSourceDelete {
		log.Println("forcing source update")
		NewSimpleUpdate(_source_foundObjects, connobj.SourceClient, namespace, connobj, false)
	} else {
		if sourceHasDelta && config.DeltaUpdate {
			log.Println("touching delta objects on source ")
			NewSimpleUpdate(_sync_objts, connobj.SourceClient, namespace, connobj, config.ForceSourceDelete)
		} else {
			if config.ForceSourceDelete {
				log.Println("forcing source delete-purge")
				NewSimpleUpdate(_source_foundObjects, connobj.SourceClient, namespace, connobj, connobj.Config.ForceSourceDelete)
			}
			log.Println("skipped sync  - end")
		}
	}

	// print size of foundObjects map
	log.Println("SOURCE Found", len(_source_foundObjects), "objects")
	log.Println("TargetNotOnSource Found", len(_target_foundObjects), "objects")
	log.Printf("sourceHasDelta: %+v\n", sourceHasDelta)
	log.Printf("config.DeltaUpdate: %+v\n", config.DeltaUpdate)
	log.Printf("config.ForceSourceDelete: %+v\n", config.ForceSourceDelete)
	log.Printf("config.ForceSourceRefresh: %+v\n", config.ForceSourceRefresh)
	log.Println("to be synced: ", len(_sync_objts), " objects")
	//log.Printf("%+v\n", _sync_objts)
}

func NewSimpleUpdate(things map[string]*objectstorage.ObjectSummary, client objectstorage.ObjectStorageClient, ns string, config core.ConnectionObj, delete bool) {
	log.Printf("Forcing delete: %+v\n", delete)

	maxWorkers := config.Config.RenamerMaxWorker

	var actionStr string
	if delete {
		actionStr = "delete"
	} else {
		actionStr = "Update"
	}
	var wg sync.WaitGroup

	// Create a channel to receive errors from goroutines
	errCh := make(chan error)
	workerCh := make(chan struct{}, maxWorkers)

	for _, objSum := range things {
		wg.Add(1)

		// Acquire a worker slot from the channel
		workerCh <- struct{}{}
		go func(obj objectstorage.ObjectSummary) {
			defer func() {
				// Release the worker slot back to the channel
				<-workerCh

				// Notify the wait group that the goroutine has finished
				wg.Done()
			}()

			if !delete {
				err := process(ns, config, objSum, client, 0)
				if err != nil {
					errCh <- err
				}
			} else {
				err := deletes(ns, config, objSum, client, 0)
				if err != nil {
					errCh <- err
				}
			}

			log.Printf("Processed (%+v) %v", actionStr, &objSum.Name)
		}(*objSum)
	}

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

func SimpleUpdate(things map[string]*objectstorage.ObjectSummary, client objectstorage.ObjectStorageClient, ns string, config core.ConnectionObj, delete bool) {
	log.Printf("Forcing delete: %+v\n", delete)
	var actionStr string
	if delete {
		actionStr = "delete"
	} else {
		actionStr = "Update"
	}
	batchSize := config.Config.BatchSize
	limit := config.Config.Limit
	progressInterval := config.Config.ProgressInterval

	batches := divideIntoBatches(things, batchSize, limit)
	log.Printf("number in things: %d \n", len(things))
	if len(things) < limit {
		limit = len(things)
	}
	log.Printf("Processing up to %d batches of %d objects each\n", len(batches), batchSize)
	counter := 0

	// Create a buffered channel to limit concurrency
	concurrencyCh := make(chan struct{}, config.Config.MaxConcurrency)

	for i, batch := range batches {
		// Block sending to the channel when there are already MaxConcurrency active goroutines
		concurrencyCh <- struct{}{}
		go func(batch []*objectstorage.ObjectSummary, batchIndex int) {
			defer func() {
				// Read from the channel to signal that this goroutine has finished processing
				<-concurrencyCh
			}()
			for _, objSum := range batch {
				if !delete {
					process(ns, config, objSum, client, 0)
				} else {
					deletes(ns, config, objSum, client, 0)
				}
				counter++
				if counter%progressInterval == 0 {
					log.Printf("Processed (%+v) %d/%d objects in batch %d\n", actionStr, counter, limit, batchIndex+1)
				}
				if counter >= limit {
					return
				}
			}
		}(batch, i)
	}
	// Wait for all goroutines to finish
	for i := 0; i < config.Config.MaxConcurrency; i++ {
		concurrencyCh <- struct{}{}
	}
}

func divideIntoBatches(things map[string]*objectstorage.ObjectSummary, batchSize int, limit int) [][]*objectstorage.ObjectSummary {
	numBatches := (limit + batchSize - 1) / batchSize
	if numBatches > len(things) {
		numBatches = len(things)
	}
	batches := make([][]*objectstorage.ObjectSummary, numBatches)
	i := 0
	for _, objSum := range things {
		if i == numBatches {
			i = 0
		}
		batches[i] = append(batches[i], objSum)
		i++
	}
	return batches
}

func process(ns string, config core.ConnectionObj, objSum *objectstorage.ObjectSummary, client objectstorage.ObjectStorageClient, index int) error {

	defaultRetryPolicy := common.DefaultRetryPolicy()

	coR := objectstorage.CopyObjectRequest{

		NamespaceName: &ns,
		BucketName:    &config.Config.Source.Bucketname,

		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      objSum.Name,
			DestinationRegion:     &config.Config.Source.Region,
			DestinationNamespace:  &ns,
			DestinationBucket:     &config.Config.Source.Bucketname,
			DestinationObjectName: objSum.Name},
	}
	coR.RequestMetadata = common.RequestMetadata{
		RetryPolicy: &defaultRetryPolicy,
	}

	_, err := client.CopyObject(context.Background(), coR)
	if err != nil {
		log.Println("Error Copying Object:", err)
		return err
	}
	return nil
}

func deletes(ns string, config core.ConnectionObj, objSum *objectstorage.ObjectSummary, client objectstorage.ObjectStorageClient, index int) error {
	defaultRetryPolicy := common.DefaultRetryPolicy()
	doR := objectstorage.DeleteObjectRequest{
		NamespaceName: &ns,
		BucketName:    &config.Config.Source.Bucketname,
		ObjectName:    objSum.Name,
	}

	doR.RequestMetadata = common.RequestMetadata{
		RetryPolicy: &defaultRetryPolicy,
	}

	_, err := client.DeleteObject(context.Background(), doR)
	if err != nil {
		log.Println("Error Copying Object:", err)
		return err
	}
	return nil
}
