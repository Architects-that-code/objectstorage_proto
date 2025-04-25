# objectstorage_proto

What are we trying to solve



ObjectStorage replication is a one way, point-forward action.
This means that if you have a bucket in one region, and you want to replicate it to another region, you can do that but any data that exists in the source bucket will not be replicated to the destination region.

Need a way to find delta of source and target
Need a way to find source files created before X date
Need a way to prototype a multi-sync solution (does not need to be replication service)
Need a way to generate lots of files for testing
Need a way to clean up


To get started:
configuration file: deltaconfig.yaml
 =- 'should be' pretty self explanatory - some global settings (auth, source target) - then some 'module specific ones'


 DISCLAIMER:  this code is NOT production ready, NOR is it intended to be.  It is a prototype to test out some ideas and concepts.

 SSH command to proxy thru self made basion
 ssh -o ProxyCommand="ssh -W %h:%p opc@150.136.138.165" opc@10.0.1.123  

 