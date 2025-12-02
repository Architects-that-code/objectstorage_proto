# OCI Toolkit for Object Storage

## Overview

This project is a prototype toolkit for working with Oracle Cloud Infrastructure (OCI) Object Storage. It provides utilities for tasks such as listing objects, computing deltas between buckets, generating test files, renaming objects, checking replication setups, and more. 

**Disclaimer:** This code is NOT production-ready and is intended for prototyping and testing concepts related to Object Storage replication and management. Use with caution, especially operations that modify or delete data.

Key features:
- Compute differences (deltas) between source and target buckets and optionally sync by touching or deleting objects.
- Generate large numbers of test files for replication testing.
- Rename/move objects within a bucket.
- Check bucket sizes, replication policies, and network paths (service gateway vs. internet).
- Preflight checks for read limits.

## Setup

### Prerequisites
- Go 1.23+ (see go.mod for details)
- OCI SDK for Go (`go get github.com/oracle/oci-go-sdk/v65`)
- Configuration file: Copy `deltaconfig.sample.yaml` to `deltaconfig.yaml` and fill in your OCI details.
- OCI credentials configured (supports instance principal or custom profiles).

### Installation
1. Clone the repository:
   ```
   git clone https://github.com/Architects-that-code/objectstorage_proto.git
   cd objectstorage_proto
   ```

2. Install dependencies:
   ```
   go mod tidy
   ```

3. Build the binary:
   ```
   make build
   ```
   Or run directly with `go run src_go/main.go`.

## Configuration

Edit `deltaconfig.yaml`:
```yaml
source:
  profilename: "your-profile"  # OCI config profile name
  bucketname: "source-bucket"
  region: "us-ashburn-1"
target:
  profilename: "your-profile"
  bucketname: "target-bucket"
  region: "us-phoenix-1"
home_region: "us-ashburn-1"
deltaupdate: true  # Enable delta syncing
batchsize: 100
limit: 1000
progressinterval: 100
maxconcurrency: 10
force_source_delete: false
force_source_refresh: false
configpath: "~/.oci/config"  # Path to OCI config file
useinstanceprincipal: false
renamer-maxworker: 10  # Concurrency for renamer
maker-numfile: 5000  # Number of files to generate
maker-maxfilesize: 10485760  # Max file size in bytes (10MB)
```

## Building and Running

- Build: `make build`
- Run: `./oci-toolkit-object-storage` (or `go run src_go/main.go`)

Upon running, select an option from the menu:
1. GetReader: List objects in source and target buckets.
2. GetRenamer: Move objects to a new subfolder (caution: modifies data).
3. GetDelta: Compute delta and optionally sync.
4. GetMaker: Generate test files in source bucket (caution: creates many objects).
5. GetPreflight: Test read limits.
6. CheckPath: Determine Object Storage endpoint type.
7. GetSizes: Get approximate object counts and replication info.
8. GetSingleReader: List objects from source bucket only.
9. SWAPPING: Analyze replication setup (in development).

## Packages

- **core**: Handles configuration loading, client creation, and common Object Storage operations like listing objects and getting counts.
- **util**: Utility functions for printing banners.
- **reader**: Functions for listing and counting objects in buckets.
- **maker**: Generates and uploads random test files.
- **delta**: Computes differences between buckets and performs sync operations.
- **preflight**: Tests API read limits.
- **renamer**: Renames/moves objects within a bucket.
- **swapper**: Analyzes replication setups.
- **stuff**: Miscellaneous utilities like endpoint detection.

For detailed documentation, see GoDoc comments in the source files.

## Examples

### Listing Objects
Select option 1 to list and count objects in source and target.

### Generating Test Files
Configure `maker-numfile` and select option 4. This will create files in the source bucket.

### Computing Delta
Select option 3. If `deltaupdate: true`, it will touch delta objects to trigger replication.

## TODOs and Potential Refactors
- Add more robust error handling and retries in all operations.
- Implement full swapping logic in swapper package (currently only analyzes).
- Add unit tests for all packages.
- Support multipart uploads for large file generation in maker.
- Optimize memory usage for large buckets in reader and delta.
- Consider adding CLI flags instead of menu for better automation.
- Refactor concurrent operations to use context for cancellation.
- Explore integrating with OCI CLI for authentication.

## Lint and Vet Checks
Running `go vet ./...` reports no issues.
Running `golint ./...` suggests minor style improvements, such as consistent naming and additional comments.

For contributions or issues, see the GitHub repository.
