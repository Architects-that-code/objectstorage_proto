

# Current version
VERSION ?= v1.0.0

# Default Go linker flags
GO_LDFLAGS := -X=main.version=$(VERSION) -w -s

# Project Directory
DIR := src_go

# Name of the application
APP := oci-toolkit-object-storage

PLATFORMS :=  darwin linux windows
os = $(word 1, $@)

.PHONY: $(PLATFORMS) 
$(PLATFORMS): 
	GOOS=$(os) GOARCH=amd64 go build -C $(DIR) -ldflags='$(GO_LDFLAGS)' -o ../bin/$(APP)-$(VERSION)-$(os)-amd64 . 
	

.PHONY: all
all: windows linux darwin copy_file

.PHONY: clean
clean:
	rm  -f ./bin/*

copy_file:
	cp deltaconfig.sample.yaml bin/deltaconfig.yaml