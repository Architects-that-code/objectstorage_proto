

# Current version
VERSION ?= proto-v1.0.0

# Default Go linker flags
GO_LDFLAGS := -X=main.version=$(VERSION) -w -s

# Project Directory
DIR := src_go

# Name of the application
APP := oci-toolkit-object-storage



.PHONY: mac-amd64
mac-amd64:
	GOOS=darwin GOARCH=amd64 go build -C $(DIR) -ldflags='$(GO_LDFLAGS)' -o ../dist/$(APP)-$(VERSION)-mac-amd64 .

.PHONY: all
all: windows-amd64 linux-amd64 linux-arm64 mac-amd64 mac-arm copy_file

.PHONY: clean
clean:
	rm  -f ./dist/*

copy_file:
	cp deltaconfig.sample.yaml dist/deltaconfig.yaml

windows-amd64:
	GOOS=windows GOARCH=amd64 go build -C $(DIR) -ldflags='$(GO_LDFLAGS)' -o ../dist/$(APP)-$(VERSION)-windows-amd64 .

linux-amd64:
	GOOS=linux GOARCH=amd64 go build -C $(DIR) -ldflags='$(GO_LDFLAGS)' -o ../dist/$(APP)-$(VERSION)-linux-amd64 .

linux-arm64:
	GOOS=linux GOARCH=arm64 go build -C $(DIR) -ldflags='$(GO_LDFLAGS)' -o ../dist/$(APP)-$(VERSION)-linux-arm64 .

mac-arm:
	GOOS=darwin GOARCH=arm64 go build -C $(DIR) -ldflags='$(GO_LDFLAGS)' -o ../dist/$(APP)-$(VERSION)-mac-arm .