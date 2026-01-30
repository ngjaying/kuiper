BUILD_PATH ?= _build
PACKAGES_PATH ?= _packages

VERSION := $(shell git describe --tags --always)
ARCH := $(shell go env GOARCH)
OS := $(shell go env GOOS)
PACKAGE_NAME := kuiper-$(VERSION)-$(OS)-$(ARCH)
GO              := GO111MODULE=on go

FAILPOINT_ENABLE  := find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/failpoint/bin/failpoint-ctl enable
FAILPOINT_DISABLE := find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/failpoint/bin/failpoint-ctl disable

TARGET ?= lfedge/ekuiper

export KUIPER_SOURCE := $(shell pwd)

# Get cached eKuiper dir
MODULE := github.com/lf-edge/ekuiper/v2
GO_MOD_CACHE := $(shell go env GOMODCACHE)

# Get the effective module and version, handling replacement output
EK_INFO := $(shell go list -m $(MODULE) | awk '{ if ($$3 == "=>") print $$4 " " $$5; else print $$1 " " $$2 }')
EK_MODULE := $(word 1,$(EK_INFO))
EK_VERSION := $(word 2,$(EK_INFO))

# Use MODULE for EK_DIR, as Go caches under the original path
EK_DIR := $(GO_MOD_CACHE)/$(EK_MODULE)@$(EK_VERSION)

# Debugging output
print-vars:
	@echo "MODULE: $(MODULE)"
	@echo "GO_MOD_CACHE: $(GO_MOD_CACHE)"
	@echo "EK_MODULE: $(EK_MODULE)"
	@echo "EK_VERSION: $(EK_VERSION)"
	@echo "EK_DIR: $(EK_DIR)"

.PHONY: build
build: build_ex

.PHONY:pkg
pkg: pkg_ex
	@if [ "$$(uname -s)" = "Linux" ]; then make -C deploy/packages; fi

.PHONY: build_prepare
build_prepare:
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/services
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/services/schemas
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/data
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/functions
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/portable
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/log

META_IN_EX := \
	etc/sinks/file.json \
	etc/sinks/log.json \
	etc/sinks/memory.json \
	etc/sinks/mqtt.json \
	etc/sinks/neuron.json \
	etc/sinks/nop.json \
	etc/sinks/redis.json \
	etc/sinks/redisPub.json \
	etc/sinks/rest.json \
	etc/sinks/websocket.json \
	extensions/sinks/sql/sql.json \
	extensions/sinks/kafka/kafka.json \
	extensions/sinks/image/image.json \
	extensions/sinks/influx/influx.json \
	extensions/sinks/influx2/influx2.json \
	etc/sources/file.json \
	etc/sources/file.yaml \
	etc/sources/httppull.json \
	etc/sources/httppull.yaml \
	etc/sources/httppush.json \
	etc/sources/httppush.yaml \
	etc/sources/memory.json \
	etc/sources/memory.yaml \
	etc/sources/neuron.json \
	etc/sources/neuron.yaml \
	etc/sources/redis.json \
	etc/sources/redis.yaml \
	etc/sources/redisSub.json \
	etc/sources/redisSub.yaml \
	etc/sources/simulator.json \
	etc/sources/simulator.yaml \
	etc/sources/websocket.json \
	etc/sources/websocket.yaml \
	extensions/sources/sql/sql.yaml \
	extensions/sources/sql/sql.json \
	extensions/sources/kafka/kafka.yaml \
	extensions/sources/kafka/kafka.json \
	extensions/sources/video/video.yaml \
	extensions/sources/video/video.json

EXT_IN_EX := \
	sources/can.yaml \
	sources/can.json

.PHONY: build_ex
build_ex: SHELL:=/bin/bash -euo pipefail
build_ex: build_prepare
	@echo "Compiling"
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/v2/cmd.Version=$(VERSION)_ex -X github.com/lf-edge/ekuiper/v2/cmd.LoadFileType=relative" -tags "ex script" -o kuiperd main.go
	@if [ "$$(uname -s)" = "Linux" ] && [ ! -z $$(which upx) ]; then upx ./kuiperd; fi
	@mv ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Overwrite etc from LF"
	@rsync -a --chmod=+w --exclude='sources/' --exclude='sinks/' $(EK_DIR)/etc/ $(BUILD_PATH)/$(PACKAGE_NAME)/etc/
	@echo $(META_IN_EX) | tr ' ' '\n' | while read meta; do \
		echo "Meta: $${meta}"; \
		file_path=$(EK_DIR)/$${meta}; \
		if [[ $${meta} == extensions/* ]]; then \
			new_path=$$(echo $${meta} | sed 's/^extensions/etc/' | awk -F'/' '{print $$1 "/" $$2 "/" $$4}'); \
		else \
			new_path=$${meta}; \
		fi; \
		echo "New path: $${new_path}"; \
		if [ "$$(basename $${meta} | cut -d. -f2)" = "json" ]; then \
		  rm -f $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path}; \
		  jq -c . $${file_path} > $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path};\
		else \
		  cp -f $${file_path} $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path}; \
		fi \
	done
	@echo "Overwrite etc from ext"
	@echo $(EXT_IN_EX) | tr ' ' '\n' | while read plugin; do \
		file_path=etc/$${plugin}; \
		if [ "$$(basename $${plugin} | cut -d. -f2)" = "json" ]; then \
		  rm -f $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path}; \
		  jq -c . $${file_path} > $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path};\
		else \
		  cp -f $${file_path} $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path}; \
		fi \
	done
	@rsync -av --checksum etc_ex/ $(BUILD_PATH)/$(PACKAGE_NAME)/etc/
	@echo "Build successfully"

.PHONY: pkg_ex
pkg_ex: build_ex
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME)-ex.tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME)-ex.tar.gz $(PACKAGES_PATH)
	@echo "Package for Neuron EX success"

META_IN_SDV := \
	etc/sinks/file.json \
	etc/sinks/log.json \
	etc/sinks/memory.json \
	etc/sinks/mqtt.json \
	etc/sinks/neuron.json \
	etc/sinks/nop.json \
	etc/sinks/rest.json \
	etc/sinks/websocket.json \
	etc/sources/file.json \
	etc/sources/file.yaml \
	etc/sources/httppull.json \
	etc/sources/httppull.yaml \
	etc/sources/httppush.json \
	etc/sources/httppush.yaml \
	etc/sources/memory.json \
	etc/sources/memory.yaml \
	etc/sources/neuron.json \
	etc/sources/neuron.yaml \
	etc/sources/simulator.json \
	etc/sources/simulator.yaml \
	etc/sources/websocket.yaml \
	extensions/sources/video/video.yaml \
	extensions/sources/video/video.json

EXT_IN_SDV := \
	sources/can.yaml \
	sources/can.json \
	sources/nano.yaml \
	sources/nanoquery.yaml

.PHONY: build_sdv
build_sdv: SHELL:=/bin/bash -euo pipefail
build_sdv: build_prepare
	@echo "Compiling"
	GO111MODULE=on CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/v2/cmd.Version=$(VERSION)_sdv -X github.com/lf-edge/ekuiper/v2/cmd.LoadFileType=relative" -tags "core sdv compression ui prometheus template schema" -o kuiperd main.go
	@mv ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Overwrite etc from LF"
	@rsync -a --chmod=+w --exclude='sources/' --exclude='sinks/' $(EK_DIR)/etc/ $(BUILD_PATH)/$(PACKAGE_NAME)/etc/
	@echo $(META_IN_SDV) | tr ' ' '\n' | while read meta; do \
        echo "Meta: $${meta}"; \
        file_path=$(EK_DIR)/$${meta}; \
        if [[ $${meta} == extensions/* ]]; then \
            new_path=$$(echo $${meta} | sed 's/^extensions/etc/' | awk -F'/' '{print $$1 "/" $$2 "/" $$4}'); \
        else \
            new_path=$${meta}; \
        fi; \
        echo "New path: $${new_path}"; \
        if [ "$$(basename $${meta} | cut -d. -f2)" = "json" ]; then \
		  rm -f $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path}; \
		  jq -c . $${file_path} > $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path};\
		else \
		  cp -f $${file_path} $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path}; \
		fi \
    done
	@echo "Overwrite etc from ext"
	@echo $(EXT_IN_SDV) | tr ' ' '\n' | while read plugin; do \
		file_path=etc/$${plugin}; \
		if [ "$$(basename $${plugin} | cut -d. -f2)" = "json" ]; then \
		  rm -f $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path}; \
		  jq -c . $${file_path} > $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path};\
		else \
		  cp -f $${file_path} $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path}; \
		fi \
	done
	@rsync -av --checksum etc_sdv/ $(BUILD_PATH)/$(PACKAGE_NAME)/etc/
	@echo "Build successfully"

.PHONY: pkg_sdv
pkg_sdv: build_sdv
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME)-sdv.tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME)-sdv.tar.gz $(PACKAGES_PATH)
	@echo "Package for SDV flow success"

.PHONY: build_fvt
build_fvt: SHELL:=/bin/bash -euo pipefail
build_fvt: build_prepare
	@echo "Compiling"
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/v2/cmd.Version=$(VERSION) -X github.com/lf-edge/ekuiper/v2/cmd.LoadFileType=relative" -tags "core compression ui prometheus sdv ex schema $(EXTRA_TAGS)" -o kuiperd main.go
	@mv ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Overwrite etc from LF"
	@rsync -a --chmod=+w --exclude='sources/' --exclude='sinks/' $(EK_DIR)/etc/ $(BUILD_PATH)/$(PACKAGE_NAME)/etc/
	@echo $(META_IN_SDV) | tr ' ' '\n' | while read meta; do \
		echo "Meta: $${meta}"; \
		file_path=$(EK_DIR)/$${meta}; \
		if [[ $${meta} == extensions/* ]]; then \
			new_path=$$(echo $${meta} | sed 's/^extensions/etc/' | awk -F'/' '{print $$1 "/" $$2 "/" $$4}'); \
		else \
			new_path=$${meta}; \
		fi; \
		echo "New path: $${new_path}"; \
		if [ "$$(basename $${meta} | cut -d. -f2)" = "json" ]; then \
		  rm -f $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path}; \
		  jq -c . $${file_path} > $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path};\
		else \
		  cp -f $${file_path} $(BUILD_PATH)/$(PACKAGE_NAME)/$${new_path}; \
		fi \
	done
	@echo "Overwrite etc from ext"
	@echo $(EXT_IN_SDV) | tr ' ' '\n' | while read plugin; do \
		file_path=etc/$${plugin}; \
		if [ "$$(basename $${plugin} | cut -d. -f2)" = "json" ]; then \
		  rm -f $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path}; \
		  jq -c . $${file_path} > $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path};\
		else \
		  cp -f $${file_path} $(BUILD_PATH)/$(PACKAGE_NAME)/$${file_path}; \
		fi \
	done
	@rsync -av --checksum etc_sdv/ $(BUILD_PATH)/$(PACKAGE_NAME)/etc/
	@echo "Build successfully"

.PHONY: real_pkg
real_pkg:
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME).tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME).tar.gz $(PACKAGES_PATH)
	@echo "Package build success"

.PHONY: clean
clean:
	@rm -rf cross_build.tar linux_amd64 linux_arm64 linux_arm_v7 linux_386
	@rm -rf _build _packages _plugins

tidy:
	@echo "go mod tidy"
	go mod tidy && git diff go.mod go.sum