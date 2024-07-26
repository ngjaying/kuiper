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
MODULE_PATH := github.com/lf-edge/ekuiper/v2
GO_MOD_CACHE := $(shell go env GOMODCACHE)
EK_VERSION := $(shell go list -m $(MODULE_PATH) | awk -F' ' '{print $$2}')
EK_DIR := $(GO_MOD_CACHE)/$(MODULE_PATH)@$(EK_VERSION)

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

PLUGINS_IN_EX := \
	extensions/sinks/sql   \
	extensions/sources/random \
	extensions/sources/sql \
	extensions/sources/video

.PHONY: build_ex
build_ex: SHELL:=/bin/bash -euo pipefail
build_ex: build_prepare
	@echo "Compiling"
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/v2/cmd.Version=$(VERSION)_ex -X github.com/lf-edge/ekuiper/v2/cmd.LoadFileType=relative" -tags "full" -o kuiperd *.go
	@if [ "$$(uname -s)" = "Linux" ] && [ ! -z $$(which upx) ]; then upx ./kuiperd; fi
	@mv ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Overwrite etc"
	@cp -rf $(EK_DIR)/etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@echo $(PLUGINS_IN_EX) | tr ' ' '\n' | while read plugin; do \
		full_plugin_dir=$(EK_DIR)/$${plugin}; \
		find $${full_plugin_dir} -type f \( -name "*.json" -o -name "*.yaml" \) | while read line; do \
			relative_path=$${line#$(EK_DIR)/}; \
			type=$$(echo $${relative_path} | cut -d'/' -f2); \
			cp -f $${line} $(BUILD_PATH)/$(PACKAGE_NAME)/etc/$${type}/$$(basename $${line}); \
		done; \
	done
	@cp -rf etc_sdv/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@echo "Build successfully"

.PHONY: pkg_ex
pkg_ex: build_ex
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME)-ex.tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME)-ex.tar.gz $(PACKAGES_PATH)
	@echo "Package for Neuron EX success"

PLUGINS_IN_SDV := \
	extensions/sources/video

.PHONY: build_sdv
build_sdv: SHELL:=/bin/bash -euo pipefail
build_sdv: build_prepare
	@echo "Compiling"
	GO111MODULE=on CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/v2/cmd.Version=$(VERSION)_sdv -X github.com/lf-edge/ekuiper/v2/cmd.LoadFileType=relative" -tags "core sdv compression ui prometheus template" -o kuiperd *.go
	@mv ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Overwrite etc"
	@cp -rf $(EK_DIR)/etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@echo $(PLUGINS_IN_SDV) | tr ' ' '\n' | while read plugin; do \
		full_plugin_dir=$(EK_DIR)/$${plugin}; \
		find $${full_plugin_dir} -type f \( -name "*.json" -o -name "*.yaml" \) | while read line; do \
			relative_path=$${line#$(EK_DIR)/}; \
			type=$$(echo $${relative_path} | cut -d'/' -f2); \
			cp -f $${line} $(BUILD_PATH)/$(PACKAGE_NAME)/etc/$${type}/$$(basename $${line}); \
		done; \
	done
	@cp -rf etc_sdv/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@echo "Build successfully"

.PHONY: pkg_sdv
pkg_sdv: build_sdv
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME)-sdv.tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME)-sdv.tar.gz $(PACKAGES_PATH)
	@echo "Package for SDV flow success"


.PHONY: build_geely
build_geely: SHELL:=/bin/bash -euo pipefail
build_geely: build_prepare
	@echo "Compiling"
	GO111MODULE=on CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/v2/cmd.Version=$(VERSION)_geely -X github.com/lf-edge/ekuiper/v2/cmd.LoadFileType=relative" -tags "core compression ui prometheus geely" -o kuiperd *.go
	@mv ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Overwrite etc"
	@cp -rf $(EK_DIR)/etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@echo $(PLUGINS_IN_SDV) | tr ' ' '\n' | while read plugin; do \
		full_plugin_dir=$(EK_DIR)/$${plugin}; \
		find $${full_plugin_dir} -type f \( -name "*.json" -o -name "*.yaml" \) | while read line; do \
			relative_path=$${line#$(EK_DIR)/}; \
			type=$$(echo $${relative_path} | cut -d'/' -f2); \
			cp -f $${line} $(BUILD_PATH)/$(PACKAGE_NAME)/etc/$${type}/$$(basename $${line}); \
		done; \
	done
	@cp -rf etc_geely/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/dbc/geely
	@cp dbc/geely/geely.json $(BUILD_PATH)/$(PACKAGE_NAME)/dbc/geely/
	@echo "Build successfully"

.PHONY: pkg_geely
pkg_geely: build_geely
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME)-geely.tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME)-geely.tar.gz $(PACKAGES_PATH)
	@echo "Package for Geely successful"

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

lint:tools/lint/bin/golangci-lint
	@echo "linting"
	tools/lint/bin/golangci-lint run ./... ./extensions/... ./tools/kubernetes/...
	cd sdk/go && ../../tools/lint/bin/golangci-lint run

tools/lint/bin/golangci-lint:
	GOBIN=tools/lint/bin go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

tools/failpoint/bin/failpoint-ctl:
	GOBIN=$(shell pwd)/tools/failpoint/bin $(GO) install github.com/pingcap/failpoint/failpoint-ctl@2eaa328

failpoint-enable: tools/failpoint/bin/failpoint-ctl
# Converting gofail failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable: tools/failpoint/bin/failpoint-ctl
# Restoring gofail failpoints...
	@$(FAILPOINT_DISABLE)
