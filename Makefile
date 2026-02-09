#-----------------------------------------------------------------------------
# RediSearch Makefile
#
# This Makefile acts as a thin wrapper around the build.sh script, providing
# backward compatibility for existing make targets while using build.sh for
# all actual build operations.
#-----------------------------------------------------------------------------

.NOTPARALLEL:
.EXPORT_ALL_VARIABLES:

MAKEFLAGS += --no-print-directory

ROOT := $(shell pwd)
BUILD_SCRIPT := $(ROOT)/build.sh

# Default target
.DEFAULT_GOAL := build

# Simple tasks to pass through to build script
build pack test-vecsim bench-vecsim: $(BUILD_SCRIPT)
	@$(BUILD_SCRIPT) $@

.PHONY: build pack test-vecsim bench-vecsim

upload-artifacts:
	@$(ROOT)/deps/RediSearch/sbin/upload-artifacts

.PHONY: upload-artifacts
