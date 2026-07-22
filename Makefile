PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=datadog
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test_datadog_json
test: test_datadog_json

test_datadog_json: release
	cmake --build build/release --config Release --target datadog_json_test
	./build/release/extension/datadog/datadog_json_test
