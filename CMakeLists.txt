cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME datadog)

# Native builds use OpenSSL through DuckDB's bundled cpp-httplib. DuckDB-WASM
# supplies its browser-backed HTTPUtil, so Emscripten builds intentionally
# avoid both OpenSSL and native sockets. JSON uses DuckDB's bundled yyjson in
# both builds.
if(NOT EMSCRIPTEN)
  find_package(OpenSSL REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD
    "17"
    CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Locate DuckDB's bundled third-party headers (httplib + yyjson).
set(DATADOG_DUCKDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/duckdb")
if(NOT EXISTS "${DATADOG_DUCKDB_DIR}/third_party/httplib/httplib.hpp")
  # When built from within the DuckDB tree the submodule may live one level up.
  set(DATADOG_DUCKDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../duckdb")
endif()

include_directories(src/include)

set(EXTENSION_SOURCES src/datadog_extension.cpp src/datadog_secret.cpp
                      src/datadog_client.cpp src/datadog_json.cpp
                      src/logs_table.cpp src/datadog_catalog.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

foreach(target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_include_directories(${target} PRIVATE
                             "${DATADOG_DUCKDB_DIR}/third_party/yyjson/include")
  if(NOT EMSCRIPTEN)
    target_include_directories(${target} PRIVATE
                               "${DATADOG_DUCKDB_DIR}/third_party/httplib")
    # Select cpp-httplib's OpenSSL-enabled (and separately-namespaced) build.
    target_compile_definitions(${target} PRIVATE CPPHTTPLIB_OPENSSL_SUPPORT)
    target_link_libraries(${target} OpenSSL::SSL OpenSSL::Crypto)
  endif()
endforeach()

# Small offline helper test for JSON discovery parsing and search request generation. It is
# intentionally a C++ test rather than a test-only SQL function, keeping internals out of the
# extension's public SQL surface.
add_executable(datadog_json_test EXCLUDE_FROM_ALL
                                 test/cpp/datadog_json_test.cpp
                                 src/datadog_json.cpp)
target_include_directories(
  datadog_json_test PRIVATE src/include
                             "${DATADOG_DUCKDB_DIR}/third_party/yyjson/include")
target_link_libraries(datadog_json_test dummy_static_extension_loader duckdb_static)
set_target_properties(
  datadog_json_test
  PROPERTIES RUNTIME_OUTPUT_DIRECTORY
             "${CMAKE_BINARY_DIR}/extension/datadog")

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
