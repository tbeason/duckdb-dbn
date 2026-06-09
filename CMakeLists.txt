cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME dbn)

find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)

set(EXTENSION_SOURCES
    src/dbn_extension.cpp
    src/dbn_native_decoder.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# The DBN record-struct/enum definitions are vendored in src/include/databento
# (no external databento-cpp dependency). C++14 for std::make_unique; do NOT
# raise this to 17 — DuckDB core builds as C++11, and a C++17 TU emits inline-
# variable definitions for DuckDB's `static constexpr` members (e.g.
# LogicalType::VARCHAR) that collide at link time with the out-of-line
# definitions in libduckdb_static on GNU ld.
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_14)
target_compile_features(${LOADABLE_EXTENSION_NAME} PRIVATE cxx_std_14)

target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
