cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME dbn)

find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

# Reuse databento-cpp's type definitions (TradeMsg, RecordHeader, RType, Action,
# Side, FlagSet) directly from its headers. We do NOT add_subdirectory the
# upstream CMake project — its decoder source pulls in cpp-httplib/json/date/zstd
# transitive deps that fight DuckDB's namespaced zstd. Instead, this Phase 1
# walking skeleton implements a small native file parser in src/dbn_native_decoder
# against those same struct layouts. Phase 2+ widens the rtype switch.
set(DATABENTO_CPP_INCLUDE_DIR
    ${CMAKE_CURRENT_LIST_DIR}/third_party/databento-cpp/include)

# databento-cpp's constants.hpp pulls in <databento/system.hpp> and
# <databento/version.hpp>, both of which are configure_file'd by upstream from
# cmake/*.hpp.in templates. We replicate that step here so the generated stubs
# end up on our include path. Keep PROJECT_VERSION_* synced with the submodule
# (third_party/databento-cpp/CMakeLists.txt:9 → currently 0.59.0).
set(DBN_DATABENTO_GENERATED_INC
    ${CMAKE_BINARY_DIR}/extension/dbn/databento_generated_include)
set(_dbn_saved_project_version       "${PROJECT_VERSION}")
set(_dbn_saved_project_version_major "${PROJECT_VERSION_MAJOR}")
set(_dbn_saved_project_version_minor "${PROJECT_VERSION_MINOR}")
set(_dbn_saved_project_version_patch "${PROJECT_VERSION_PATCH}")
set(PROJECT_VERSION       "0.59.0")
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 59)
set(PROJECT_VERSION_PATCH 0)
configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/third_party/databento-cpp/cmake/system.hpp.in
    ${DBN_DATABENTO_GENERATED_INC}/databento/system.hpp
    @ONLY)
configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/third_party/databento-cpp/cmake/version.hpp.in
    ${DBN_DATABENTO_GENERATED_INC}/databento/version.hpp
    @ONLY)
set(PROJECT_VERSION       "${_dbn_saved_project_version}")
set(PROJECT_VERSION_MAJOR "${_dbn_saved_project_version_major}")
set(PROJECT_VERSION_MINOR "${_dbn_saved_project_version_minor}")
set(PROJECT_VERSION_PATCH "${_dbn_saved_project_version_patch}")

set(EXTENSION_SOURCES
    src/dbn_extension.cpp
    src/dbn_native_decoder.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# DuckDB's build_*_extension macros don't propagate CMAKE_CXX_STANDARD; databento-cpp's
# headers use std::string_view, std::optional, std::byte (all C++17). Force C++17 explicitly.
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)
target_compile_features(${LOADABLE_EXTENSION_NAME} PRIVATE cxx_std_17)

# Header-only consumption of databento-cpp's public types.
# databento_stubs ships minimal <httplib.h> and <nlohmann/json.hpp> shims so
# we can compile exceptions.hpp (transitively included via record.hpp) without
# pulling in the real HTTP client / JSON library — the exception class bodies
# live in databento-cpp's .cpp files which we don't compile.
set(DBN_DATABENTO_STUBS_DIR ${CMAKE_CURRENT_LIST_DIR}/third_party/databento_stubs)
target_include_directories(${EXTENSION_NAME} PRIVATE
    ${DATABENTO_CPP_INCLUDE_DIR}
    ${DBN_DATABENTO_GENERATED_INC}
    ${DBN_DATABENTO_STUBS_DIR})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE
    ${DATABENTO_CPP_INCLUDE_DIR}
    ${DBN_DATABENTO_GENERATED_INC}
    ${DBN_DATABENTO_STUBS_DIR})

target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
