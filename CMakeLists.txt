cmake_minimum_required(VERSION 3.18)

# Keep the benchmark toolchain as the default; allow CC/CXX or a toolchain file.
if(NOT DEFINED CMAKE_C_COMPILER AND NOT DEFINED ENV{CC})
    set(CMAKE_C_COMPILER clang-17)
endif()
if(NOT DEFINED CMAKE_CXX_COMPILER AND NOT DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER clang++-17)
endif()
project(borges LANGUAGES C CXX)

set(CMAKE_C_STANDARD 17)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(ABSL_PROPAGATE_CXX_STD ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Preserve the evaluated benchmark's optimization and instruction-set flags.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -pthread -Wall -fno-omit-frame-pointer -pedantic -O3 -DNDEBUG -g3 -Wno-long-long -Wno-unused-variable \
                    -Wno-unused-but-set-variable -Wno-variadic-macros -Wno-unused-lambda-capture -Wno-c99-extensions \
                    -Wno-zero-length-array -Wno-deprecated-declarations -mavx2 -mclflushopt -mclwb")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -O3 -no-pie -fuse-ld=lld-17 -static-libstdc++")

find_package(Threads REQUIRED)
find_library(jemalloc_lib jemalloc REQUIRED)
find_package(spdlog REQUIRED)
find_library(ROARING_LIB roaring REQUIRED)

# The default matches the configuration uploaded by scripts/run.sh. A separate
# file permits building another backend without editing the runtime defaults.
set(RHODES_CONFIG_FILE "${CMAKE_SOURCE_DIR}/src/common/config.json" CACHE FILEPATH
    "JSON configuration used to select the shared-memory backend")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${RHODES_CONFIG_FILE}")
set(USE_RDMA OFF)
if(NOT EXISTS "${RHODES_CONFIG_FILE}")
    message(FATAL_ERROR "Missing config file: ${RHODES_CONFIG_FILE}")
endif()
file(READ "${RHODES_CONFIG_FILE}" RHODES_CONFIG_JSON)
if(RHODES_CONFIG_JSON MATCHES "\"use_rdma\"[ \t\r\n]*:[ \t\r\n]*(true|false)")
    if(CMAKE_MATCH_1 STREQUAL "true")
        set(USE_RDMA ON)
    endif()
else()
    message(STATUS "use_rdma not found in ${RHODES_CONFIG_FILE}; defaulting to CXL")
endif()
if(USE_RDMA)
    message(STATUS "Shared-memory backend: RDMA (from ${RHODES_CONFIG_FILE})")
else()
    message(STATUS "Shared-memory backend: CXL (from ${RHODES_CONFIG_FILE})")
endif()

# Only shared implementation files belong in the library. Executable entry
# points and generated files must never be collected by a recursive source glob.
add_library(misc_cpp STATIC
    src/common/CXLMemory.cpp
    src/common/Socket.cpp
    src/common/Time.cpp
    src/common/flags.cpp
)
target_include_directories(misc_cpp PUBLIC "${CMAKE_SOURCE_DIR}" "${CMAKE_SOURCE_DIR}/src")

include(FetchContent)
FetchContent_Declare(xxHash
    GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
    GIT_TAG v0.8.1
)
FetchContent_MakeAvailable(xxHash)
add_library(xxhash STATIC "${xxhash_SOURCE_DIR}/xxhash.c")
target_include_directories(xxhash PUBLIC "${xxhash_SOURCE_DIR}")
add_library(xxHash::xxhash ALIAS xxhash)
target_link_libraries(misc_cpp PUBLIC xxHash::xxhash)

FetchContent_Declare(absl
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20230125.3
)
FetchContent_MakeAvailable(absl)
target_link_libraries(misc_cpp PUBLIC
    absl::flat_hash_map
    absl::synchronization
    absl::strings
    absl::base
    absl::log
    absl::log_internal_check_op
    absl::check
    absl::flags_parse
)
if(APPLE)
    find_package(glog REQUIRED)
    find_package(gflags REQUIRED)
    target_link_libraries(misc_cpp PUBLIC ${jemalloc_lib} glog::glog gflags)
else()
    target_link_libraries(misc_cpp PUBLIC ${jemalloc_lib} glog gflags)
endif()
target_link_libraries(misc_cpp PUBLIC spdlog::spdlog)

add_library(cxlalloc_static STATIC IMPORTED)
set_target_properties(cxlalloc_static PROPERTIES
    IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/dependencies/cxlalloc/libcxlalloc_static.a"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/dependencies/cxlalloc"
)
target_link_libraries(misc_cpp PUBLIC cxlalloc_static ${ROARING_LIB})

if(USE_RDMA)
    target_sources(misc_cpp PRIVATE src/rdma/RdmaRegion.cpp)
    target_compile_definitions(misc_cpp PUBLIC USE_RDMA)
    find_library(IBVERBS_LIB ibverbs REQUIRED)
    # Connection metadata is exchanged over TCP; only libibverbs is used.
    target_link_libraries(misc_cpp PUBLIC ${IBVERBS_LIB})

    add_executable(rdma_mem_server src/rdma/RdmaMemoryServer.cpp src/rdma/RdmaRegion.cpp)
    add_executable(validate_append src/rdma/ValidateAppend.cpp src/rdma/RdmaRegion.cpp)
    add_executable(rdma_test EXCLUDE_FROM_ALL src/rdma/RdmaTest.cpp src/rdma/RdmaRegion.cpp)
    add_executable(dump_roots EXCLUDE_FROM_ALL src/rdma/DumpRoots.cpp src/rdma/RdmaRegion.cpp)
    foreach(target IN ITEMS rdma_mem_server validate_append rdma_test dump_roots)
        target_include_directories(${target} PRIVATE "${CMAKE_SOURCE_DIR}/src/rdma")
        target_link_libraries(${target} PRIVATE
            ${IBVERBS_LIB} glog gflags Threads::Threads)
    endforeach()
endif()

add_executable(sequencer src/sequencer/Sequencer.cpp)
add_executable(server src/workload/Server.cpp)
add_executable(client src/workload/Client.cpp)
foreach(target IN ITEMS sequencer server client)
    target_link_libraries(${target} PRIVATE misc_cpp cxlalloc_static Threads::Threads)
endforeach()

# A glob is appropriate for the optional formatting target, not compilation.
file(GLOB_RECURSE FORMAT_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_SOURCE_DIR}/src/*.cpp" "${CMAKE_SOURCE_DIR}/src/*.h")
list(FILTER FORMAT_SOURCES EXCLUDE REGEX "/build[^/]*/")
add_custom_target(format
    COMMAND clang-format -style=LLVM -i -sort-includes ${FORMAT_SOURCES}
    VERBATIM
)
