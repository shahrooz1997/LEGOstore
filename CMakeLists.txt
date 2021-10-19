cmake_minimum_required(VERSION 3.10)

# Disable in-source builds.
if(" ${CMAKE_SOURCE_DIR}" STREQUAL " ${CMAKE_BINARY_DIR}")
    message(FATAL_ERROR "
    FATAL: In-source builds are not allowed.
    You should create a separate directory for build files.")
endif()

project(LEGOStore)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Werror -Wconversion")

# Get the sources.
file(GLOB SOURCES src/*.cpp)
set(LIB_SOURCES ${SOURCES})
set(MAIN_SOURCES ${CMAKE_SOURCE_DIR}/src/Client_driver.cpp ${CMAKE_SOURCE_DIR}/src/Server_driver.cpp
        ${CMAKE_SOURCE_DIR}/src/Controller.cpp ${CMAKE_SOURCE_DIR}/src/Metadata_Server.cpp)
list(REMOVE_ITEM LIB_SOURCES ${MAIN_SOURCES})

# Generate protocol buffer code.
add_custom_command(OUTPUT ${CMAKE_SOURCE_DIR}/inc/gbuffer.pb.h ${CMAKE_SOURCE_DIR}/src/gbuffer.pb.cpp
        COMMAND protoc ARGS --proto_path=${CMAKE_SOURCE_DIR}/inc --cpp_out=${CMAKE_SOURCE_DIR}/inc ${CMAKE_SOURCE_DIR}/inc/gbuffer.proto
        COMMAND mv ARGS ${CMAKE_SOURCE_DIR}/inc/gbuffer.pb.cc ${CMAKE_SOURCE_DIR}/src/gbuffer.pb.cpp
        DEPENDS ${CMAKE_SOURCE_DIR}/inc/gbuffer.proto)

# Build LEGOStore library.
link_libraries(m erasurecode rocksdb dl protobuf pthread)
include_directories(./inc)
add_library(LEGOStore_lib ${LIB_SOURCES} ${CMAKE_SOURCE_DIR}/src/gbuffer.pb.cpp)

# Create the components using the LEGOStore library.
link_libraries(LEGOStore_lib)
add_executable(Client ${CMAKE_SOURCE_DIR}/src/Client_driver.cpp)
add_executable(Server ${CMAKE_SOURCE_DIR}/src/Server_driver.cpp)
add_executable(Controller ${CMAKE_SOURCE_DIR}/src/Controller.cpp)
add_executable(Metadata_Server ${CMAKE_SOURCE_DIR}/src/Metadata_Server.cpp)

# So you can run the project inside the CMAKE_BINARY_DIR directory.
add_custom_target(create_logs_directory ALL
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/logs
        BYPRODUCTS ${CMAKE_BINARY_DIR}/logs)
add_custom_target(copy_dependencies ALL
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/config ${CMAKE_BINARY_DIR}/config
        BYPRODUCTS ${CMAKE_BINARY_DIR}/config)
add_custom_target(cleandb ALL
        COMMAND rm -rf db*.temp server*_output.txt client*_output.txt cont_output_*.txt logfile_*.txt logs/* cont_logs/*
        knossos)
