cmake_minimum_required(VERSION 3.3)
project(GetFileClient)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -Wextra -pthread")

set(SOURCE_FILES gfclient.c gfclient.h gfclient_download.c workload.c workload.h steque.c steque.h)
add_executable(GetFileClient ${SOURCE_FILES})