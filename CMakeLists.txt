cmake_minimum_required(VERSION 2.7)

project(MyKV)

set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g2 -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

include_directories(.)

aux_source_directory(. DIR_SRCS)

add_subdirectory(test)

add_library(MyKV ${DIR_SRCS})
add_executable(myKVShell myKVShell.c)
add_executable(myKVServer myKVServer.c)

target_link_libraries(MyKV ssl crypto)
target_link_libraries(myKVShell MyKV ssl crypto ibverbs)
target_link_libraries(myKVServer MyKV ssl crypto ibverbs pthread)