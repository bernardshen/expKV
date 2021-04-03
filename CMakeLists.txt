cmake_minimum_required(VERSION 2.7)

project(MyKV)

aux_source_directory(. DIR_SRCS)

add_subdirectory(test)

add_library(MyKV)