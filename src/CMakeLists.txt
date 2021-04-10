aux_source_directory(. DIR_SRCS)

add_library(MyKV ${DIR_SRCS})
add_executable(myKVShell myKVShell.c)
add_executable(myKVServer myKVServer.c)

target_link_libraries(MyKV ssl crypto)
target_link_libraries(myKVShell MyKV ssl crypto ibverbs)
target_link_libraries(myKVServer MyKV ssl crypto ibverbs pthread)