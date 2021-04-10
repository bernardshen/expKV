# aux_source_directory(. DIR_SRCS)

add_library(MyKV SHARED cm.c city.c base_table.c mm.c RPCClient.c simple_table.c RPCServer.c)
add_executable(myKVShell myKVShell.c)
add_executable(myKVServer myKVServer.c)

target_link_libraries(MyKV ssl crypto ibverbs)
target_link_libraries(myKVShell MyKV ssl crypto ibverbs)
target_link_libraries(myKVServer MyKV ssl crypto ibverbs pthread)