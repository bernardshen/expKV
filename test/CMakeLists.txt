include_directories(..)

add_executable(testSimpleTable testSimple.c)
add_executable(testMM testMM.c)
add_executable(testCMServer testCMServer.c)
add_executable(testCMClient testCMClient.c)

target_link_libraries(testSimpleTable MyKV ssl ibverbs)
target_link_libraries(testMM MyKV ssl ibverbs)
target_link_libraries(testCMServer MyKV ssl ibverbs)
target_link_libraries(testCMClient MyKV ssl ibverbs)