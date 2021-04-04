include_directories(..)

add_executable(testSimpleTable testSimple.c)
add_executable(testMM testMM.c)

target_link_libraries(testSimpleTable MyKV ssl)
target_link_libraries(testMM MyKV ssl ibverbs)