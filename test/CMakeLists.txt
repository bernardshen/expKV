include_directories(..)

add_executable(test1 testsimple.c)

target_link_libraries(test1 MyKV ssl)