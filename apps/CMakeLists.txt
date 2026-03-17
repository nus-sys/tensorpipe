add_executable(hello hello.cc)
target_link_libraries(hello PRIVATE tensorpipe)

add_executable(shm_basic_channel shm_basic_channel.cc)
target_link_libraries(shm_basic_channel PRIVATE tensorpipe)
