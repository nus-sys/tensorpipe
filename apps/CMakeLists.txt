add_executable(hello hello.cc)
target_link_libraries(hello PRIVATE tensorpipe)

add_executable(xrpc_basic_channel xrpc_basic_channel.cc)
target_link_libraries(xrpc_basic_channel PRIVATE tensorpipe)

add_executable(xrpc_server xrpc_server.cc)
target_link_libraries(xrpc_server PRIVATE tensorpipe)

add_executable(xrpc_client xrpc_client.cc)
target_link_libraries(xrpc_client PRIVATE tensorpipe)
