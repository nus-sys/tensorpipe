add_executable(hello hello.cc)
target_link_libraries(hello PRIVATE tensorpipe)

add_executable(xrpc_server xrpc_server.cc)
target_link_libraries(xrpc_server PRIVATE tensorpipe)

add_executable(xrpc_client xrpc_client.cc)
target_link_libraries(xrpc_client PRIVATE tensorpipe)

add_executable(xrpc_bench_server xrpc_bench_server.cc)
target_link_libraries(xrpc_bench_server PRIVATE tensorpipe)

add_executable(xrpc_bench_client xrpc_bench_client.cc)
target_link_libraries(xrpc_bench_client PRIVATE tensorpipe)
