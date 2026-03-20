// XRPC transport test — client process
//
// Usage:
//   1. Start rpc_mgr:   ./diancie/rpc_mgr &
//   2. Start server:    ./apps/xrpc_server &
//   3. Start client:    ./apps/xrpc_client
//
// The client connects to the server, sends a payload and a tensor, then exits.

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <tensorpipe/tensorpipe.h>
#include <tensorpipe/transport/xrpc/factory.h>

int main() {
  tensorpipe::Context ctx;
  ctx.registerTransport(
      0, "xrpc", tensorpipe::transport::xrpc::create("127.0.0.1", 12345));
  ctx.registerChannel(0, "basic", tensorpipe::channel::basic::create());

  const std::string addr = "xrpc://xrpc_test_svc";

  std::cout << "[client] connecting to " << addr << "\n";
  auto pipe = ctx.connect(addr);

  // Payload: a plain byte string.
  std::string text = "hello from xrpc client";

  // Tensor: a small float array.
  std::vector<float> tensor = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};

  tensorpipe::Message msg;
  msg.payloads.resize(1);
  msg.payloads[0].data = text.data();
  msg.payloads[0].length = text.size();

  msg.tensors.resize(1);
  msg.tensors[0].buffer = tensorpipe::CpuBuffer{tensor.data()};
  msg.tensors[0].length = tensor.size() * sizeof(float);

  std::atomic<bool> done{false};

  pipe->write(std::move(msg), [&](const tensorpipe::Error& err) {
    if (err) {
      std::cerr << "[client] write error: " << err.what() << "\n";
    } else {
      std::cout << "[client] message sent successfully\n";
    }
    done = true;
  });

  while (!done.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  pipe->close();
  ctx.close();
  ctx.join();

  std::cout << "[client] done\n";
  return 0;
}
