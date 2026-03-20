// XRPC transport test — server process
//
// Usage:
//   1. Start rpc_mgr:   ./diancie/rpc_mgr &
//   2. Start server:    ./apps/xrpc_server
//   3. Start client:    ./apps/xrpc_client
//
// The server listens on the xrpc transport for a single message, prints it,
// then exits.

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

  std::atomic<bool> done{false};
  auto listener = ctx.listen({addr});
  std::cout << "[server] listening on " << addr << "\n";

  listener->accept([&](const tensorpipe::Error& err,
                       std::shared_ptr<tensorpipe::Pipe> pipe) {
    if (err) {
      std::cerr << "[server] accept error: " << err.what() << "\n";
      done = true;
      return;
    }
    std::cout << "[server] accepted connection\n";

    pipe->readDescriptor([pipe, &done](const tensorpipe::Error& err,
                                       tensorpipe::Descriptor desc) {
      if (err) {
        std::cerr << "[server] readDescriptor error: " << err.what() << "\n";
        done = true;
        return;
      }

      std::cout << "[server] descriptor: " << desc.payloads.size()
                << " payload(s), " << desc.tensors.size() << " tensor(s)\n";

      // Allocate receive buffers.
      auto payloadBuf =
          std::make_shared<std::vector<char>>(desc.payloads[0].length);
      auto tensorBuf = std::make_shared<std::vector<float>>(
          desc.tensors[0].length / sizeof(float));

      tensorpipe::Allocation alloc;
      alloc.payloads.resize(1);
      alloc.payloads[0].data = payloadBuf->data();
      alloc.tensors.resize(1);
      alloc.tensors[0].buffer = tensorpipe::CpuBuffer{tensorBuf->data()};

      pipe->read(
          std::move(alloc),
          [pipe, payloadBuf, tensorBuf, &done](const tensorpipe::Error& err) {
            if (err) {
              std::cerr << "[server] read error: " << err.what() << "\n";
              done = true;
              return;
            }

            std::string text(payloadBuf->begin(), payloadBuf->end());
            std::cout << "[server] payload: \"" << text << "\"\n";

            std::cout << "[server] tensor:  [";
            for (std::size_t i = 0; i < tensorBuf->size(); ++i) {
              if (i)
                std::cout << ", ";
              std::cout << (*tensorBuf)[i];
            }
            std::cout << "]\n";

            pipe->close();
            done = true;
          });
    });
  });

  while (!done.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  listener->close();
  ctx.close();
  ctx.join();

  std::cout << "[server] done\n";
  return 0;
}
