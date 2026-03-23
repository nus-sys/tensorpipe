// XRPC transport benchmark — client process
//
// Usage:
//   1. Start rpc_mgr:   ./diancie/rpc_mgr &
//   2. Start server:    ./apps/xrpc_bench_server [num_iterations]
//   3. Start client:    ./apps/xrpc_bench_client [payload_size] [num_iterations]
//
// The client connects to the server, then performs a ping-pong benchmark:
// for each iteration it sends a message, waits for the echo, and records the
// round-trip latency.  At the end it prints median and average latency.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include <tensorpipe/tensorpipe.h>
#include <tensorpipe/transport/xrpc/factory.h>

static const size_t kDefaultPayloadSize = 4096;
static const int kDefaultIterations = 1000;
static const int kWarmUpRounds = 10;

using Clock = std::chrono::high_resolution_clock;
using Duration = std::chrono::duration<double, std::micro>;

struct BenchState {
  std::shared_ptr<tensorpipe::Pipe> pipe;
  std::vector<char> sendBuf;
  std::vector<char> recvBuf;
  std::vector<double> latencies; // microseconds
  int warmUpsLeft;
  int itersLeft;
  std::atomic<bool>& done;
  Clock::time_point sendTime;
};

static void pingPongLoop(BenchState& state);

static void sendPing(BenchState& state) {
  tensorpipe::Message msg;
  msg.payloads.resize(1);
  msg.payloads[0].data = state.sendBuf.data();
  msg.payloads[0].length = state.sendBuf.size();

  state.sendTime = Clock::now();

  state.pipe->write(std::move(msg), [&state](const tensorpipe::Error& err) {
    if (err) {
      std::cerr << "[client] write error: " << err.what() << "\n";
      state.done = true;
      return;
    }
    // Now read the echo back.
    state.pipe->readDescriptor(
        [&state](const tensorpipe::Error& err, tensorpipe::Descriptor desc) {
          if (err) {
            std::cerr << "[client] readDescriptor error: " << err.what()
                      << "\n";
            state.done = true;
            return;
          }

          tensorpipe::Allocation alloc;
          alloc.payloads.resize(desc.payloads.size());
          state.recvBuf.resize(desc.payloads[0].length);
          alloc.payloads[0].data = state.recvBuf.data();

          state.pipe->read(
              std::move(alloc), [&state](const tensorpipe::Error& err) {
                if (err) {
                  std::cerr << "[client] read error: " << err.what() << "\n";
                  state.done = true;
                  return;
                }

                auto elapsed =
                    Duration(Clock::now() - state.sendTime).count();

                if (state.warmUpsLeft > 0) {
                  --state.warmUpsLeft;
                } else {
                  state.latencies.push_back(elapsed);
                  --state.itersLeft;
                }

                pingPongLoop(state);
              });
        });
  });
}

static void pingPongLoop(BenchState& state) {
  if (state.itersLeft <= 0) {
    state.done = true;
    return;
  }
  sendPing(state);
}

static void printResults(std::vector<double>& latencies, size_t payloadSize) {
  if (latencies.empty()) {
    std::cout << "[client] no measurements recorded\n";
    return;
  }

  std::sort(latencies.begin(), latencies.end());

  double sum =
      std::accumulate(latencies.begin(), latencies.end(), 0.0);
  double avg = sum / latencies.size();
  double median = latencies[latencies.size() / 2];
  double p75 = latencies[(size_t)(latencies.size() * 0.75)];
  double p90 = latencies[(size_t)(latencies.size() * 0.90)];
  double p95 = latencies[(size_t)(latencies.size() * 0.95)];
  double minVal = latencies.front();
  double maxVal = latencies.back();

  std::cout << "\n===== XRPC Benchmark Results =====\n";
  std::cout << "Payload size:   " << payloadSize << " bytes\n";
  std::cout << "Iterations:     " << latencies.size() << "\n";
  std::cout << "Warm-up rounds: " << kWarmUpRounds << "\n\n";

  fprintf(
      stdout,
      "%-12s %-12s %-12s %-12s %-12s %-12s %-12s\n",
      "avg (us)",
      "median (us)",
      "p75 (us)",
      "p90 (us)",
      "p95 (us)",
      "min (us)",
      "max (us)");
  fprintf(
      stdout,
      "%-12.2f %-12.2f %-12.2f %-12.2f %-12.2f %-12.2f %-12.2f\n",
      avg,
      median,
      p75,
      p90,
      p95,
      minVal,
      maxVal);
  std::cout << "==================================\n";
}

int main(int argc, char* argv[]) {
  size_t payloadSize = kDefaultPayloadSize;
  int numIterations = kDefaultIterations;

  if (argc > 1) {
    payloadSize = std::atol(argv[1]);
  }
  if (argc > 2) {
    numIterations = std::atoi(argv[2]);
  }

  tensorpipe::Context ctx;
  ctx.registerTransport(
      0, "xrpc", tensorpipe::transport::xrpc::create("127.0.0.1", 12345));
  ctx.registerChannel(0, "basic", tensorpipe::channel::basic::create());

  const std::string addr = "xrpc://xrpc_bench_svc";

  std::cout << "[client] connecting to " << addr << "\n";
  std::cout << "[client] payload_size=" << payloadSize
            << " iterations=" << numIterations
            << " warmup=" << kWarmUpRounds << "\n";

  auto pipe = ctx.connect(addr);

  // Fill send buffer with a pattern.
  std::vector<char> sendBuf(payloadSize);
  for (size_t i = 0; i < payloadSize; ++i) {
    sendBuf[i] = static_cast<char>((i >> 8) ^ (i & 0xff));
  }

  std::atomic<bool> done{false};

  int totalRounds = numIterations + kWarmUpRounds;

  BenchState state{
      .pipe = std::move(pipe),
      .sendBuf = std::move(sendBuf),
      .recvBuf = {},
      .latencies = {},
      .warmUpsLeft = kWarmUpRounds,
      .itersLeft = numIterations,
      .done = done,
      .sendTime = {},
  };
  state.latencies.reserve(numIterations);

  // The server must be started with at least totalRounds iterations.
  (void)totalRounds;

  pingPongLoop(state);

  while (!done.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  printResults(state.latencies, state.sendBuf.size());

  state.pipe->close();
  ctx.close();
  ctx.join();

  return 0;
}
