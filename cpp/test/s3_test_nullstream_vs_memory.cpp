#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>

#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/GetObjectResult.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/memory/AWSMemory.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <thread>
#include <vector>

using namespace Aws;
using namespace Aws::S3;
using namespace Aws::S3Crt;

struct TestConfig {
    std::string bucketName;
    std::string objectKeyPrefix;
    size_t objectSize;  // Expected object size for throughput calculation
    int concurrencyLevel;
    int requestCount;
    int fileCount;
    std::string testName;
};

struct PerformanceMetrics {
    std::chrono::milliseconds totalDuration{0};
    std::chrono::milliseconds minLatency{std::chrono::milliseconds::max()};
    std::chrono::milliseconds maxLatency{0};
    std::chrono::milliseconds avgLatency{0};
    double throughputMBps = 0.0;
    size_t totalBytesTransferred = 0;
    int successfulRequests = 0;
    int failedRequests = 0;
    size_t peakMemoryUsage = 0;  // Approximate
};

// Custom stream buffer that discards all data (like /dev/null)
class NullStreamBuf : public std::streambuf {
  public:
  NullStreamBuf() = default;
  
  protected:
  // Override overflow to discard all data (output)
  int_type overflow(int_type c) override {
    // Just return the character to indicate success
    return c;
  }
  
  // Override xsputn for bulk writes - discard all data (output)
  std::streamsize xsputn(const char* s, std::streamsize n) override {
    // Pretend we wrote all the data by returning n
    return n;
  }
  
  // Override underflow for input operations - always EOF
  int_type underflow() override {
    return traits_type::eof();
  }
  
  // Override xsgetn for bulk reads - always return 0 (no data available)
  std::streamsize xsgetn(char* s, std::streamsize n) override {
    return 0;
  }
  
  // Override seekoff for seeking operations
  pos_type seekoff(off_type off, std::ios_base::seekdir way, std::ios_base::openmode which) override {
    // Always return success for seeking
    return pos_type(off);
  }
  
  // Override seekpos for absolute positioning
  pos_type seekpos(pos_type pos, std::ios_base::openmode which) override {
    // Always return the requested position
    return pos;
  }
};

// Custom stream that discards all data (bidirectional iostream)
class NullStream : public std::iostream {
  private:
  NullStreamBuf null_buf_;
  
  public:
  NullStream() : std::iostream(&null_buf_) {}
  
  // Make it movable
  NullStream(NullStream&& other) noexcept : std::iostream(std::move(other)), null_buf_(std::move(other.null_buf_)) {
    set_rdbuf(&null_buf_);
  }
  
  NullStream& operator=(NullStream&& other) noexcept {
    if (this != &other) {
      std::iostream::operator=(std::move(other));
      null_buf_ = std::move(other.null_buf_);
      set_rdbuf(&null_buf_);
    }
    return *this;
  }
  
  // Delete copy operations
  NullStream(const NullStream&) = delete;
  NullStream& operator=(const NullStream&) = delete;
};


class PerformanceTester {
private:
    std::atomic<int> completedRequests{0};
    std::atomic<int> successCount{0};
    std::atomic<int> failureCount{0};
    std::atomic<size_t> totalBytes{0};
    std::vector<std::chrono::milliseconds> latencies;
    std::mutex latencyMutex;
    std::condition_variable completionCV;
    std::mutex completionMutex;

    void resetCounters() {
        completedRequests = 0;
        successCount = 0;
        failureCount = 0;
        totalBytes = 0;
        latencies.clear();
    }

    PerformanceMetrics calculateMetrics(std::chrono::high_resolution_clock::time_point startTime, 
                                       std::chrono::high_resolution_clock::time_point endTime,
                                       const TestConfig& config) {
        PerformanceMetrics metrics;
        
        metrics.totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        metrics.successfulRequests = successCount;
        metrics.failedRequests = failureCount;
        metrics.totalBytesTransferred = totalBytes;
        
        if (!latencies.empty()) {
            std::lock_guard<std::mutex> lock(latencyMutex);
            metrics.minLatency = *std::min_element(latencies.begin(), latencies.end());
            metrics.maxLatency = *std::max_element(latencies.begin(), latencies.end());
            
            auto totalLatency = std::accumulate(latencies.begin(), latencies.end(), 
                                              std::chrono::milliseconds(0));
            metrics.avgLatency = totalLatency / latencies.size();
        }
        
        if (metrics.totalDuration.count() > 0) {
            double seconds = metrics.totalDuration.count() / 1000.0;
            double megabytes = metrics.totalBytesTransferred / (1024.0 * 1024.0);
            metrics.throughputMBps = megabytes / seconds;
        }
        
        return metrics;
    }

public:
    // Test with null stream (discard all data)
    PerformanceMetrics testNullStream(const TestConfig& config) {
        std::cout << "\n=== Testing Null Stream (Discard Data): " << config.testName << " ===" << std::endl;
        
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-west-2";
        clientConfig.executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(config.concurrencyLevel);
        
        Aws::S3::S3Client s3Client(clientConfig);
        
        resetCounters();
        auto startTime = std::chrono::high_resolution_clock::now();
        latencies.resize(config.requestCount);
        
        // Launch async requests
        for (int i = 0; i < config.requestCount; ++i) {
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket(config.bucketName.c_str());
            request.SetKey(config.objectKeyPrefix + "_" + std::to_string(i % config.fileCount));
            
            // Use null stream factory to discard all response data
            request.SetResponseStreamFactory([]() {
                return Aws::New<NullStream>("NullStream");
            });
            
            auto requestStartTime = std::chrono::high_resolution_clock::now();
            s3Client.GetObjectAsync(request,
                [this, requestStartTime, config](const Aws::S3::S3Client* /*client*/,
                                                const Aws::S3::Model::GetObjectRequest& /*request*/,
                                                const Aws::S3::Model::GetObjectOutcome& outcome,
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& /*context*/) {
                    
                    auto requestEndTime = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(requestEndTime - requestStartTime);
                    
                    {
                        std::lock_guard<std::mutex> lock(latencyMutex);
                        latencies[completedRequests] = latency;
                    }
                    
                    if (outcome.IsSuccess()) {
                        successCount++;
                        totalBytes += config.objectSize;  // Expected size since we're discarding
                    } else {
                        failureCount++;
                        std::cout << "✗ Request failed: " << outcome.GetError().GetMessage() << std::endl;
                    }
                    
                    int completed = ++completedRequests;
                    if (completed >= config.requestCount) {
                        std::lock_guard<std::mutex> lock(completionMutex);
                        completionCV.notify_one();
                    }
                });
        }
        
        // Wait for completion
        std::unique_lock<std::mutex> lock(completionMutex);
        completionCV.wait(lock, [&] { return completedRequests >= config.requestCount; });
        
        auto endTime = std::chrono::high_resolution_clock::now();
        
        return calculateMetrics(startTime, endTime, config);
    }
    
    // Test with memory buffer (store all data in memory)
    PerformanceMetrics testMemoryBuffer(const TestConfig& config) {
        std::cout << "\n=== Testing Memory Buffer (Store Data): " << config.testName << " ===" << std::endl;
        
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-west-2";
        clientConfig.executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(config.concurrencyLevel);
        
        Aws::S3::S3Client s3Client(clientConfig);
        
        resetCounters();
        
        // Pre-allocate memory buffers for all requests
        std::vector<std::unique_ptr<char[]>> buffers(config.requestCount);
        const size_t alignment = 4096;
        
        for (int i = 0; i < config.requestCount; ++i) {
            void* data = nullptr;
            if (posix_memalign(&data, alignment, config.objectSize) != 0) {
                std::cerr << "Failed to allocate aligned memory for request " << i << std::endl;
                continue;
            }
            buffers[i] = std::unique_ptr<char[]>(static_cast<char*>(data));
        }

        auto startTime = std::chrono::high_resolution_clock::now();
        latencies.resize(config.requestCount);
        
        // Launch async requests
        for (int i = 0; i < config.requestCount; ++i) {
            if (!buffers[i]) continue;
            
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket(config.bucketName.c_str());
            request.SetKey(config.objectKeyPrefix + "_" + std::to_string(i % config.fileCount));
            
            // Use memory buffer stream factory
            request.SetResponseStreamFactory([buffer = buffers[i].get(), objectSize = config.objectSize]() {
                auto stream_buf = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
                    "GetObjectStream",
                    (unsigned char*)buffer,
                    objectSize);
                return Aws::New<Aws::IOStream>("GetObjectStream", stream_buf);
            });
            
            auto requestStartTime = std::chrono::high_resolution_clock::now();
            s3Client.GetObjectAsync(request,
                [this, requestStartTime, config](const Aws::S3::S3Client* /*client*/,
                                                const Aws::S3::Model::GetObjectRequest& /*request*/,
                                                const Aws::S3::Model::GetObjectOutcome& outcome,
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& /*context*/) {
                    
                    auto requestEndTime = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(requestEndTime - requestStartTime);
                    
                    {
                        std::lock_guard<std::mutex> lock(latencyMutex);
                        latencies[completedRequests] = latency;
                    }
                    
                    if (outcome.IsSuccess()) {
                        successCount++;
                        auto& result = outcome.GetResult();
                        long long contentLength = result.GetContentLength();
                        totalBytes += contentLength;
                    } else {
                        failureCount++;
                        std::cout << "✗ Request failed: " << outcome.GetError().GetMessage() << std::endl;
                    }
                    
                    int completed = ++completedRequests;
                    if (completed >= config.requestCount) {
                        std::lock_guard<std::mutex> lock(completionMutex);
                        completionCV.notify_one();
                    }
                });
        }
        
        // Wait for completion
        std::unique_lock<std::mutex> lock(completionMutex);
        completionCV.wait(lock, [&] { return completedRequests >= config.requestCount; });
        
        auto endTime = std::chrono::high_resolution_clock::now();
        
        return calculateMetrics(startTime, endTime, config);
    }
};

void printMetrics(const std::string& testName, const PerformanceMetrics& metrics) {
    std::cout << "\n📊 " << testName << " Results:" << std::endl;
    std::cout << "─────────────────────────────────────────────────────────────────────────────────" << std::endl;
    std::cout << "✓ Total Duration: " << metrics.totalDuration.count() << " ms" << std::endl;
    std::cout << "✓ Successful Requests: " << metrics.successfulRequests << std::endl;
    std::cout << "✓ Failed Requests: " << metrics.failedRequests << std::endl;
    std::cout << "✓ Total Bytes Transferred: " << std::fixed << std::setprecision(2) 
              << (metrics.totalBytesTransferred / (1024.0 * 1024.0)) << " MB" << std::endl;
    std::cout << "✓ Throughput: " << std::fixed << std::setprecision(2) 
              << metrics.throughputMBps << " MB/s" << std::endl;
    std::cout << "✓ Average Latency: " << metrics.avgLatency.count() << " ms" << std::endl;
    std::cout << "✓ Min Latency: " << metrics.minLatency.count() << " ms" << std::endl;
    std::cout << "✓ Max Latency: " << metrics.maxLatency.count() << " ms" << std::endl;
    std::cout << std::endl;
}

void compareResults(const PerformanceMetrics& nullStreamMetrics, const PerformanceMetrics& memoryBufferMetrics) {
    std::cout << "\n🔍 Performance Comparison:" << std::endl;
    std::cout << "═════════════════════════════════════════════════════════════════════════════════" << std::endl;
    
    double throughputImprovement = ((nullStreamMetrics.throughputMBps - memoryBufferMetrics.throughputMBps) / memoryBufferMetrics.throughputMBps) * 100.0;
    double latencyImprovement = ((memoryBufferMetrics.avgLatency.count() - nullStreamMetrics.avgLatency.count()) / static_cast<double>(memoryBufferMetrics.avgLatency.count())) * 100.0;
    
    std::cout << "📈 Throughput Comparison:" << std::endl;
    std::cout << "   Null Stream: " << std::fixed << std::setprecision(2) << nullStreamMetrics.throughputMBps << " MB/s" << std::endl;
    std::cout << "   Memory Buffer: " << std::fixed << std::setprecision(2) << memoryBufferMetrics.throughputMBps << " MB/s" << std::endl;
    std::cout << "   Improvement: " << std::fixed << std::setprecision(1) << throughputImprovement << "%" << std::endl;
    
    std::cout << "\n⏱️ Latency Comparison:" << std::endl;
    std::cout << "   Null Stream: " << nullStreamMetrics.avgLatency.count() << " ms" << std::endl;
    std::cout << "   Memory Buffer: " << memoryBufferMetrics.avgLatency.count() << " ms" << std::endl;
    std::cout << "   Improvement: " << std::fixed << std::setprecision(1) << latencyImprovement << "%" << std::endl;
    
    std::cout << "\n💾 Memory Usage:" << std::endl;
    std::cout << "   Null Stream: ~0 MB (data discarded)" << std::endl;
    std::cout << "   Memory Buffer: ~" << std::fixed << std::setprecision(1) 
              << (memoryBufferMetrics.totalBytesTransferred / (1024.0 * 1024.0)) << " MB (data stored)" << std::endl;
    
    std::cout << "\n🎯 Summary:" << std::endl;
    if (throughputImprovement > 5.0) {
        std::cout << "   ✅ Null stream shows significant performance improvement (" << std::fixed << std::setprecision(1) << throughputImprovement << "%)" << std::endl;
        std::cout << "   💡 Consider using null stream for tests where data content is not needed" << std::endl;
    } else if (throughputImprovement > 0) {
        std::cout << "   ✅ Null stream shows minor performance improvement (" << std::fixed << std::setprecision(1) << throughputImprovement << "%)" << std::endl;
    } else {
        std::cout << "   ℹ️ Memory buffer performs similar to null stream" << std::endl;
        std::cout << "   💡 Network and I/O overhead may be the limiting factor" << std::endl;
    }
    
    std::cout << std::endl;
}

void runPerformanceTests() {
    std::vector<TestConfig> testConfigs = {
        {"uat-test-bucket-temp-001", "test-object-4.0mb.dat", 4 * 1024 * 1024, 16, 4000, 1000, "Null Stream vs Memory Buffer (4MB Objects)"},
        // {"uat-test-bucket-temp-001", "test-object-4.0mb.dat", 4 * 1024 * 1024, 32, 200, 10, "Null Stream vs Memory Buffer (4MB Objects, Higher Concurrency)"},
    };
    
    for (const auto& config : testConfigs) {
        std::cout << "\n" << std::string(90, '#') << std::endl;
        std::cout << "🚀 Running Test: " << config.testName << std::endl;
        std::cout << "🪣 Bucket: " << config.bucketName << ", Object: " << config.objectKeyPrefix << std::endl;
        std::cout << "⚙️ Concurrency: " << config.concurrencyLevel << ", Requests: " << config.requestCount 
                  << ", Files: " << config.fileCount << std::endl;
        std::cout << std::string(90, '#') << std::endl;
        
        PerformanceTester tester;

               // Test memory buffer
        auto memoryBufferMetrics = tester.testMemoryBuffer(config);
        printMetrics("Memory Buffer", memoryBufferMetrics); 
        // Small delay between tests
        // std::this_thread::sleep_for(std::chrono::seconds(2));
        
        //                 // Test null stream
        auto nullStreamMetrics = tester.testNullStream(config);
        printMetrics("Null Stream", nullStreamMetrics);

        
        // Compare results
        // compareResults(nullStreamMetrics, memoryBufferMetrics);
    }
}

int main(int argc, char* argv[]) {
    // Initialize AWS SDK
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    options.loggingOptions.logger_create_fn = [] {
        return std::make_shared<Aws::Utils::Logging::DefaultLogSystem>(
            Aws::Utils::Logging::LogLevel::Info, "nullstream_vs_memory_test");
    };
    
    std::cout << "🔧 Initializing AWS SDK..." << std::endl;
    Aws::InitAPI(options);
    std::cout << "✓ AWS SDK initialized" << std::endl;
    
    int result = 0;
    try {
        std::cout << "\n🎯 AWS S3 Performance Test: Null Stream vs Memory Buffer" << std::endl;
        std::cout << "═══════════════════════════════════════════════════════════" << std::endl;
        std::cout << "This test compares the performance of discarding data (null stream)" << std::endl;
        std::cout << "versus storing data in memory buffers." << std::endl;
        
        if (argc > 1) {
            std::cout << "\n📋 Usage: " << argv[0] << std::endl;
            std::cout << "📋 Make sure to configure your AWS credentials and update bucket/object names." << std::endl;
        }
        
        runPerformanceTests();
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Error: " << e.what() << std::endl;
        result = 1;
    } catch (...) {
        std::cerr << "❌ Unknown error occurred" << std::endl;
        result = 1;
    }
    
    // Cleanup
    std::cout << "🔧 Shutting down AWS SDK..." << std::endl;
    Aws::ShutdownAPI(options);
    std::cout << "✓ AWS SDK shutdown complete" << std::endl;
    
    return result;
} 