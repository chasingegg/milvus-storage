#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/threading/Executor.h>
// #include <aws/core/utils/threading/PooledThreadExecutor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>

#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/GetObjectResult.h>

// Include fstream for file streaming
// #include <aws/fstream.h>

// Include headers for pre-allocated buffer streaming
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>

// Include CRT core for proper initialization
#include <aws/crt/Api.h>

// Headers for POSIX I/O and Direct I/O
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cerrno> // For errno

#include <chrono>
#include <iostream>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <fstream>
#include <memory>
#include <iomanip>
#include <future>
#include <sstream>
#include <queue>
#include <functional>

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
    int writerThreads; // Number of threads for writing files
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

class ThreadPool {
public:
    ThreadPool(size_t numThreads) : stop(false) {
        for(size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] {
                while(true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
                        if(this->stop && this->tasks.empty()) {
                            return;
                        }
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    template<class F>
    void enqueue(F&& f) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if(stop) {
                throw std::runtime_error("enqueue on stopped ThreadPool");
            }
            tasks.emplace(std::forward<F>(f));
        }
        condition.notify_one();
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for(std::thread &worker : workers) {
            worker.join();
        }
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

class PerformanceTester {
private:
    // Atomics for tracking live test state
    std::atomic<int> completedRequests{0};
    std::atomic<int> successCount{0};
    std::atomic<int> failureCount{0};
    std::atomic<size_t> totalBytes{0};
    std::atomic<int> completedWrites{0}; // Track completed write operations

    // Other state
    std::vector<std::chrono::milliseconds> latencies;
    std::mutex latencyMutex;
    std::condition_variable completionCV;
    std::mutex completionMutex;
    std::condition_variable writeCompletionCV; // Separate CV for write completion
    std::mutex writeCompletionMutex;
    std::unique_ptr<ThreadPool> writerPool;

public:
    PerformanceTester(int numWriterThreads) {
        if (numWriterThreads > 0) {
            writerPool = std::make_unique<ThreadPool>(numWriterThreads);
        }
    }

    // Test S3Client async performance
    PerformanceMetrics testS3ClientAsync(const TestConfig& config) {
        std::cout << "\n=== Testing S3Client Async: " << config.testName << " ===" << std::endl;
        
        // Configure S3Client
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-west-2";
        clientConfig.executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(config.concurrencyLevel);
        
        Aws::S3::S3Client s3Client(clientConfig);
        
        resetCounters();
        auto startTime = std::chrono::high_resolution_clock::now();
        // latencies.clear();
        latencies.resize(config.requestCount);
        
        // Launch async requests
        for (int i = 0; i < config.requestCount; ++i) {
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket(config.bucketName.c_str());
            request.SetKey(config.objectKeyPrefix + "_" + std::to_string(i % config.fileCount));
            
            // Create a unique filename for this download
            // const std::string outputFileName = "/data/storage/s3_download_" + std::to_string(i) + ".dat";
            // // Set the response stream factory to a file stream.
            // // This tells the SDK to stream the response data directly to the file
            // // instead of buffering it in memory.
            // request.SetResponseStreamFactory([=]() {
            //     return Aws::New<Aws::FStream>("GetObjectStream",
            //                                   outputFileName.c_str(),
            //                                   std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
            // });

            // For O_DIRECT, memory buffers must be aligned. We also need to ensure
            // the size of our transfer is a multiple of the block size.
            // NOTE: This assumes config.objectSize is a multiple of the alignment.
            const size_t alignment = 4096;
            void* data = nullptr;
            if (posix_memalign(&data, alignment, config.objectSize) != 0) {
                std::cerr << "✗ Failed to allocate aligned memory" << std::endl;
                continue; // Skip this request
            }
            // Use a shared_ptr with a custom deleter to manage the aligned buffer.
            auto buffer = std::shared_ptr<char>((char*)data, [](char* p){ free(p); });

            // Set the response stream factory to use our pre-allocated buffer.
            request.SetResponseStreamFactory([buffer, objectSize = config.objectSize]() {
                auto stream_buf = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
                    "GetObjectStream",
                    (unsigned char*)buffer.get(),
                    objectSize);
                return Aws::New<Aws::IOStream>("GetObjectStream", stream_buf);
            });
            
            auto requestStartTime = std::chrono::high_resolution_clock::now();
            
            s3Client.GetObjectAsync(request, 
                [this, requestStartTime, &config, i, buffer](const Aws::S3::S3Client* client,
                                                const Aws::S3::Model::GetObjectRequest& req,
                                                const Aws::S3::Model::GetObjectOutcome& outcome,
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                    
                    auto requestEndTime = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                        requestEndTime - requestStartTime);

                    latencies[i] = latency;
                    
                    if (outcome.IsSuccess()) {
                        successCount++;
                        // The body has been streamed to the pre-allocated buffer.
                        auto& result = outcome.GetResult();
                        long long contentLength = result.GetContentLength();
                        totalBytes += contentLength;
                        
                        // Now, write the buffer to a local file using O_DIRECT
                        const std::string outputFileName = "/data/gaochao/s3_download_" + std::to_string(i) + ".dat";
                        
                        auto writeFile = [this, buffer, contentLength, outputFileName, &config] {
                            int fd = open(outputFileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
                            if (fd == -1) {
                                std::cerr << "✗ Failed to open file with O_DIRECT: " << outputFileName << " (errno: " << errno << ")" << std::endl;
                            } else {
                                ssize_t bytes_written = write(fd, buffer.get(), contentLength);
                                if (bytes_written == -1) {
                                    std::cerr << "✗ Failed to write to file with O_DIRECT: " << outputFileName << " (errno: " << errno << ")" << std::endl;
                                } else if (bytes_written < contentLength) {
                                    std::cerr << "✗ Incomplete write to file: " << outputFileName << std::endl;
                                }
                                close(fd);
                            }
                            
                            // Notify that this write operation is complete
                            int writes_completed = ++completedWrites;
                            if (writes_completed >= config.requestCount) {
                                std::lock_guard<std::mutex> lock(writeCompletionMutex);
                                writeCompletionCV.notify_one();
                            }
                        };
                        
                        if (writerPool) {
                            writerPool->enqueue(writeFile);
                        } else {
                            writeFile(); // Fallback to synchronous write
                        }

                        // std::cout << "✓ Request successful, enqueued write to " << outputFileName
                        //             << " (" << contentLength << " bytes, " 
                        //             << latency.count() << "ms)" << std::endl;

                    } else {
                        failureCount++;
                        std::cout << "✗ Request failed: " << outcome.GetError().GetMessage() << std::endl;
                        
                        // Even failed requests need to be counted for write completion
                        int writes_completed = ++completedWrites;
                        if (writes_completed >= config.requestCount) {
                            std::lock_guard<std::mutex> lock(writeCompletionMutex);
                            writeCompletionCV.notify_one();
                        }
                    }
                    
                    int completed = ++completedRequests;
                    if (completed >= config.requestCount) {
                        std::lock_guard<std::mutex> lock(completionMutex);
                        completionCV.notify_one();
                    }
                });
            
        }
        
        // Wait for both download completion AND write completion
        std::unique_lock<std::mutex> downloadLock(completionMutex);
        completionCV.wait(downloadLock, [&] { return completedRequests >= config.requestCount; });
        std::cout << "✓ All downloads completed, waiting for writes to finish..." << std::endl;
        
        std::unique_lock<std::mutex> writeLock(writeCompletionMutex);
        writeCompletionCV.wait(writeLock, [&] { return completedWrites >= config.requestCount; });
        std::cout << "✓ All writes completed!" << std::endl;
        
        auto endTime = std::chrono::high_resolution_clock::now();
        
        return calculateMetrics(startTime, endTime, config);
    }
    
    // Test S3CrtClient async performance
    PerformanceMetrics testS3CrtClientAsync(const TestConfig& config) {
        std::cout << "\n=== Testing S3CrtClient Async: " << config.testName << " ===" << std::endl;
        
        try {
            Aws::S3Crt::ClientConfiguration clientConfig;
            clientConfig.region = "us-west-2";
            clientConfig.throughputTargetGbps = 20;
            
            Aws::S3Crt::S3CrtClient s3CrtClient(clientConfig);
            
            std::cout << "Starting S3CrtClient async operations..." << std::endl;
            
            resetCounters();
            auto startTime = std::chrono::high_resolution_clock::now();
            latencies.resize(config.requestCount);
            
            // Launch async requests
            for (int i = 0; i < config.requestCount; ++i) {
                Aws::S3Crt::Model::GetObjectRequest request;
                request.SetBucket(config.bucketName.c_str());
                request.SetKey(config.objectKeyPrefix + "_" + std::to_string(i % config.fileCount));
                
                // For O_DIRECT, memory buffers must be aligned.
                const size_t alignment = 4096;
                void* data = nullptr;
                if (posix_memalign(&data, alignment, config.objectSize) != 0) {
                    std::cerr << "✗ Failed to allocate aligned memory" << std::endl;
                    continue;
                }
                auto buffer = std::shared_ptr<char>((char*)data, [](char* p){ free(p); });

                // Set the response stream factory to use our pre-allocated buffer.
                request.SetResponseStreamFactory([buffer, objectSize = config.objectSize]() {
                    auto stream_buf = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
                        "GetObjectStream",
                        (unsigned char*)buffer.get(),
                        objectSize);
                    return Aws::New<Aws::IOStream>("GetObjectStream", stream_buf);
                });

                auto requestStartTime = std::chrono::high_resolution_clock::now();
                
                s3CrtClient.GetObjectAsync(request,
                    [this, requestStartTime, &config, i, buffer](const Aws::S3Crt::S3CrtClient* client,
                                                    const Aws::S3Crt::Model::GetObjectRequest& req,
                                                    const Aws::S3Crt::Model::GetObjectOutcome& outcome,
                                                    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                        
                        auto requestEndTime = std::chrono::high_resolution_clock::now();
                        auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                            requestEndTime - requestStartTime);
                        
                        latencies[i] = latency;
                        
                        if (outcome.IsSuccess()) {
                            successCount++;
                            // The body has been streamed to the pre-allocated buffer.
                            auto& result = outcome.GetResult();
                            long long contentLength = result.GetContentLength();
                            totalBytes += contentLength;
                            
                            // Now, write the buffer to a local file using O_DIRECT
                            const std::string outputFileName = "/data/gaochao/s3crt_download_" + std::to_string(i) + ".dat";
                            
                            auto writeFile = [this, buffer, contentLength, outputFileName, &config] {
                                int fd = open(outputFileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
                                if (fd == -1) {
                                    std::cerr << "✗ Failed to open file with O_DIRECT: " << outputFileName << " (errno: " << errno << ")" << std::endl;
                                } else {
                                    ssize_t bytes_written = write(fd, buffer.get(), contentLength);
                                    if (bytes_written == -1) {
                                        std::cerr << "✗ Failed to write to file with O_DIRECT: " << outputFileName << " (errno: " << errno << ")" << std::endl;
                                    } else if (bytes_written < contentLength) {
                                        std::cerr << "✗ Incomplete write to file: " << outputFileName << std::endl;
                                    }
                                    close(fd);
                                }
                                
                                // Notify that this write operation is complete
                                int writes_completed = ++completedWrites;
                                if (writes_completed >= config.requestCount) {
                                    std::lock_guard<std::mutex> lock(writeCompletionMutex);
                                    writeCompletionCV.notify_one();
                                }
                            };
                            
                            if (writerPool) {
                                writerPool->enqueue(writeFile);
                            } else {
                                writeFile(); // Fallback to synchronous write
                            }
                            
                            // std::cout << "✓ Request successful, enqueued write to " << outputFileName
                                        // << " (" << contentLength << " bytes, " 
                                        // << latency.count() << "ms)" << std::endl;

                        } else {
                            failureCount++;
                            std::cout << "✗ Request failed: " << outcome.GetError().GetMessage() << std::endl;
                            
                            // Even failed requests need to be counted for write completion
                            int writes_completed = ++completedWrites;
                            if (writes_completed >= config.requestCount) {
                                std::lock_guard<std::mutex> lock(writeCompletionMutex);
                                writeCompletionCV.notify_one();
                            }
                        }
                        
                        int completed = ++completedRequests;
                        if (completed >= config.requestCount) {
                            std::lock_guard<std::mutex> lock(completionMutex);
                            completionCV.notify_one();
                        }
                    });
                
            }
            
            // Wait for both download completion AND write completion
            std::unique_lock<std::mutex> downloadLock(completionMutex);
            completionCV.wait(downloadLock, [&] { return completedRequests >= config.requestCount; });
            std::cout << "✓ All downloads completed, waiting for writes to finish..." << std::endl;
            
            std::unique_lock<std::mutex> writeLock(writeCompletionMutex);
            writeCompletionCV.wait(writeLock, [&] { return completedWrites >= config.requestCount; });
            std::cout << "✓ All writes completed!" << std::endl;
            
            auto endTime = std::chrono::high_resolution_clock::now();
            
            return calculateMetrics(startTime, endTime, config);
            
        } catch (const std::exception& e) {
            std::cerr << "✗ Exception in S3CrtClient test: " << e.what() << std::endl;
            PerformanceMetrics emptyMetrics;
            emptyMetrics.failedRequests = config.requestCount;
            return emptyMetrics;
        } catch (...) {
            std::cerr << "✗ Unknown exception in S3CrtClient test" << std::endl;
            PerformanceMetrics emptyMetrics;
            emptyMetrics.failedRequests = config.requestCount;
            return emptyMetrics;
        }
    }
    
private:
    void resetCounters() {
        completedRequests = 0;
        successCount = 0;
        failureCount = 0;
        totalBytes = 0;
        completedWrites = 0; // Reset write completion counter
        latencies.clear();
    }
    
    PerformanceMetrics calculateMetrics(const std::chrono::high_resolution_clock::time_point& startTime,
                                      const std::chrono::high_resolution_clock::time_point& endTime,
                                      const TestConfig& config) {
        PerformanceMetrics metrics;
        
        metrics.totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        metrics.successfulRequests = successCount.load();
        metrics.failedRequests = failureCount.load();
        metrics.totalBytesTransferred = totalBytes.load();
        
        // Calculate latency statistics
        if (!latencies.empty()) {
            std::sort(latencies.begin(), latencies.end());
            metrics.minLatency = latencies.front();
            metrics.maxLatency = latencies.back();
            
            auto totalLatency = std::chrono::milliseconds(0);
            for (const auto& latency : latencies) {
                totalLatency += latency;
            }
            metrics.avgLatency = totalLatency / latencies.size();
        }
        
        // Calculate throughput
        if (metrics.totalDuration.count() > 0) {
            double durationSeconds = metrics.totalDuration.count() / 1000.0;
            double megabytes = metrics.totalBytesTransferred / (1024.0 * 1024.0);
            metrics.throughputMBps = megabytes / durationSeconds;
        }
        
        return metrics;
    }
};

void printMetrics(const std::string& clientType, const PerformanceMetrics& metrics) {
    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << clientType << " Performance Metrics:" << std::endl;
    std::cout << std::string(50, '=') << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Total Duration:        " << metrics.totalDuration.count() << " ms" << std::endl;
    std::cout << "Successful Requests:   " << metrics.successfulRequests << std::endl;
    std::cout << "Failed Requests:       " << metrics.failedRequests << std::endl;
    std::cout << "Total Bytes:           " << metrics.totalBytesTransferred << " bytes ("
              << (metrics.totalBytesTransferred / (1024.0 * 1024.0)) << " MB)" << std::endl;
    std::cout << "Throughput:            " << metrics.throughputMBps << " MB/s" << std::endl;
    std::cout << "Min Latency:           " << metrics.minLatency.count() << " ms" << std::endl;
    std::cout << "Max Latency:           " << metrics.maxLatency.count() << " ms" << std::endl;
    std::cout << "Average Latency:       " << metrics.avgLatency.count() << " ms" << std::endl;
    std::cout << std::string(50, '=') << std::endl;
}

void compareMetrics(const PerformanceMetrics& s3Metrics, const PerformanceMetrics& s3CrtMetrics) {
    std::cout << "\n" << std::string(60, '=') << std::endl;
    std::cout << "PERFORMANCE COMPARISON" << std::endl;
    std::cout << std::string(60, '=') << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    
    // Throughput comparison
    double throughputImprovement = ((s3CrtMetrics.throughputMBps - s3Metrics.throughputMBps) / s3Metrics.throughputMBps) * 100.0;
    std::cout << "Throughput Improvement (S3CRT vs S3): " << throughputImprovement << "%" << std::endl;
    
    // Latency comparison
    double latencyImprovement = ((s3Metrics.avgLatency.count() - s3CrtMetrics.avgLatency.count()) / (double)s3Metrics.avgLatency.count()) * 100.0;
    std::cout << "Average Latency Improvement (S3CRT vs S3): " << latencyImprovement << "%" << std::endl;
    
    // Duration comparison
    double durationImprovement = ((s3Metrics.totalDuration.count() - s3CrtMetrics.totalDuration.count()) / (double)s3Metrics.totalDuration.count()) * 100.0;
    std::cout << "Total Duration Improvement (S3CRT vs S3): " << durationImprovement << "%" << std::endl;
    
    std::cout << std::string(60, '=') << std::endl;
}

void saveResultsToFile(const TestConfig& config, 
                      const PerformanceMetrics& s3Metrics, 
                      const PerformanceMetrics& s3CrtMetrics) {
    std::string filename = "performance_results_" + 
                          std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now().time_since_epoch()).count()) + ".csv";
    
    static bool headerWritten = false;
    std::ofstream file(filename, std::ios::app);
    
    if (!headerWritten) {
        file << "TestName,ClientType,TotalDuration(ms),SuccessfulRequests,ThroughputMBps,AvgLatency(ms),MinLatency(ms),MaxLatency(ms),TotalBytes\n";
        headerWritten = true;
    }
    
    // S3Client results
    file << config.testName << ",S3Client," 
         << s3Metrics.totalDuration.count() << ","
         << s3Metrics.successfulRequests << ","
         << s3Metrics.throughputMBps << ","
         << s3Metrics.avgLatency.count() << ","
         << s3Metrics.minLatency.count() << ","
         << s3Metrics.maxLatency.count() << ","
         << s3Metrics.totalBytesTransferred << "\n";
    
    // S3CrtClient results
    file << config.testName << ",S3CrtClient," 
         << s3CrtMetrics.totalDuration.count() << ","
         << s3CrtMetrics.successfulRequests << ","
         << s3CrtMetrics.throughputMBps << ","
         << s3CrtMetrics.avgLatency.count() << ","
         << s3CrtMetrics.minLatency.count() << ","
         << s3CrtMetrics.maxLatency.count() << ","
         << s3CrtMetrics.totalBytesTransferred << "\n";
    
    file.close();
    std::cout << "Results saved to: " << filename << std::endl;
}

void runPerformanceTests() {
    // Test configurations for different scenarios
    std::vector<TestConfig> testConfigs = {
        //    std::string bucketName;
        // std::string objectKeyPrefix;
        // size_t objectSize;  // Expected object size for throughput calculation
        // int concurrencyLevel;
        // int requestCount;
        // int fileCount;
        // int writerThreads;
        // std::string testName;
        // Small objects, low concurrency
        // {"uat-test-bucket-temp-001", "test-small-object.dat", 1024 * 1024, 16, 1000, 10, 8, "Small Objects (1MB), 8 Writers"},
        
        // // Medium objects, medium concurrency
        {"uat-test-bucket-temp-001", "test-object-4.0mb.dat", 4 * 1024 * 1024, 16, 1000, 10, 8, "Medium Objects (4MB), Medium Concurrency"},
        
        // // Large objects, high concurrency
        // {"your-test-bucket", "test-large-object.dat", 100 * 1024 * 1024, 10, 5, 10, "Large Objects (100MB), High Concurrency"},
    };
    
    for (const auto& config : testConfigs) {
        PerformanceTester tester(config.writerThreads);
        std::cout << "\n" << std::string(80, '#') << std::endl;
        std::cout << "Running Test: " << config.testName << std::endl;
        std::cout << "Bucket: " << config.bucketName << ", Object: " << config.objectKeyPrefix << std::endl;
        std::cout << "Concurrency: " << config.concurrencyLevel << ", Requests: " << config.requestCount 
                  << ", Files: " << config.fileCount << ", Writers: " << config.writerThreads << std::endl;
        std::cout << std::string(80, '#') << std::endl;
        
        // Test S3Client
        auto s3Metrics = tester.testS3ClientAsync(config);
        printMetrics("S3Client", s3Metrics);
        
        // Small delay between tests
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        // Test S3CrtClient
        auto s3CrtMetrics = tester.testS3CrtClientAsync(config);
        printMetrics("S3CrtClient", s3CrtMetrics);
        
        // Compare results
        // compareMetrics(s3Metrics, s3CrtMetrics);
        
        // Save results to file
        // saveResultsToFile(config, s3Metrics, s3CrtMetrics);
    }
}

int main(int argc, char* argv[]) {
    // CRITICAL: Initialize AWS CRT API first - this is required for S3CrtClient
    std::cout << "Initializing AWS CRT Core API..." << std::endl;
    Aws::Crt::ApiHandle apiHandle;
    std::cout << "✓ AWS CRT Core API initialized" << std::endl;
    
    // Initialize AWS SDK with CRT support
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info; // Reduced logging for less noise
    options.loggingOptions.logger_create_fn = [] {
        return std::make_shared<Aws::Utils::Logging::DefaultLogSystem>(
            Aws::Utils::Logging::LogLevel::Info, "performance_test");
    };
    
    // Enable CRT support for S3CrtClient
    // options.httpOptions.installSigPipeHandler = true;
    
    // Initialize AWS SDK after CRT
    std::cout << "Initializing AWS SDK..." << std::endl;
    Aws::InitAPI(options);
    std::cout << "✓ AWS SDK initialized with CRT support" << std::endl;
    
    int result = 0;
    try {
        std::cout << "AWS SDK C++ S3 Async Performance Test" << std::endl;
        std::cout << "======================================" << std::endl;
        
        if (argc > 1) {
            std::cout << "Usage: " << argv[0] << std::endl;
            std::cout << "Make sure to configure your AWS credentials and update bucket/object names in the code." << std::endl;
        }
        
        runPerformanceTests();
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        result = 1;
    } catch (...) {
        std::cerr << "Unknown error occurred" << std::endl;
        result = 1;
    }
    
    // CRITICAL: Proper cleanup order - SDK first, then CRT
    std::cout << "Shutting down AWS SDK..." << std::endl;
    Aws::ShutdownAPI(options);
    std::cout << "✓ AWS SDK shutdown complete" << std::endl;
    
    // Let CRT ApiHandle cleanup automatically when it goes out of scope
    std::cout << "AWS CRT will cleanup automatically..." << std::endl;
    
    return result;
} 