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
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>

#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/GetObjectResult.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectResult.h>

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

public:
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
            
            auto requestStartTime = std::chrono::high_resolution_clock::now();
            
            s3Client.GetObjectAsync(request, 
                [this, requestStartTime, config, i](const Aws::S3::S3Client* client,
                                                const Aws::S3::Model::GetObjectRequest& req,
                                                const Aws::S3::Model::GetObjectOutcome& outcome,
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                    
                    auto requestEndTime = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                        requestEndTime - requestStartTime);
                    
                    // {
                        // std::lock_guard<std::mutex> lock(latencyMutex);
                        // latencies.push_back(latency);
                    // }
                    latencies[i] = latency;
                    
                    if (outcome.IsSuccess()) {
                        successCount++;
                        // auto& result = outcome.GetResult();
                        // auto& stream = result.GetBody();
                        auto& result = const_cast<Aws::S3::Model::GetObjectResult&>(outcome.GetResult());
                        auto& stream = result.GetBody();

                        // Calculate bytes transferred
                        stream.seekg(0, std::ios::end);
                        size_t bytes = stream.tellg();
                        totalBytes += bytes;
                        stream.seekg(0, std::ios::beg);
                        
                        // std::cout << "✓ Request completed successfully (" << bytes << " bytes, " 
                                //   << latency.count() << "ms)" << std::endl;
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
            
            // Add small delay to prevent overwhelming the service
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        // Wait for completion
        std::unique_lock<std::mutex> lock(completionMutex);
        completionCV.wait(lock, [&] { return completedRequests >= config.requestCount; });
        
        auto endTime = std::chrono::high_resolution_clock::now();
        
        return calculateMetrics(startTime, endTime, config);
    }
    
    // Test S3CrtClient async performance
    PerformanceMetrics testS3CrtClientAsync(const TestConfig& config) {
        std::cout << "\n=== Testing S3CrtClient Async: " << config.testName << " ===" << std::endl;
        
        // Configure S3CrtClient
        Aws::S3Crt::ClientConfiguration clientConfig;
        clientConfig.region = "us-west-2";
        clientConfig.throughputTargetGbps = 20.0; // Set high throughput target
        
        Aws::S3Crt::S3CrtClient s3CrtClient(clientConfig);
        
        resetCounters();
        auto startTime = std::chrono::high_resolution_clock::now();
        latencies.resize(config.requestCount);
        
        // Launch async requests
        for (int i = 0; i < config.requestCount; ++i) {
            Aws::S3Crt::Model::GetObjectRequest request;
            request.SetBucket(config.bucketName.c_str());
            request.SetKey(config.objectKeyPrefix + "_" + std::to_string(i % config.fileCount));
            
            auto requestStartTime = std::chrono::high_resolution_clock::now();
            
            s3CrtClient.GetObjectAsync(request,
                [this, requestStartTime, config, i](const Aws::S3Crt::S3CrtClient* client,
                                                const Aws::S3Crt::Model::GetObjectRequest& req,
                                                const Aws::S3Crt::Model::GetObjectOutcome& outcome,
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                    
                    auto requestEndTime = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                        requestEndTime - requestStartTime);
                    
                    // {
                    //     std::lock_guard<std::mutex> lock(latencyMutex);
                    //     latencies.push_back(latency);
                    // }
                    latencies[i] = latency;
                    
                    if (outcome.IsSuccess()) {
                        successCount++;
                        // auto& result = outcome.GetResult();
                        // auto& stream = result.GetBody();
                        auto& result = const_cast<Aws::S3Crt::Model::GetObjectResult&>(outcome.GetResult());
                        auto& stream = result.GetBody();
                        
                        // Calculate bytes transferred
                        stream.seekg(0, std::ios::end);
                        size_t bytes = stream.tellg();
                        totalBytes += bytes;
                        stream.seekg(0, std::ios::beg);
                        
                        // std::cout << "✓ Request completed successfully (" << bytes << " bytes, " 
                                //   << latency.count() << "ms)" << std::endl;
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
            
            // Add small delay to prevent overwhelming the service
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        // Wait for completion
        std::unique_lock<std::mutex> lock(completionMutex);
        completionCV.wait(lock, [&] { return completedRequests >= config.requestCount; });
        
        auto endTime = std::chrono::high_resolution_clock::now();
        
        return calculateMetrics(startTime, endTime, config);
    }
    
    PerformanceMetrics testS3ClientHeadObjectAsync(const TestConfig& config) {
        std::cout << "\n=== Testing S3Client HeadObject Async: " << config.testName << " ===" << std::endl;

        // Configure S3Client
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-east-1";
        clientConfig.executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(config.concurrencyLevel);
        clientConfig.scheme = Aws::Http::Scheme::HTTP;
        clientConfig.endpointOverride = "http://127.0.0.1:9000";
        auto credentials = Aws::Auth::AWSCredentials("minioadmin", "minioadmin");
        Aws::S3::S3Client s3Client(credentials, clientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);


        resetCounters();
        auto startTime = std::chrono::high_resolution_clock::now();
        latencies.resize(config.requestCount);

        // Launch async requests
        for (int i = 0; i < config.requestCount; ++i) {
            Aws::S3::Model::HeadObjectRequest request;
            request.SetBucket(config.bucketName.c_str());
            request.SetKey(config.objectKeyPrefix);
            // request.SetKey(config.objectKeyPrefix + "_" + std::to_string(i % config.fileCount));

            auto requestStartTime = std::chrono::high_resolution_clock::now();

            s3Client.HeadObjectAsync(request,
                [this, requestStartTime, config, i](const Aws::S3::S3Client* client,
                                                const Aws::S3::Model::HeadObjectRequest& req,
                                                const Aws::S3::Model::HeadObjectOutcome& outcome,
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                    
                    auto requestEndTime = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                        requestEndTime - requestStartTime);

                    latencies[i] = latency;

                    if (outcome.IsSuccess()) {
                        successCount++;
                        totalBytes += outcome.GetResult().GetContentLength();
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

    PerformanceMetrics testS3CrtClientHeadObjectAsync(const TestConfig& config) {
        std::cout << "\n=== Testing S3CrtClient HeadObject Async: " << config.testName << " ===" << std::endl;
        
        // Configure S3CrtClient
        Aws::S3Crt::ClientConfiguration clientConfig;
        clientConfig.region = "us-east-1";
        clientConfig.throughputTargetGbps = 20.0; // Set high throughput target
        clientConfig.scheme = Aws::Http::Scheme::HTTP;
        clientConfig.endpointOverride = "http://127.0.0.1:9000";

        auto credentials = Aws::Auth::AWSCredentials("minioadmin", "minioadmin");
        Aws::S3Crt::S3CrtClient s3CrtClient(credentials, clientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
        
        resetCounters();
        auto startTime = std::chrono::high_resolution_clock::now();
        latencies.resize(config.requestCount);
        
        // Launch async requests
        for (int i = 0; i < config.requestCount; ++i) {
            Aws::S3Crt::Model::HeadObjectRequest request;
            request.SetBucket(config.bucketName.c_str());
            request.SetKey(config.objectKeyPrefix);
            // request.SetKey(config.objectKeyPrefix + "_" + std::to_string(i % config.fileCount));
            
            auto requestStartTime = std::chrono::high_resolution_clock::now();
            
            s3CrtClient.HeadObjectAsync(request,
                [this, requestStartTime, config, i](const Aws::S3Crt::S3CrtClient* client,
                                                const Aws::S3Crt::Model::HeadObjectRequest& req,
                                                const Aws::S3Crt::Model::HeadObjectOutcome& outcome,
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                    
                    auto requestEndTime = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                        requestEndTime - requestStartTime);
                    
                    latencies[i] = latency;
                    
                    if (outcome.IsSuccess()) {
                        successCount++;
                        totalBytes += outcome.GetResult().GetContentLength();
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
private:
    void resetCounters() {
        completedRequests = 0;
        successCount = 0;
        failureCount = 0;
        totalBytes = 0;
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
    PerformanceTester tester;
    
    // Test configurations for different scenarios
    std::vector<TestConfig> testConfigs = {
        // Small objects, low concurrency
        {"a-bucket", "files/index_files/459290575288344123/1/459290575288139881/459290575288343516/_mem.index.bin", 1024 * 1024, 16, 1, 10, "Small Objects (1MB), Low Concurrency"},
        
        // // Medium objects, medium concurrency
        // {"uat-test-bucket-temp-001", "test-object-4.0mb.dat", 4 * 1024 * 1024, 16, 1000, 10, "Medium Objects (4MB), Medium Concurrency"},
        
        // // Large objects, high concurrency
        // {"your-test-bucket", "test-large-object.dat", 100 * 1024 * 1024, 10, 5, 10, "Large Objects (100MB), High Concurrency"},
    };
    
    for (const auto& config : testConfigs) {
        std::cout << "\n" << std::string(80, '#') << std::endl;
        std::cout << "Running Test: " << config.testName << std::endl;
        std::cout << "Bucket: " << config.bucketName << ", Object: " << config.objectKeyPrefix << std::endl;
        std::cout << "Concurrency: " << config.concurrencyLevel << ", Requests: " << config.requestCount << ", Files: " << config.fileCount << std::endl;
        std::cout << std::string(80, '#') << std::endl;
        
        // Test S3Client
        // auto s3Metrics = tester.testS3ClientAsync(config);
        // printMetrics("S3Client GetObject", s3Metrics);
        
        // Small delay between tests
        // std::this_thread::sleep_for(std::chrono::seconds(2));
        
        // Test S3CrtClient
        // auto s3CrtMetrics = tester.testS3CrtClientAsync(config);
        // printMetrics("S3CrtClient GetObject", s3CrtMetrics);
        
        // Small delay between tests
        // std::this_thread::sleep_for(std::chrono::seconds(2));

        auto s3HeadMetrics = tester.testS3ClientHeadObjectAsync(config);
        printMetrics("S3Client HeadObject", s3HeadMetrics);
        
        // Small delay between tests
        // std::this_thread::sleep_for(std::chrono::seconds(2));

        auto s3CrtHeadMetrics = tester.testS3CrtClientHeadObjectAsync(config);
        printMetrics("S3CrtClient HeadObject", s3CrtHeadMetrics);
        
        // // Compare results
        // compareMetrics(s3Metrics, s3CrtMetrics);
        
        // Save results to file
        // saveResultsToFile(config, s3Metrics, s3CrtMetrics);
    }
}

int main(int argc, char* argv[]) {
    // Initialize AWS SDK
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    options.loggingOptions.logger_create_fn = [] {
        return std::make_shared<Aws::Utils::Logging::DefaultLogSystem>(
            Aws::Utils::Logging::LogLevel::Info, "performance_test");
    };
    
    Aws::InitAPI(options);
    
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
    }
    
    // Cleanup AWS SDK
    Aws::ShutdownAPI(options);
    
    return 0;
} 