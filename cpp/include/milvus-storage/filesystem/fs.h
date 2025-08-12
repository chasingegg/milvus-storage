// Copyright 2024 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/util/uri.h>
#include "arrow/util/string.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/logging.h"
#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/io/memory.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/filesystem/type_fwd.h"
#include "milvus-storage/common/log.h"

#include <chrono>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/threading/Executor.h>
// #include <aws/core/utils/threading/PooledThreadExecutor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <memory>
#include <string>
#include <sstream>
#include <fstream>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <fstream>
#include <memory>
#include <iomanip>
#include <future>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include "milvus-storage/common/result.h"
#include "milvus-storage/common/config.h"
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/S3CrtErrors.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3-crt/model/AbortMultipartUploadRequest.h>
#include <aws/s3-crt/model/CompleteMultipartUploadRequest.h>
#include <aws/s3-crt/model/CompletedMultipartUpload.h>
#include <aws/s3-crt/model/CompletedPart.h>
#include <aws/s3-crt/model/CopyObjectRequest.h>
#include <aws/s3-crt/model/CreateBucketRequest.h>
#include <aws/s3-crt/model/CreateMultipartUploadRequest.h>
#include <aws/s3-crt/model/DeleteBucketRequest.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/DeleteObjectsRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/HeadBucketRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/s3-crt/model/ListBucketsResult.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/ObjectCannedACL.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/UploadPartRequest.h>

using ::arrow::fs::internal::RemoveTrailingSlash;

namespace milvus_storage {

  class DirectIOStreambuf : public std::streambuf {
public:
    explicit DirectIOStreambuf(const std::string& filename, size_t buffer_size = 4096 * 16)
        : d_filename(filename),
          d_fd(-1),
          d_buffer_size(buffer_size),
          d_buffer(nullptr),
          d_total_bytes_written(0) {
        
        if (d_buffer_size % d_alignment != 0) {
            d_buffer_size = ((d_buffer_size / d_alignment) + 1) * d_alignment;
        }

        d_fd = open(d_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0666);
        if (d_fd == -1) {
            perror("DirectIOStreambuf: Failed to open file with O_DIRECT");
            throw std::runtime_error("Failed to open file with O_DIRECT for " + d_filename);
        }

        if (posix_memalign(reinterpret_cast<void**>(&d_buffer), d_alignment, d_buffer_size) != 0) {
            close(d_fd);
            throw std::runtime_error("Failed to allocate aligned memory for DirectIOStreambuf");
        }

        setp(d_buffer, d_buffer + d_buffer_size);
    }

    ~DirectIOStreambuf() override {
        sync();
        if (d_fd != -1) {
            close(d_fd);
            d_fd = -1;
        }
        if (d_buffer) {
            free(d_buffer);
            d_buffer = nullptr;
        }
    }

protected:
    int_type overflow(int_type ch = traits_type::eof()) override {
        if (d_fd == -1) {
            return traits_type::eof();
        }

        if (!flush_aligned_blocks()) {
            return traits_type::eof();
        }

        if (ch != traits_type::eof()) {
            if (pptr() < epptr()) {
                *pptr() = static_cast<char>(ch);
                pbump(1);
            } else {
                return traits_type::eof();
            }
        }

        return traits_type::not_eof(ch);
    }

    int sync() override {
        if (d_fd == -1) return -1;
        
        if (!flush_aligned_blocks()) return -1;

        size_t remainder_size = pptr() - pbase();
        if (remainder_size > 0) {
            size_t padded_size = ((remainder_size + d_alignment - 1) / d_alignment) * d_alignment;

            memset(pbase() + remainder_size, 0, padded_size - remainder_size);

            ssize_t written = write(d_fd, pbase(), padded_size);
            if (written == -1 || static_cast<size_t>(written) != padded_size) {
                perror("DirectIOStreambuf: Final padded write failed");
                // Continue to attempt truncation
            }

            d_total_bytes_written += remainder_size;
            
            // Clear the buffer since it has been flushed
            setp(pbase(), epptr());

            if (ftruncate(d_fd, d_total_bytes_written) != 0) {
                perror("DirectIOStreambuf: ftruncate failed");
                return -1;
            }
        }
        
        return 0;
    }

private:
    bool flush_aligned_blocks() {
        size_t current_size = pptr() - pbase();
        if (current_size == 0 || d_fd == -1) {
            return true;
        }
        
        size_t write_size = (current_size / d_alignment) * d_alignment;
        if (write_size > 0) {
            ssize_t written = write(d_fd, pbase(), write_size);
            if (written == -1) {
                perror("DirectIOStreambuf: write failed");
                return false;
            }
            if (static_cast<size_t>(written) != write_size) {
                return false; // Short write is an error
            }
            
            d_total_bytes_written += written;
            size_t remaining_size = current_size - written;
            memmove(pbase(), pbase() + written, remaining_size);
            setp(pbase(), epptr());
            pbump(static_cast<int>(remaining_size));
        }
        return true;
    }

    std::string d_filename;
    int         d_fd;
    size_t      d_total_bytes_written;

    const size_t      d_alignment = 4096;
    size_t            d_buffer_size;
    char*             d_buffer;
};

/**
 * A simple Aws::IOStream wrapper around the DirectIOStreambuf.
 */
class DirectIOStream : public Aws::IOStream {
public:
    explicit DirectIOStream(const std::string& filename)
        : Aws::IOStream(&streambuf), streambuf(filename) {}
private:
    DirectIOStreambuf streambuf;
};


class ThreadPool {
public:
    ThreadPool(size_t);
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args) 
        -> std::future<typename std::result_of<F(Args...)>::type>;
    ~ThreadPool();
private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
    :   stop(false)
{
    for(size_t i = 0; i<threads; ++i)
        workers.emplace_back(
            [this]
            {
                for(;;)
                {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
                        if(this->stop && this->tasks.empty())
                            return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            }
        );
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;
    auto task = std::make_shared< std::packaged_task<return_type()> >(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if(stop) throw std::runtime_error("enqueue on stopped ThreadPool");
        tasks.emplace([task](){ (*task)(); });
    }
    condition.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for(std::thread &worker: workers)
        worker.join();
}

using ArrowFileSystemPtr = std::shared_ptr<arrow::fs::FileSystem>;

struct ArrowFileSystemConfig {
  std::string address = "localhost:9000";
  std::string bucket_name = "a-bucket";
  std::string access_key_id = "minioadmin";
  std::string access_key_value = "minioadmin";
  std::string root_path = "files";
  std::string storage_type = "minio";
  std::string cloud_provider = "aws";
  std::string iam_endpoint = "";
  std::string log_level = "warn";
  std::string region = "";
  bool useSSL = false;
  std::string sslCACert = "";
  bool useIAM = false;
  bool useVirtualHost = false;
  int64_t requestTimeoutMs = 3000;
  bool gcp_native_without_auth = false;
  std::string gcp_credential_json = "";
  bool use_custom_part_upload = true;

  std::string ToString() const {
    std::stringstream ss;
    ss << "[address=" << address << ", bucket_name=" << bucket_name << ", root_path=" << root_path
       << ", storage_type=" << storage_type << ", cloud_provider=" << cloud_provider
       << ", iam_endpoint=" << iam_endpoint << ", log_level=" << log_level << ", region=" << region
       << ", useSSL=" << std::boolalpha << useSSL << ", sslCACert=" << sslCACert.size()  // only print cert length
       << ", useIAM=" << std::boolalpha << useIAM << ", useVirtualHost=" << std::boolalpha << useVirtualHost
       << ", requestTimeoutMs=" << requestTimeoutMs << ", gcp_native_without_auth=" << std::boolalpha
       << gcp_native_without_auth << "]";

    return ss.str();
  }
};

class FileSystemProducer {
  public:
  virtual ~FileSystemProducer() = default;

  virtual Result<ArrowFileSystemPtr> Make() = 0;
};

inline Aws::String ConvertToAwsString(const std::string& str) {
  // Direct construction of Aws::String from std::string doesn't work because
  // it uses a specific Allocator class.
  return Aws::String(str.begin(), str.end());
}

inline std::string FormatRangeString(int64_t start, int64_t length) {
  // Format a HTTP range header value
  std::stringstream ss;
  ss << "bytes=" << start << "-" << start + length - 1;
  return ss.str();
}

class CrtStringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
  public:
  CrtStringViewStream(const void* data, int64_t nbytes)
      : Aws::Utils::Stream::PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
                                                  static_cast<size_t>(nbytes)),
        std::iostream(this) {}
};

inline Aws::IOStreamFactory CrtAwsWriteableStreamFactory(void* data, int64_t nbytes) {
  return [=]() { return Aws::New<CrtStringViewStream>("", data, nbytes); };
}

struct S3Path {
  std::string bucket;
  std::string key;
};

inline S3Path FromString(const std::string& s) {
  constexpr char kSep = '/';
  const auto src = RemoveTrailingSlash(s);
  auto first_sep = src.find_first_of(kSep);
  if (first_sep == 0) {
    return S3Path{"", ""};
  }
  if (first_sep == std::string::npos) {
    return S3Path{std::string(src), ""};
  }
  S3Path path;
  path.bucket = std::string(src.substr(0, first_sep));
  path.key = std::string(src.substr(first_sep + 1));
  return path;
}

class S3CrtClientWrapper : public Aws::S3Crt::S3CrtClient {
  public:
  S3CrtClientWrapper(std::shared_ptr<Aws::S3Crt::S3CrtClient> client)
      : s3_crt_client_(std::move(client)) {
      mmap_thread_pool_ = std::make_shared<ThreadPool>(8);
  }


  size_t GetObjectSize(const std::string& bucket, const std::string& key) {
    Aws::S3Crt::Model::HeadObjectRequest req;
    req.SetBucket(ConvertToAwsString(bucket));
    req.SetKey(ConvertToAwsString(key));
    auto outcome = s3_crt_client_->HeadObject(req);
    if (!outcome.IsSuccess()) {
      const auto& err = outcome.GetError();
      std::stringstream ss;
      ss << "FUCK GetObjectSize failed. "
         << "Bucket: " << bucket << ", Key: " << key
         << ", ErrorType: " << static_cast<int>(err.GetErrorType())
         << ", ExceptionName: " << err.GetExceptionName()
         << ", Message: " << err.GetMessage();
      throw std::runtime_error(ss.str());
    }
    return outcome.GetResult().GetContentLength();
  }

  size_t GetObjectRange(const std::string& bucket, const std::string& key, int64_t position, int64_t nbytes, void* out) {
    if (nbytes == 0) {
      return 0;
    }
    Aws::S3Crt::Model::GetObjectRequest req;
    req.SetBucket(ConvertToAwsString(bucket));
    req.SetKey(ConvertToAwsString(key));
    // req.SetRange(ConvertToAwsString(FormatRangeString(position, nbytes)));
    LOG_STORAGE_INFO_ << "FUCK " << FormatRangeString(position, nbytes);
    req.SetRange(FormatRangeString(position, nbytes));
    req.SetResponseStreamFactory(CrtAwsWriteableStreamFactory(out, nbytes));

    auto outcome = s3_crt_client_->GetObject(req);
    if (!outcome.IsSuccess()) {
      LOG_STORAGE_INFO_ << "FUCK " << FormatRangeString(position, nbytes) << " " << bucket << " " << key << " " << nbytes << "GetObjectRange failed. "
         << "Bucket: " << bucket << ", Key: " << key
         << ", ErrorType: " << static_cast<int>(outcome.GetError().GetErrorType())
         << ", ExceptionName: " << outcome.GetError().GetExceptionName()
         << ", Message: " << outcome.GetError().GetMessage();
      throw std::runtime_error(outcome.GetError().GetMessage());
    }

    return outcome.GetResult().GetContentLength();
  }

  size_t GetObjectRangeToFile(const std::string& bucket, const std::string& key, int64_t position, int64_t nbytes, const std::string& local_filepath) {
    Aws::S3Crt::Model::GetObjectRequest req;
    req.SetBucket(ConvertToAwsString(bucket));
    req.SetKey(ConvertToAwsString(key));
    req.SetRange(ConvertToAwsString(FormatRangeString(position, nbytes)));
    // req.SetResponseStreamFactory([=]() {
    //   return Aws::New<DirectIOStream>("S3DirectIOStream", local_filepath);
    // });
    req.SetResponseStreamFactory(Aws::IOStreamFactory([local_filepath](){ 
      return Aws::New<Aws::FStream>("GetObjectStream",
                                    local_filepath.c_str(),
                                    std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    }));
    auto outcome = s3_crt_client_->GetObject(req);
    if (!outcome.IsSuccess()) {
      throw std::runtime_error(outcome.GetError().GetMessage());
    }
    outcome.GetResult().GetBody().flush();
    return outcome.GetResult().GetContentLength();
  }

  size_t ReadBatchToFile(const std::string& bucket, const std::string& key, const std::string& local_filepath_prefix,
      const std::vector<size_t>& offsets, const std::vector<size_t>& lengths, const std::vector<int64_t>& file_names,
      const std::function<void(int)>& mmap_func) {
  
    std::atomic<size_t> completed_requests = 0;
    std::mutex cv_mutex;
    std::condition_variable cv;

    {
      std::unique_lock<std::mutex> lock(mu_);

      auto start_time = std::chrono::high_resolution_clock::now();
      
      for (size_t i = 0; i < offsets.size(); i++) {
        size_t start = offsets[i];
        size_t length = lengths[i];
        std::string local_filepath = local_filepath_prefix + "_" + std::to_string(file_names[i]);

        Aws::S3Crt::Model::GetObjectRequest req;
        req.SetBucket(ConvertToAwsString(bucket));
        req.SetKey(ConvertToAwsString(key));
        req.SetRange(ConvertToAwsString(FormatRangeString(start, length)));
        // req.SetResponseStreamFactory([=]() {
        //   return Aws::New<DirectIOStream>("S3DirectIOStream", local_filepath);
        // });
        req.SetResponseStreamFactory(Aws::IOStreamFactory([local_filepath](){ 
          return Aws::New<Aws::FStream>("GetObjectStream",
                                        local_filepath.c_str(),
                                        std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        }));

        s3_crt_client_->GetObjectAsync(req, 
            [this, i, &local_filepath, &completed_requests, &offsets, &cv, &cv_mutex, &mmap_func](
                const Aws::S3Crt::S3CrtClient*, const Aws::S3Crt::Model::GetObjectRequest&,
                const Aws::S3Crt::Model::GetObjectOutcome& outcome,
                const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            
            // auto end_time = std::chrono::high_resolution_clock::now();
            // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            // LOG_STORAGE_INFO_ << "FUCK GetFileAsync " << i << " " << duration.count() << "ms";

            if (outcome.IsSuccess()) {
              outcome.GetResult().GetBody().flush();
              // mmap_thread_pool_->enqueue([this, i, &local_filepath, &mmap_func, &completed_requests, &offsets, &cv, &cv_mutex]() {
                mmap_func(i);
                if (++completed_requests == offsets.size()) {
                    std::lock_guard<std::mutex> lock(cv_mutex);
                    cv.notify_one();
                }
              // });
            } else {
                LOG_STORAGE_INFO_ << "FUCK GetFileAsync " << i << " failed";
                remove(local_filepath.c_str());
                if (++completed_requests == offsets.size()) {
                    std::lock_guard<std::mutex> lock(cv_mutex);
                    cv.notify_one();
                }
            }
            // LOG_STORAGE_INFO_ << "FUCK GetFileAsync " << i << " completed";
        });
      }
    }

    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait(lock, [&] { return completed_requests == offsets.size(); });

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG_STORAGE_INFO_ << "FUCK GetFileAsync " << " " << duration.count() << "ms";

    // Note: The caller of this function is responsible for deleting the
    // downloaded files on disk using the path in the returned MappedFile objects.
    return offsets.size();
  }
 
  size_t ReadBatchToMemory(const std::string& bucket, const std::string& key, std::vector<void*>& out,
      const std::vector<size_t>& offsets, const std::vector<size_t>& lengths) {
    std::atomic<size_t> completed_requests = 0;
    std::mutex cv_mutex;
    std::condition_variable cv;
    
    for (size_t i = 0; i < offsets.size(); i++) {
      size_t start = offsets[i];
      size_t length = lengths[i];

      Aws::S3Crt::Model::GetObjectRequest req;
      req.SetBucket(ConvertToAwsString(bucket));
      req.SetKey(ConvertToAwsString(key));
      req.SetRange(ConvertToAwsString(FormatRangeString(start, length)));
      req.SetResponseStreamFactory(CrtAwsWriteableStreamFactory(out[i], length));

      auto start_time = std::chrono::high_resolution_clock::now();

      s3_crt_client_->GetObjectAsync(req, 
          [this, i, &completed_requests, &offsets, &cv, &cv_mutex, start_time](
              const Aws::S3Crt::S3CrtClient*, const Aws::S3Crt::Model::GetObjectRequest&,
              const Aws::S3Crt::Model::GetObjectOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
          
          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
          LOG_STORAGE_INFO_ << "FUCK GetMemAsync " << i << " " << duration.count() << "ms";

          if (++completed_requests == offsets.size()) {
              std::lock_guard<std::mutex> lock(cv_mutex);
              cv.notify_one();
          }
      });
    }



    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait(lock, [&] { return completed_requests == offsets.size(); });

    return offsets.size();
  }

  private:
    std::shared_ptr<Aws::S3Crt::S3CrtClient> s3_crt_client_ = nullptr;
    std::shared_ptr<ThreadPool> mmap_thread_pool_ = nullptr;
    std::mutex mu_;
};

class ArrowFileSystemSingleton {
  private:
  ArrowFileSystemSingleton(){};

  public:
  ArrowFileSystemSingleton(const ArrowFileSystemSingleton&) = delete;
  ArrowFileSystemSingleton& operator=(const ArrowFileSystemSingleton&) = delete;

  ~ArrowFileSystemSingleton() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_finalized_) {
      if (s3_crt_client_ != nullptr) {
        s3_crt_client_.reset();
        Aws::ShutdownAPI(aws_options_);
      }
      is_finalized_ = true;
    }
    if (afs_ != nullptr) {
      afs_.reset();
    }
  }

  static ArrowFileSystemSingleton& GetInstance() {
    static ArrowFileSystemSingleton instance;
    return instance;
  }

  void Init(const ArrowFileSystemConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_initialized_) {
      if (s3_crt_client_ == nullptr) {
        s3_crt_client_ = createCrtClient(config);
      }
      is_initialized_ = true;
    }
    if (afs_ == nullptr) {
      afs_ = createArrowFileSystem(config).value();
    }
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_finalized_) {
      if (s3_crt_client_ != nullptr) {
        s3_crt_client_.reset();
        Aws::ShutdownAPI(aws_options_);
      }
      is_finalized_ = true;
    }
    if (afs_ != nullptr) {
      afs_.reset();
    }
  }

  ArrowFileSystemPtr GetArrowFileSystem() {
    std::lock_guard<std::mutex> lock(mutex_);
    return afs_;
  }

  std::shared_ptr<S3CrtClientWrapper> GetCrtClinet() {
    std::lock_guard<std::mutex> lock(client_mutex_);
    return s3_crt_client_;
  }

  std::pair<std::string, std::string> GetBucketAndKey(const std::string& path) {
    auto path_pair = FromString(path);
    return {path_pair.bucket, path_pair.key};
  }

  private:
  Result<ArrowFileSystemPtr> createArrowFileSystem(const ArrowFileSystemConfig& config);

  std::shared_ptr<S3CrtClientWrapper> createCrtClient(const ArrowFileSystemConfig& config);

  private:
  std::shared_ptr<S3CrtClientWrapper> s3_crt_client_ = nullptr;
  Aws::SDKOptions aws_options_;
  std::mutex client_mutex_;
  std::atomic<bool> is_initialized_ = false;
  std::atomic<bool> is_finalized_ = false;

  ArrowFileSystemPtr afs_ = nullptr;
  std::mutex mutex_;
};

enum class StorageType {
  None = 0,
  Local = 1,
  Minio = 2,
  Remote = 3,
};

enum class CloudProviderType : int8_t {
  UNKNOWN = 0,
  AWS = 1,
  GCP = 2,
  ALIYUN = 3,
  AZURE = 4,
  TENCENTCLOUD = 5,
};

static std::map<std::string, StorageType> StorageType_Map = {{"local", StorageType::Local},
                                                             {"remote", StorageType::Remote}};

static std::map<std::string, CloudProviderType> CloudProviderType_Map = {{"aws", CloudProviderType::AWS},
                                                                         {"gcp", CloudProviderType::GCP},
                                                                         {"aliyun", CloudProviderType::ALIYUN},
                                                                         {"azure", CloudProviderType::AZURE},
                                                                         {"tencent", CloudProviderType::TENCENTCLOUD}};

}  // namespace milvus_storage
