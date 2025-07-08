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

// MappedFile struct and mmap_file function
struct MappedFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;
    std::string path;

    ~MappedFile() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd != -1) {
            close(fd);
        }
    }

    MappedFile(MappedFile&& other) noexcept 
        : data(other.data), size(other.size), fd(other.fd), path(std::move(other.path)) {
        other.data = nullptr;
        other.fd = -1;
    }
    MappedFile& operator=(MappedFile&& other) noexcept {
        if (this != &other) {
            if (data != nullptr && data != MAP_FAILED) munmap(data, size);
            if (fd != -1) close(fd);
            data = other.data;
            size = other.size;
            fd = other.fd;
            path = std::move(other.path);
            other.data = nullptr;
            other.fd = -1;
        }
        return *this;
    }

    MappedFile() = default;
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
};

inline MappedFile mmap_file(const char* filepath, size_t expected_size = 0) {
    MappedFile mapped_file;
    mapped_file.path = filepath;
    mapped_file.fd = open(filepath, O_RDONLY);
    if (mapped_file.fd == -1) {
        perror("open");
        return mapped_file;
    }

    struct stat sb;
    if (fstat(mapped_file.fd, &sb) == -1) {
        perror("fstat");
        close(mapped_file.fd);
        mapped_file.fd = -1;
        return mapped_file;
    }
    mapped_file.size = sb.st_size;

    if (expected_size > 0 && mapped_file.size != expected_size) {
        fprintf(stderr, "mmap_file error: file size %zu does not match expected size %zu for file %s\n", mapped_file.size, expected_size, filepath);
        close(mapped_file.fd);
        mapped_file.fd = -1;
        return mapped_file;
    }

    if (mapped_file.size == 0) { // Can't mmap empty file
        close(mapped_file.fd);
        mapped_file.fd = -1;
        mapped_file.data = nullptr; 
        return mapped_file;
    }

    mapped_file.data = mmap(NULL, mapped_file.size, PROT_READ, MAP_PRIVATE, mapped_file.fd, 0);
    if (mapped_file.data == MAP_FAILED) {
        perror("mmap");
        close(mapped_file.fd);
        mapped_file.fd = -1;
        return mapped_file;
    }
    
    return mapped_file;
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

inline Aws::String
ConvertToAwsString(const std::string& str) {
    return Aws::String(str.c_str(), str.size());
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
      : s3_crt_client_(std::move(client)) {}


  size_t GetObjectSize(const std::string& bucket, const std::string& key) {
    Aws::S3Crt::Model::HeadObjectRequest req;
    req.SetBucket(ConvertToAwsString(bucket));
    req.SetKey(ConvertToAwsString(key));
    auto outcome = s3_crt_client_->HeadObject(req);
    if (!outcome.IsSuccess()) {
      throw std::runtime_error(outcome.GetError().GetMessage());
    }
    return outcome.GetResult().GetContentLength();
  }

  size_t GetObjectRange(const std::string& bucket, const std::string& key, int64_t position, int64_t nbytes, void* out) {
    Aws::S3Crt::Model::GetObjectRequest req;
    req.SetBucket(ConvertToAwsString(bucket));
    req.SetKey(ConvertToAwsString(key));
    req.SetRange(ConvertToAwsString(FormatRangeString(position, nbytes)));
    req.SetResponseStreamFactory(CrtAwsWriteableStreamFactory(out, nbytes));

    auto outcome = s3_crt_client_->GetObject(req);
    if (!outcome.IsSuccess()) {
      throw std::runtime_error(outcome.GetError().GetMessage());
    }

    auto& stream = outcome.GetResult().GetBody();
    stream.ignore(nbytes);
    // NOTE: the stream is a stringstream by default, there is no actual error
    // to check for.  However, stream.fail() may return true if EOF is reached.
    return stream.gcount();
  }

  size_t ReadBatchToFile(const std::string& bucket, const std::string& key, const std::string& local_filepath_prefix,
      const std::vector<size_t>& offsets, const std::vector<size_t>& lengths, const std::vector<int64_t>& file_names,
      const std::function<void(int)>& mmap_func) {
  
    std::atomic<size_t> completed_requests = 0;
    std::mutex cv_mutex;
    std::condition_variable cv;
    
    for (size_t i = 0; i < offsets.size(); i++) {
      size_t start = offsets[i];
      size_t length = lengths[i];
      std::string local_filepath = local_filepath_prefix + "_" + std::to_string(file_names[i]);

      Aws::S3Crt::Model::GetObjectRequest req;
      req.SetBucket(ConvertToAwsString(bucket));
      req.SetKey(ConvertToAwsString(key));
      req.SetRange(ConvertToAwsString(FormatRangeString(start, length)));
      req.SetResponseStreamFactory(Aws::IOStreamFactory([local_filepath](){ 
        return Aws::New<Aws::FStream>("GetObjectStream",
                                      local_filepath.c_str(),
                                      std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
      }));

      auto start_time = std::chrono::high_resolution_clock::now();

      s3_crt_client_->GetObjectAsync(req, 
          [this, i, &local_filepath, &completed_requests, &offsets, &cv, &cv_mutex, &mmap_func, start_time](
              const Aws::S3Crt::S3CrtClient*, const Aws::S3Crt::Model::GetObjectRequest&,
              const Aws::S3Crt::Model::GetObjectOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
          
          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
          std::cout << "FUCK GetObjectAsync " << i << " took " << duration.count() << "ms" << std::endl;

          if (outcome.IsSuccess()) {
              mmap_func(i);
          } else {
              remove(local_filepath.c_str());
          }

          if (++completed_requests == offsets.size()) {
              std::lock_guard<std::mutex> lock(cv_mutex);
              cv.notify_one();
          }
      });
    }

    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait(lock, [&] { return completed_requests == offsets.size(); });

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
          std::cout << "FUCK GetObjectAsync " << i << " took " << duration.count() << "ms" << std::endl;

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
};

class ArrowFileSystemSingleton {
  private:
  ArrowFileSystemSingleton(){};

  public:
  ArrowFileSystemSingleton(const ArrowFileSystemSingleton&) = delete;
  ArrowFileSystemSingleton& operator=(const ArrowFileSystemSingleton&) = delete;

  ~ArrowFileSystemSingleton() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (afs_ != nullptr) {
      afs_.reset();
    }
    if (!is_finalized_) {
      if (s3_crt_client_ != nullptr) {
        s3_crt_client_.reset();
        Aws::ShutdownAPI(aws_options_);
      }
      is_finalized_ = true;
    }
  }

  static ArrowFileSystemSingleton& GetInstance() {
    static ArrowFileSystemSingleton instance;
    return instance;
  }

  void Init(const ArrowFileSystemConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (afs_ == nullptr) {
      afs_ = createArrowFileSystem(config).value();
    }
    if (!is_initialized_) {
      if (s3_crt_client_ == nullptr) {
        s3_crt_client_ = createCrtClient(config);
      }
      is_initialized_ = true;
    }
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (afs_ != nullptr) {
      afs_.reset();
    }
    if (!is_finalized_) {
      if (s3_crt_client_ != nullptr) {
        s3_crt_client_.reset();
        Aws::ShutdownAPI(aws_options_);
      }
      is_finalized_ = true;
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
