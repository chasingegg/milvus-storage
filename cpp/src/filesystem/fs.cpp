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

#include <arrow/filesystem/localfs.h>
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/filesystem/s3/s3_fs.h"
#include "milvus-storage/common/path_util.h"
#include "boost/filesystem/path.hpp"
#include <boost/filesystem/operations.hpp>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentials.h>
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

#ifdef MILVUS_AZURE_FS
#include "milvus-storage/filesystem/azure/azure_fs.h"
#endif

#ifdef MILVUS_OPENDAL
#endif

namespace milvus_storage {

Result<ArrowFileSystemPtr> ArrowFileSystemSingleton::createArrowFileSystem(const ArrowFileSystemConfig& config) {
  std::string out_path;
  auto storage_type = StorageType_Map[config.storage_type];
  switch (storage_type) {
    case StorageType::Local: {
      arrow::util::Uri uri_parser;
      auto uri = "file://" + config.root_path;
      RETURN_ARROW_NOT_OK(uri_parser.Parse(uri));
      ASSIGN_OR_RETURN_ARROW_NOT_OK(auto option, arrow::fs::LocalFileSystemOptions::FromUri(uri_parser, &out_path));
      boost::filesystem::path dir_path(out_path);
      if (!boost::filesystem::exists(dir_path)) {
        boost::filesystem::create_directories(dir_path);
      }
      return ArrowFileSystemPtr(new arrow::fs::LocalFileSystem(option));
    }
    case StorageType::Remote: {
      auto cloud_provider = CloudProviderType_Map[config.cloud_provider];
      switch (cloud_provider) {
#ifdef MILVUS_AZURE_FS
        case CloudProviderType::AZURE: {
          auto producer = std::make_shared<AzureFileSystemProducer>(config);
          return producer->Make();
        }
#endif
        case CloudProviderType::AWS:
        case CloudProviderType::GCP:
        case CloudProviderType::ALIYUN:
        case CloudProviderType::TENCENTCLOUD: {
          auto producer = std::make_shared<S3FileSystemProducer>(config);
          return producer->Make();
        }
        default: {
          return Status::InvalidArgument("Unsupported cloud provider: " + config.cloud_provider);
        }
      }
    }
    default: {
      return Status::InvalidArgument("Unsupported storage type: " + config.storage_type);
    }
  }
};

std::shared_ptr<S3CrtClientWrapper> ArrowFileSystemSingleton::createCrtClient(const ArrowFileSystemConfig& config) {
  
  aws_options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Fatal; // Reduced logging for less noise
  aws_options_.loggingOptions.logger_create_fn = [] {
      return std::make_shared<Aws::Utils::Logging::DefaultLogSystem>(
          Aws::Utils::Logging::LogLevel::Fatal, "performance_test");
  };
    aws_options_.ioOptions.clientBootstrap_create_fn =
        []() {
          Aws::Crt::Io::EventLoopGroup event_loop_group(1);
          Aws::Crt::Io::DefaultHostResolver default_host_resolver(
              event_loop_group, /*maxHosts=*/8, /*maxTTL=*/30);
          auto client_bootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>(
              "Aws_Init_Cleanup", event_loop_group, default_host_resolver);
          client_bootstrap->EnableBlockingShutdown();
          return client_bootstrap;
        };
  Aws::InitAPI(aws_options_);


  Aws::S3Crt::ClientConfiguration client_config;
  client_config.endpointOverride = ConvertToAwsString(config.address);

  // Three cases:
  // 1. no ssl, verifySSL=false
  // 2. self-signed certificate, verifySSL=false
  // 3. CA-signed certificate, verifySSL=true
  if (config.useSSL) {
      client_config.scheme = Aws::Http::Scheme::HTTPS;
      client_config.verifySSL = true;
      // if (!config.sslCACert.empty()) {
      //     client_config.caPath = ConvertToAwsString(config.sslCACert);
      //     client_config.verifySSL = false;
      // }
  } else {
      client_config.scheme = Aws::Http::Scheme::HTTP;
      client_config.verifySSL = false;
  }

  client_config.requestTimeoutMs = config.requestTimeoutMs == 0
                                ? 10000
                                : config.requestTimeoutMs;

  if (!config.region.empty()) {
      client_config.region = ConvertToAwsString(config.region);
  }
  client_config.throughputTargetGbps = 25;
  client_config.maxConnections = 80;

  std::cout << "FUCK client_config.endpointOverride: " << client_config.endpointOverride.c_str() << std::endl;
  std::cout << "FUCK client_config.region: " << client_config.region.c_str() << " region:" << config.region << std::endl;

  if (config.useIAM) {
      auto provider =
          std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
      auto aws_credentials = provider->GetAWSCredentials();
      assert(!aws_credentials.GetAWSAccessKeyId().empty());
      assert(!aws_credentials.GetAWSSecretKey().empty());
      assert(!aws_credentials.GetSessionToken().empty());

      auto s3_client = std::make_shared<Aws::S3Crt::S3CrtClient>(
          provider,
          client_config,
          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
          config.useVirtualHost);
      return std::make_shared<S3CrtClientWrapper>(std::move(s3_client));
  } else {
      auto s3_client = std::make_shared<Aws::S3Crt::S3CrtClient>(
        Aws::Auth::AWSCredentials(
            ConvertToAwsString(config.access_key_id),
            ConvertToAwsString(config.access_key_value)),
        client_config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        config.useVirtualHost);
      return std::make_shared<S3CrtClientWrapper>(std::move(s3_client));
  }
}

};  // namespace milvus_storage
