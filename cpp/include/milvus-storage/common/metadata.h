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

#include <parquet/metadata.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <cstdint>
#include <memory>
#include <string>
#include "milvus-storage/common/result.h"
#include "milvus-storage/common/type_fwd.h"
#include "milvus-storage/packed/chunk_manager.h"

namespace milvus_storage {

class FieldIDList {
  public:
  FieldIDList() = default;

  explicit FieldIDList(const std::vector<FieldID>& field_ids);

  bool operator==(const FieldIDList& other) const;

  void Add(FieldID field_id);

  FieldID Get(size_t index) const;

  size_t size() const;

  bool empty() const;

  static Result<FieldIDList> Make(const std::shared_ptr<arrow::Schema>& schema);

  std::string ToString() const;

  private:
  std::vector<FieldID> field_ids_;
};

class GroupFieldIDList {
  public:
  GroupFieldIDList() = default;

  explicit GroupFieldIDList(int64_t size);

  explicit GroupFieldIDList(const std::vector<std::vector<int>>& list);

  explicit GroupFieldIDList(const std::vector<FieldIDList>& list);

  static GroupFieldIDList Make(std::vector<std::vector<int>>& column_groups, FieldIDList& field_id_list);

  bool operator==(const GroupFieldIDList& other) const;

  void AddFieldIDList(const FieldIDList& field_ids);

  FieldIDList GetFieldIDList(size_t index) const;

  size_t num_groups() const;

  bool empty() const;

  std::string Serialize() const;

  static GroupFieldIDList Deserialize(const std::string& input);

  private:
  std::vector<FieldIDList> list_;
};

class RowGroupMetadata {
  public:
  RowGroupMetadata() = default;

  explicit RowGroupMetadata(size_t memory_size, int64_t row_num, int64_t row_offset);

  size_t memory_size() const;
  int64_t row_num() const;
  int64_t row_offset() const;

  std::string ToString() const;
  std::string Serialize() const;
  static RowGroupMetadata Deserialize(const std::string& input);

  private:
  size_t memory_size_;
  int64_t row_num_;
  int64_t row_offset_;
};

class RowGroupMetadataVector {
  public:
  RowGroupMetadataVector() = default;

  explicit RowGroupMetadataVector(const std::vector<RowGroupMetadata>& metadata);

  void Add(const RowGroupMetadata& metadata);

  const RowGroupMetadata& Get(size_t index) const;

  size_t size() const;

  size_t row_num() const;

  size_t memory_size() const;

  void clear();

  std::string ToString() const;

  std::string Serialize() const;

  static RowGroupMetadataVector Deserialize(const std::string& input);

  private:
  std::vector<RowGroupMetadata> vector_;
};

class PackedFileMetadata {
  public:
  PackedFileMetadata() = default;

  explicit PackedFileMetadata(const std::shared_ptr<parquet::FileMetaData>& metadata,
                              const RowGroupMetadataVector& row_group_metadata,
                              const std::map<FieldID, ColumnOffset>& field_id_mapping,
                              const GroupFieldIDList& group_field_id_list,
                              const std::string& storage_version);

  static Result<std::shared_ptr<PackedFileMetadata>> Make(std::shared_ptr<parquet::FileMetaData> metadata);

  const RowGroupMetadataVector GetRowGroupMetadataVector();

  const RowGroupMetadata& GetRowGroupMetadata(int index) const;

  const std::map<FieldID, ColumnOffset>& GetFieldIDMapping();

  const GroupFieldIDList GetGroupFieldIDList();

  const std::shared_ptr<parquet::FileMetaData>& GetParquetMetadata();

  const std::string& GetStorageVersion() const;

  int num_row_groups() const;

  size_t total_memory_size() const;

  private:
  std::shared_ptr<parquet::FileMetaData> parquet_metadata_;
  RowGroupMetadataVector row_group_metadata_;
  std::map<FieldID, ColumnOffset> field_id_mapping_;
  GroupFieldIDList group_field_id_list_;
  std::string storage_version_;
};

}  // namespace milvus_storage
