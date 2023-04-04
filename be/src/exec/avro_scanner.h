// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string_view>

#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/file_scanner.h"
#include "exec/json_scanner.h"
#include "exprs/json_functions.h"
#include "fs/fs.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "util/raw_container.h"
#include "util/slice.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "avro.h"
#include "libserdes/serdes.h"
#ifdef __cplusplus
}
#endif

namespace starrocks {

using AvroPath = SimpleJsonPath;


class AvroBenchData {
public:
    StatusOr<avro_value_t> get_next_row() {
        if (_cur_row >= _avro_values.size()) {
            return Status::EndOfFile("no rows in buffer");
        } else {
            return _avro_values[_cur_row++];
        }
    }

    void put_row(avro_value_t avro_value) {
        _avro_values.emplace_back(avro_value);
    }

    int64_t get_rows() {
        return _avro_values.size();
    }

private:
    std::vector<avro_value_t> _avro_values;
    int64_t _cur_row = 0;
};

class AvroScanner final : public FileScanner {
public:
    AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter);

    AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, AvroBenchData bench_data);

    // A new constructor is introduced for the single test.
    AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, std::string schema_text);
    ~AvroScanner() override;

    // Open this scanner, will initialize information needed
    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    // Close this scanner
    void close() override;

    static std::string preprocess_jsonpaths(std::string jsonpath);
    struct SlotInfo {
        SlotInfo() : id_(-2) {}
        SlotId id_;
        TypeDescriptor type_;
        std::string key_;
    };

private:
    Status _construct_avro_types();
    Status _construct_cast_exprs();
    StatusOr<ChunkPtr> _cast_chunk(const starrocks::ChunkPtr& src_chunk);
    Status _create_src_chunk(ChunkPtr* chunk);
    Status _parse_avro(Chunk* chunk, std::shared_ptr<SequentialFile> file);
    void _report_error(const std::string& line, const std::string& err_msg);
    Status _construct_row(avro_value_t avro_value, Chunk* chunk);
    void _materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);
    Status _construct_column(const avro_value_t& input_value, Column* column, const TypeDescriptor& type_desc,
                             const std::string& col_name);
    Status _extract_field(avro_value_t& input_value, std::vector<AvroPath> paths, avro_value_t& output_value);
    Status _handle_union(avro_value_t input_value, avro_value_t& branch);
    Status _get_array_element(avro_value_t* cur_value, size_t idx, avro_value_t* element);
    std::string _preprocess_jsonpaths(std::string jsonpath);
    Status _construct_row_without_jsonpath(const avro_value_t& avro_value, Chunk* chunk);

    const TBrokerScanRange& _scan_range;
    serdes_t* _serdes;
    std::string _schema_text;
    char _err_buf[512];
    std::vector<Column*> _column_raw_ptrs;
    ByteBufferPtr _parser_buf;
    std::vector<std::vector<AvroPath>> _json_paths;
    std::vector<TypeDescriptor> _avro_types;
    std::vector<Expr*> _cast_exprs;
    ObjectPool _pool;
    std::shared_ptr<SequentialFile> _file;
    std::unordered_map<std::string_view, SlotDescriptor*> _slot_desc_dict;
    // std::vector<PreviousParsedItem> _prev_parsed_position;
    std::vector<bool> _found_columns;
    std::vector<SlotInfo> _data_idx_to_slot;
    bool _init_data_idx_to_slot_once;

#if BE_TEST
    avro_file_reader_t _dbreader;
#endif

    AvroBenchData _bench_data;
};

} // namespace starrocks