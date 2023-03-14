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

#include "exec/avro_scanner.h"

#include <fmt/format.h>
#include <ryu/ryu.h>
#include <iostream>

#include <algorithm>
#include <memory>
#include <sstream>
#include <utility>
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/json_parser.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/json_functions.h"
#include "formats/avro/nullable_column.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "column/adaptive_nullable_column.h"
#include <fstream>
#include <iostream>
#include <vector>
#include "exec/avro_scanner.h"
#include "exec/json_scanner.h"
#include <boost/algorithm/string.hpp>
#include "util/defer_op.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "libserdes/serdes-avro.h"
#ifdef __cplusplus
}
#endif

bool replace(std::string& str, const std::string& from, const std::string& to) {
    size_t start_pos = str.find(from);
    if(start_pos == std::string::npos)
        return false;
    str.replace(start_pos, from.length(), to);
    return true;
}

void replaceAll(std::string& str, const std::string& from, const std::string& to) {
    if(from.empty())
        return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}

namespace starrocks {

AvroScanner::AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _serdes(nullptr) {}

AvroScanner::AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, const std::string schema_text)
        : FileScanner(state, profile, scan_range.params, counter),
            _scan_range(scan_range),
            _schema_text(schema_text) {}

AvroScanner::~AvroScanner() {
#if BE_TEST
    avro_file_reader_close(_dbreader);
#else
    if (_serdes != nullptr) {
        free(_serdes);
    }
#endif
}

// Previously, when parsing avro through JsonScanner, we used.*. to handle union data types, 
// but for the new implementation, we no longer need this pattern. For example, for the following 
// avro schema:
// {
//     "name": "raw_log",
//     "type": ["null", {
//         "fields": [{
//             "name": "id",
//             "type": "string"
//         }, {
//             "name": "data",
//             "type": "string"
//         }],
//         "name": "logs",
//         "type": "record"
//     }]
// }
// If you want to select the id field, for the new implementation jsonpath can be written as $.id instead 
// of $.*.id. To ensure forward compatibility, we find the.*. pattern in the new implementation and replace 
// it with '.'. In addition, there is another case where the union is at the end of the path, where the pattern
// is.*, in which case.* is removed.
std::string AvroScanner::preprocess_jsonpaths(std::string jsonpaths) {
    replaceAll(jsonpaths, ".*.", ".");
    replaceAll(jsonpaths, ".*", "");
    return jsonpaths;
}

// If the user does not specify jsonpaths, we need to generate a corresponding jsonpath 
// based on the table name. The overall strategy is to use _ as the separator for each path. 
// As an example, suppose a table has three columns, each named:
// a_b_c | d_e | f
// So the jsonpaths we generate are:
// $.a.b.c
// $.d.e
// $.f
std::string AvroScanner::generate_jsonpaths(std::vector<std::string>& col_names) {
    std::stringstream jsonpaths_stream;
    jsonpaths_stream << "[";
    size_t col_size = col_names.size();
    for (size_t i = 0; i < col_size; i++) {
        const std::string& col_name = col_names[i];
        jsonpaths_stream << "\"$";
        std::vector<std::string> paths;
        boost::split(paths, col_name, boost::is_any_of("_"));
        for (int i=0; i<paths.size(); i++) {
            jsonpaths_stream << ".";
            jsonpaths_stream << paths[i];
        }
        jsonpaths_stream << "\"";
        if (i != col_size - 1) {
            jsonpaths_stream << ",";
        }
    }
    jsonpaths_stream << "]";
    return jsonpaths_stream.str();
}

Status AvroScanner::open() {
#if BE_TEST
    const TBrokerRangeDesc& range_desc = _scan_range.ranges[0];
    if (avro_file_reader(range_desc.path.c_str(), &_dbreader)) {
        auto err_msg = "Error opening file: " + std::string(avro_strerror());
        return Status::InternalError(err_msg);        
    }
#endif

    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }
#ifndef BE_TEST
    if (_serdes == nullptr) {
        std::string confluent_schema_registry_url;
        if (!_scan_range.params.__isset.confluent_schema_registry_url) {
            return Status::InternalError("'confluent_schema_registry_url' not set");
        } else {
            confluent_schema_registry_url = _scan_range.params.confluent_schema_registry_url;
        }

        serdes_conf_t* sconf = serdes_conf_new(NULL, 0, "schema.registry.url",
                                            confluent_schema_registry_url.c_str(), NULL);
        _serdes = serdes_new(sconf, _err_buf, sizeof(_err_buf));
        if (!_serdes) {
            LOG(ERROR) << "failed to create serdes handle: " << _err_buf;
            return Status::InternalError("failed to create serdes handle");
        }
    }
#endif
    const TBrokerRangeDesc& range = _scan_range.ranges[0];
    if (range.__isset.jsonpaths) {
        std::string jsonpaths = preprocess_jsonpaths(range.jsonpaths);
        RETURN_IF_ERROR(JsonScanner::parse_json_paths(jsonpaths, &_json_paths));
    } else {
        std::vector<std::string> col_names;
        for (int i=0; i<_src_slot_descriptors.size(); i++) {
            col_names.push_back(_src_slot_descriptors[i]->col_name());
        }
        std::string jsonpaths = generate_jsonpaths(col_names);
        RETURN_IF_ERROR(JsonScanner::parse_json_paths(jsonpaths, &_json_paths));
    }
    return Status::OK();
}

void AvroScanner::_materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        AdaptiveNullableColumn* adaptive_column =
                down_cast<AdaptiveNullableColumn*>(chunk->get_column_by_index(i).get());
        chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                             adaptive_column->materialized_raw_null_column()),
                                      i);
    }
}

Status AvroScanner::_create_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    *chunk = std::make_shared<Chunk>();
    size_t slot_size = _src_slot_descriptors.size();
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        auto column = ColumnHelper::create_column(slot_desc->type(), true, false, 0, true);
        (*chunk)->append_column(column, slot_desc->id());
    }

    return Status::OK();
}

void AvroScanner::_report_error(const std::string& line, const std::string& err_msg) {
    _state->append_error_msg_to_file(line, err_msg);
}

Status AvroScanner::_construct_row(avro_value_t avro_value, Chunk* chunk) {
    size_t slot_size = _src_slot_descriptors.size();
    size_t jsonpath_size = _json_paths.size();
    for (size_t i = 0; i < slot_size; i++) {
        if (_src_slot_descriptors[i] == nullptr) {
            continue;
        }
        auto column = down_cast<NullableColumn*>(chunk->get_column_by_slot_id(_src_slot_descriptors[i]->id()).get());
        if (i >= jsonpath_size) {
            column->append_nulls(1);
            continue;
        }
        avro_value_t output_value;
        auto st = _extract_field(avro_value, _json_paths[i], output_value);
        if (st.ok()) {
            RETURN_IF_ERROR(_construct_column(output_value, column, _src_slot_descriptors[i]->type(), _src_slot_descriptors[i]->col_name()));
        } else if (st.is_not_found()) {
            column->append_nulls(1);
        } else {
            return st;
        }
        
    }
    return Status::OK();
}

Status AvroScanner::_parse_avro(Chunk* chunk, std::shared_ptr<SequentialFile> file) {
    const int capacity = _state->chunk_size();
    DCHECK_EQ(0, chunk->num_rows());
    for (size_t num_rows = chunk->num_rows(); num_rows < capacity; /**/) {
        avro_value_t avro_value;
#ifdef BE_TEST
        // In general, we want to test component injection schemastr.
        avro_schema_error_t error;
        avro_schema_t schema = NULL;
        int result = avro_schema_from_json(_schema_text.c_str(), _schema_text.size(), &schema, &error);
        if (result != 0) {
            auto err_msg = "parse schema from json error: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        avro_value_iface_t* iface = avro_generic_class_from_schema(schema);
        if (avro_generic_value_new(iface, &avro_value)) {
            auto err_msg = "Cannot allocate new value instance: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        DeferOp avro_deleter([&] {
            avro_schema_decref(schema);
            avro_value_iface_decref(iface);
            avro_value_decref(&avro_value);
        });
        result = avro_file_reader_read_value(_dbreader, &avro_value);
        if (result != 0) {
            auto err_msg = "read avro value error: " + std::string(avro_strerror());
            return Status::EndOfFile(err_msg);
        }

        char* avro_as_json = nullptr;
        result = avro_value_to_json(&avro_value, 1, &avro_as_json);
        if (result != 0) {
            auto err_msg = "Unable to read value: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);        
        }
        free(avro_as_json);
#else
        const uint8_t* data{};
        size_t length = 0;
        auto* stream_file = down_cast<StreamLoadPipeInputStream*>(file->stream().get());
        {
            SCOPED_RAW_TIMER(&_counter->file_read_ns);
            ASSIGN_OR_RETURN(_parser_buf, stream_file->pipe()->read());
        }
        data = reinterpret_cast<uint8_t*>(_parser_buf->ptr);
        length = _parser_buf->remaining();
        DeferOp op([&] { avro_value_decref(&avro_value); });
        serdes_schema_t* schema;
        serdes_err_t err = serdes_deserialize_avro(_serdes, &avro_value, &schema, data, length, _err_buf, sizeof(_err_buf));
        if (err) {
            LOG(ERROR) << "serdes deserialize avro failed: " << _err_buf;
            return Status::InternalError("serdes deserialize avro failed");
        }
#endif
        RETURN_IF_ERROR(_construct_row(avro_value, chunk));
    }
    return Status::OK();
}


StatusOr<ChunkPtr> AvroScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    if (_scan_range.ranges.size() == 0) {
        return Status::EndOfFile("EOF of reading protobuf file");
    }
    std::shared_ptr<SequentialFile> file;
    const TBrokerRangeDesc& range_desc = _scan_range.ranges[0];
    Status st = create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &file);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create sequential files: " << st.to_string();
        return st;
    }

    ChunkPtr src_chunk;
    RETURN_IF_ERROR(_create_src_chunk(&src_chunk));
    const int chunk_capacity = _state->chunk_size();
    src_chunk->reserve(chunk_capacity);
    src_chunk->set_num_rows(0);
    st = _parse_avro(src_chunk.get(), file);
    if (!st.ok()) {
        if (!st.is_end_of_file()) {
            return st;
        }
    }
    _materialize_src_chunk_adaptive_nullable_column(src_chunk);
    return materialize(nullptr, src_chunk);
}

void AvroScanner::close() {
    FileScanner::close();
}

Status AvroScanner::_get_array_element(avro_value_t* cur_value, size_t idx, avro_value_t* element) {
    size_t element_count;
    if (avro_value_get_size(cur_value, &element_count) != 0) {
        auto err_msg = "Cannot get avro array size: " + std::string(avro_strerror());
        return Status::InternalError(err_msg);   
    }
    if (idx >= element_count) {
        auto err_msg = "array idx greater than the avro array max size";
        return Status::InternalError(err_msg);   
    }
    if (avro_value_get_by_index(cur_value, idx, element, nullptr) != 0) {
        auto err_msg = "Cannot get avro value from array: " + std::string(avro_strerror());
        return Status::InternalError(err_msg);   
    }
    return Status::OK();
}

bool construct_path_from_str(std::string path_str, std::vector<AvroPath>& paths) {
    return false;
}

// This function handles the union data type, and the reason for using loops is that there is a nested case:
//    [
//         null,
//         [
//             null,
//             [
//                 null,
//                 "string"
//             ]
//         ]
//     ]
// The whole logic is to return the deepest data, which could be null or some other data type.
Status AvroScanner::_handle_union(avro_value_t input_value, avro_value_t& branch) {
    while (avro_value_get_type(&input_value) == AVRO_UNION) {
        int disc;
        if (avro_value_get_discriminant(&input_value, &disc) != 0) {
            auto err_msg = "Cannot get union discriminan: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        if (avro_value_set_branch(&input_value, disc, &branch) != 0) {
            auto err_msg = "Cannot set union branch: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        if (avro_value_get_type(&branch) == AVRO_NULL) {
            return Status::OK();
        }
        input_value = branch;
    }
    return Status::OK();
}

// This function extracts the corresponding field data from avro data according to 
// the corresponding path of each column, where:
// input_value: input param
// paths: input param
// output_value: output param
Status AvroScanner::_extract_field(avro_value_t& input_value, std::vector<AvroPath> paths, avro_value_t& output_value) {
    avro_value_t cur_value = input_value;

    // Select the entire data
    if (paths.size() == 1 && paths[0].key == "$" && paths[0].idx == -1) {
        // Remove union
        if (avro_value_get_type(&cur_value) == AVRO_UNION) {
            avro_value_t branch;
            RETURN_IF_ERROR(_handle_union(cur_value, branch));
            cur_value = branch;
        }
        output_value = cur_value;
        return Status::OK();
    }

    // The starting point for our progression is an array, and we cannot use avro_value_get_by_name 
    // to get the value of the next field. We want the path pattern to look like this:
    // {
    //     key == "$"
    //     idx = 0,1,2,3....
    // } 
    if (avro_value_get_type(&cur_value) == AVRO_ARRAY) {
        if (paths[0].key != "$" || paths[0].idx < 0) {
            auto err_msg = "The avro root type is an array, and you should select a specific array element.";
            return Status::InternalError(err_msg);
        }
        avro_value_t element;
        RETURN_IF_ERROR(_get_array_element(&cur_value, paths[0].idx, &element));
        cur_value = element;
    }

    // paths[0] should be $, skip it
    for (int i = 1; i < paths.size(); i++) {
        // The union type is used to provide nullable semantics. For scenarios where AVRO imports, 
        // AVRO's union type can only be of this pattern:
        // {
        //      null,
        //      Other Type
        // }
        // For each iteration, we first determine if the current avro type is union. If it is union, 
        // we continue to determine if it is null. If so, we stop the progression and return.
        // If it is not null, the corresponding value is extracted and the progress continues.
        if (avro_value_get_type(&cur_value) == AVRO_UNION) {
            avro_value_t branch;
            RETURN_IF_ERROR(_handle_union(cur_value, branch));
            cur_value = branch;
            if (avro_value_get_type(&branch) == AVRO_NULL) {
                output_value = cur_value;
                return Status::OK();
            }
        }

        if (avro_value_get_type(&cur_value) != AVRO_RECORD) {
            if (i == paths.size() - 1) {
                break;
            } else {
                auto err_msg = "A non-record type was found during avro parsing. Please check the path you specified";
                return Status::InternalError(err_msg);
            }
        }
        avro_value_t next_value;
        if (avro_value_get_by_name(&cur_value, paths[i].key.c_str(), &next_value, nullptr) == 0) {
            // For each path, we first need to determine whether the path has an array element operation
            cur_value = next_value;
            if (paths[i].idx != -1) {
                // In this case, you need to remove the union:
                // $.event_params[2]
                // {
                //   "name": "event_params",
                //   "type": ["null", {
                //      "items": "string",
                //      "type": "array"
                //    }]
                // } 
                if (avro_value_get_type(&cur_value) == AVRO_UNION) {
                    avro_value_t branch;
                    RETURN_IF_ERROR(_handle_union(cur_value, branch));
                    cur_value = branch;
                    if (avro_value_get_type(&branch) == AVRO_NULL) {
                        output_value = cur_value;
                        return Status::OK();
                    }
                }
                // cur_value must be an array type, otherwise it is invalid
                if (avro_value_get_type(&cur_value) != AVRO_ARRAY) {
                    auto err_msg = "A non-array type was found during avro parsing. Please check the path you specified";
                    return Status::InternalError(err_msg);
                }
                avro_value_t element;
                RETURN_IF_ERROR(_get_array_element(&cur_value, paths[0].idx, &element));
                cur_value = element;
            }
        } else {
            // If no field can be found, end the parsing of the row and return not found.
            auto msg = strings::Substitute("Cannot get field: $0. err msg: $1.", paths[i].key, avro_strerror());
            return Status::NotFound(msg);                
        }
    }
    // Remove union
    if (avro_value_get_type(&cur_value) == AVRO_UNION) {
        avro_value_t branch;
        RETURN_IF_ERROR(_handle_union(cur_value, branch));
        cur_value = branch;
    }
    output_value = cur_value;
    return Status::OK();
}

Status AvroScanner::_construct_column(avro_value_t input_value, Column* column, const TypeDescriptor& type_desc,
                                     const std::string& col_name) {
    return add_adaptive_nullable_column(column, type_desc, col_name, input_value, !_strict_mode);
}

}