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

#include "exec/protobuf_scanner.h"

#include <fmt/format.h>
#include <ryu/ryu.h>

#include <algorithm>
#include <memory>
#include <sstream>
#include <utility>
#include "exec/protobuf_to_starrocks_converter.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/json_parser.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/json_functions.h"
#include "formats/json/nullable_column.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/compiler/parser.h>

namespace starrocks {

using PBArrayInputStream = google::protobuf::io::ArrayInputStream;
using PBTokenizer = google::protobuf::io::Tokenizer;
using PBFileDescriptorProto = google::protobuf::FileDescriptorProto;
using PBParser = google::protobuf::compiler::Parser;
using PBDescriptorPool = google::protobuf::DescriptorPool;
using PBFileDescriptor = google::protobuf::FileDescriptor;
using PBDescriptor = google::protobuf::Descriptor;
using PBDynamicMessageFactory = google::protobuf::DynamicMessageFactory;
using PBMessage = google::protobuf::Message;
using PBReflection = google::protobuf::Reflection;
using PBFieldDescriptor = google::protobuf::FieldDescriptor;
using PBEnumValueDescriptor = google::protobuf::EnumValueDescriptor;

// ProtobufScanner::ProtobufScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
//                          ScannerCounter* counter, serdes_t *serdes, const std::string message_type)
//         : FileScanner(state, profile, scan_range.params, counter),
//           _scan_range(scan_range),
//           _serdes(serdes),
//           _message_type(message_type) {}

ProtobufScanner::ProtobufScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _serdes(nullptr),
          _message_type("") {
#if BE_TEST
    raw::RawVector<char> buf(_buf_size);
    std::swap(buf, _buf);
#endif
}

ProtobufScanner::ProtobufScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, const std::string schema_text, const std::string message_type)
        : FileScanner(state, profile, scan_range.params, counter),
            _scan_range(scan_range),
            _serdes(nullptr),
            _schema_text(schema_text),
            _message_type(message_type) {
#if BE_TEST
    raw::RawVector<char> buf(_buf_size);
    std::swap(buf, _buf);
#endif
}

ProtobufScanner::~ProtobufScanner() {
    if (_serdes != nullptr) {
        free(_serdes);
    }
}

Status ProtobufScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }
#ifndef BE_TEST
    // 没有初始化serdes，从_scan_range取数据并做初始化
    if (_serdes == nullptr || _message_type.size() == 0) {
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

        if (!_scan_range.params.__isset.pb_message_type) {
            return Status::InternalError("'pb_message_type' not set");
        } else {
            _message_type = _scan_range.params.pb_message_type;
        }
    }
#endif
    return Status::OK();
}

/*
 * 问题和分析：
 * 我们需要知道get_next的使用上下文，它的接口定义是什么。按照当前的理解就是读二进制流，然后解析出来，解析的结果放在chunk里
 * 这里的chunk应该是一个二维矩阵，包含了每个列的定义。而且，我们最重要和最终的问题是要实现这个逻辑。
 * 我们需要看下是如何从队列里拿到数据的，这个是源头
 * 其次，我们需要拿到schema
 * 再者，我们有了二进制流+schema，我们需要解析数据。那么pb是如何解析数据的。
 * 拿到解析的数据后，我们需要做类型转换，并且写入到chunk里。这块还是不够清晰，比如你怎么写到chunk，怎么做类型转换，参考CSV/json
 * 还有protobuf支持schema引入，这块如何解。
 * 还有一个非常大的问题，List和Map当前没有支持
 * 
 * 
 * csv和json的完整流程是不是这样的
 * 1. 第一步需要create chunk，作为此次get_next获取到的结果的容器
 * 2. 数据如何塞到第一步创建的chunk里
 *    对于pb的基本类型，pb解析完成后就转换为C++的基本类型了，此时可以使用colunm->append，直接append数据。我们需要以SR表字段为依据
 *    搜索pb对应的field，然后做相应处理。参考_construct_row_without_jsonpath
 * 3. 然后填充完数据的chunk，如何向下传播
 *    目前来看，如果没有特殊的语义，可以调用materialize方法完成数据的物化
 * 
 * 
 * 看了下pb的基本类型，这个和C++的基本类型是一致的，那就说pb的基本类型可以和SR表的类型一一对应。所以type这块的处理可以参考csv的做法.
 * 1. 在get_next创建src chunk时使用sr定义的src_slot
 * 2. 定义一个表，可以从这个表里获取每个字段对应的pb解析函数，完成数据解析
 * 但是这里还有一个很棘手且难以解决的问题，pb的非标量如何处理和特殊属性的字段如何处理。另外，还有一个问题，如果我们希望按照SR table字段的
 * 名称去定位pb流的字段，pb的api支持getFieldByName的方式获取数据吗。
 * 
 * optinal字段：这种类型的字段可有可无。proto2在schema里定义了默认值。我想pb的api应该有办法获取Optinal字段的默认值。
 * 
 * 
 * 考虑下开发流程。实现probobuf_scanner以及对应的测试case。
 * 有一些问题需要澄清，pb动态解析的api 解析出来的pb数据和sr表的数据类型做转换
 * 架构设计，比如和schema registry的交互如何安排。
 * 
 * 
 * pb动态解析API，目前的现状是对这个过程不够熟悉，这样的话你写代码就不好写，我们得先把这个模块设计出来。
 * 我们的需求：
 * 1. 提供二进制流和schema字符串定义
 * 2. 提供变量的名称，解析出数据
 * 
 * 架构设计：
 * 1. 构造函数需要获取到schema registry的句柄
 * 
 * 我们梳理下当前的进展：
 * 1. 除了pbscanner之外的代码都还没有写，想一想这块还有哪些东西要做
 *    a. fe和be的参数传递，这个比较简单
 *    b. 还有be的调用pbscanner的流程，这个应该也比较简单
 * 2. pbscanner的主流程写完了，但是没有做测试
 * 3. list和map没有支持
 */

Status ProtobufScanner::_create_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    *chunk = std::make_shared<Chunk>();
    size_t slot_size = _src_slot_descriptors.size();
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        auto column = ColumnHelper::create_column(slot_desc->type(), true);
        (*chunk)->append_column(column, slot_desc->id());
    }

    return Status::OK();
}

void ProtobufScanner::_report_error(const std::string& line, const std::string& err_msg) {
    _state->append_error_msg_to_file(line, err_msg);
}

Status ProtobufScanner::_parse_protobuf(Chunk* chunk, std::shared_ptr<SequentialFile> file) {
    const int capacity = _state->chunk_size();
    DCHECK_EQ(0, chunk->num_rows());
    int num_columns = chunk->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get_column_by_index(i).get();
    }
    for (size_t num_rows = chunk->num_rows(); num_rows < capacity; /**/) {
        // Step1: fetch one protobuf message 
        const uint8_t* data{};
        size_t length = 0;
#ifdef BE_TEST

        [[maybe_unused]] size_t message_size = 0;
        ASSIGN_OR_RETURN(auto nread, file->read(_buf.data(), _buf_size));
        if (nread == 0) {
            return Status::EndOfFile("EOF of reading file");
        }

        data = reinterpret_cast<uint8_t*>(_buf.data());
        length = nread;

#else
        auto* stream_file = down_cast<StreamLoadPipeInputStream*>(file->stream().get());
        {
            SCOPED_RAW_TIMER(&_counter->file_read_ns);
            ASSIGN_OR_RETURN(_parser_buf, stream_file->pipe()->read());
        }
        data = reinterpret_cast<uint8_t*>(_parser_buf->ptr);
        length = _parser_buf->remaining();
#endif
        Slice record(data, length);
        const char* schema_text = nullptr;
        if (_schema_text.size() == 0) {
            // fetch message schema ID
            uint32_t schema_id = get_schema_id(_serdes, &data, &length, _err_buf, sizeof(_err_buf));
            if (schema_id == -1) {
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << strings::Substitute("Get schema id error $0.", std::string(_err_buf));
                    _report_error(record.to_string(), error_msg.str());
                }
                continue;
            }

            // fetch message schema
            serdes_schema_t *schema = serdes_schema_get(_serdes, NULL, schema_id, _err_buf, sizeof(_err_buf));
            if (!schema) {
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << strings::Substitute("Failed to get schema: $0, schema_id is $1.", std::string(_err_buf), std::to_string(schema_id));
                    _report_error(record.to_string(), error_msg.str());
                }
                continue;
            }
            schema_text = (const char*) serdes_schema_object(schema);
            if (!schema_text) {
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << strings::Substitute("Failed to get schema object. schema_id is $0.", std::string(_err_buf));
                    _report_error(record.to_string(), error_msg.str());
                }
                continue;
            }
        } else {
            schema_text = _schema_text.c_str();
        }

        // parse pb
        PBArrayInputStream raw_input(schema_text, strlen(schema_text));
        PBTokenizer input(&raw_input, NULL);
        PBFileDescriptorProto file_desc_proto;
        PBParser parser;
        if (!parser.Parse(&input, &file_desc_proto)) {
            std::stringstream error_msg;
            error_msg << "Failed to parse .proto definition:" << schema_text;
            _report_error(record.to_string(), error_msg.str());
            continue;
        }

        if (!file_desc_proto.has_name()) {
            file_desc_proto.set_name(_message_type);
        }
        
        PBDescriptorPool pool;
        const PBFileDescriptor* file_desc =
        pool.BuildFile(file_desc_proto);
        if (file_desc == NULL) {
            std::stringstream error_msg;
            error_msg << "Cannot get file descriptor from file descriptor proto: " << file_desc_proto.DebugString();
            _report_error(record.to_string(), error_msg.str());
            continue;
        }

        const PBDescriptor* message_desc = file_desc->FindMessageTypeByName(_message_type);
        if (message_desc == NULL) {
            std::stringstream error_msg;
            error_msg << "Cannot get message descriptor of message: " << file_desc->DebugString();
            _report_error(record.to_string(), error_msg.str());
            continue;
        }

        PBDynamicMessageFactory factory;
        const PBMessage* prototype_msg =
            factory.GetPrototype(message_desc);
        if (prototype_msg == NULL) {
            std::stringstream error_msg;
            error_msg << "Cannot create prototype message from message descriptor";
            _report_error(record.to_string(), error_msg.str());
            continue;
        }

        PBMessage* mutable_msg = prototype_msg->New();
        if (mutable_msg == NULL) {
            std::stringstream error_msg;
            error_msg << "Failed in prototype_msg->New(); to create mutable message";
            _report_error(record.to_string(), error_msg.str());
            continue;
        }

        if (!mutable_msg->ParseFromArray(data, length)) {
            std::stringstream error_msg;
            error_msg << "Failed to parse value in buffer";
            _report_error(record.to_string(), error_msg.str());
            continue;
        }

        const PBReflection* reflection = mutable_msg->GetReflection();
        bool no_error = true;
        bool unsupport_convertion = false;
        for (int i = 0; i < _src_slot_descriptors.size(); i++) {
            auto slot = _src_slot_descriptors[i];
            if (slot == nullptr) {
                continue;
            }
            Column* cur_column = _column_raw_ptrs[i];
            std::stringstream error_msg;
            // 整体的思路是这样的，如果在pb中没有找到对应的field，如果field是nullable的，那么对这个字段appendnull
            // 如果不是nullable的，那么返回error
            const PBFieldDescriptor* field = message_desc->FindFieldByName(slot->col_name());
            if (field == nullptr) {
                if (!slot->is_nullable()) {
                    error_msg << "Dont find " << slot->col_name() << " column in pb message.";
                    return Status::InternalError(error_msg.str());
                }
                auto* nullable = down_cast<NullableColumn*>(cur_column);
                if (!nullable->append_nulls(1)) {
                    error_msg << "Dont find " << slot->col_name() << " column in pb message, and append null error.";
                    _report_error(record.to_string(), error_msg.str());
                    chunk->set_num_rows(num_rows);
                    break;  
                } else {
                    continue;
                }    
            }

            switch (field->cpp_type()) {
            case PBFieldDescriptor::CppType::CPPTYPE_INT32:
                {
                    int32_t value = reflection->GetInt32(*mutable_msg, field);
                    switch (slot->type().type)
                    {
                    case LogicalType::TYPE_TINYINT:
                        no_error = integer_to_integer_pb_convert<int32_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_TINYINT:
                        no_error = integer_to_integer_pb_convert<int32_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_SMALLINT:
                        no_error = integer_to_integer_pb_convert<int32_t, int16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_SMALLINT:
                        no_error = integer_to_integer_pb_convert<int32_t, uint16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_INT:
                        no_error = append_pb_convert<int32_t, int32_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_UNSIGNED_INT:
                        no_error = integer_to_integer_pb_convert<int32_t, uint32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BIGINT:
                        no_error = append_pb_convert<int32_t, int64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_UNSIGNED_BIGINT:
                        no_error = append_pb_convert<int32_t, uint64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_LARGEINT:
                        no_error = append_pb_convert<int32_t, int128_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_FLOAT:
                        no_error = append_pb_convert<int32_t, float>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DOUBLE:
                        no_error = append_pb_convert<int32_t, double>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_CHAR:
                        no_error = integer_to_integer_pb_convert<int32_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BOOLEAN:
                        no_error = integer_to_integer_pb_convert<int32_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_DECIMAL32:
                        // TODO: decimal_scale 
                        no_error = decimalv3_pb_convert<int32_t, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL64:
                        // TODO: decimal_scale 
                        no_error = decimalv3_pb_convert<int32_t, int64_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL128:
                        // TODO: decimal_scale 
                        no_error = decimalv3_pb_convert<int32_t, int128_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMALV2:
                        no_error = decimalv2_pb_convert<int32_t, DecimalV2Value>(cur_column, value, slot->is_nullable());
                        break;                         
                    default:
                        no_error = false;
                        unsupport_convertion = true;
                        break;
                    }
                    if (unsupport_convertion) {
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                    } else if (!no_error) {
                        error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_INT64:
                {
                    int64_t value = reflection->GetInt64(*mutable_msg, field);
                    switch (slot->type().type)
                    {
                    case LogicalType::TYPE_TINYINT:
                        no_error = integer_to_integer_pb_convert<int64_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_TINYINT:
                        no_error = integer_to_integer_pb_convert<int64_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_SMALLINT:
                        no_error = integer_to_integer_pb_convert<int64_t, int16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_SMALLINT:
                        no_error = integer_to_integer_pb_convert<int64_t, uint16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_INT:
                        no_error = integer_to_integer_pb_convert<int64_t, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_INT:
                        no_error = integer_to_integer_pb_convert<int64_t, uint32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BIGINT:
                        no_error = append_pb_convert<int64_t, int64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_UNSIGNED_BIGINT:
                        no_error = append_pb_convert<int64_t, uint64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_LARGEINT:
                        no_error = append_pb_convert<int64_t, int128_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_FLOAT:
                        no_error = append_pb_convert<int64_t, float>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DOUBLE:
                        no_error = append_pb_convert<int64_t, double>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_CHAR:
                        no_error = integer_to_integer_pb_convert<int64_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BOOLEAN:
                        no_error = integer_to_integer_pb_convert<int64_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_DECIMAL32:
                        // TODO: decimal_scale 
                        no_error = decimalv3_pb_convert<int64_t, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL64:
                        // TODO: decimal_scale 
                        no_error = decimalv3_pb_convert<int64_t, int64_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL128:
                        // TODO: decimal_scale 
                        no_error = decimalv3_pb_convert<int64_t, int128_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMALV2:
                        no_error = decimalv2_pb_convert<int64_t, DecimalV2Value>(cur_column, value, slot->is_nullable());
                        break;                         
                    default:
                        no_error = false;
                        unsupport_convertion = true;
                        break;
                    }
                    if (unsupport_convertion) {
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                    } else if (!no_error) {
                        error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_UINT32:
                {
                    uint32_t value = reflection->GetUInt32(*mutable_msg, field);
                    switch (slot->type().type)
                    {
                    case LogicalType::TYPE_TINYINT:
                        no_error = integer_to_integer_pb_convert<uint32_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_TINYINT:
                        no_error = integer_to_integer_pb_convert<uint32_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_SMALLINT:
                        no_error = integer_to_integer_pb_convert<uint32_t, int16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_SMALLINT:
                        no_error = integer_to_integer_pb_convert<uint32_t, uint16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_INT:
                        no_error = integer_to_integer_pb_convert<uint32_t, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_INT:
                        no_error = append_pb_convert<uint32_t, uint32_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_BIGINT:
                        no_error = append_pb_convert<uint32_t, int64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_UNSIGNED_BIGINT:
                        no_error = append_pb_convert<uint32_t, uint64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_LARGEINT:
                        no_error = append_pb_convert<uint32_t, int128_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_FLOAT:
                        no_error = append_pb_convert<uint32_t, float>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DOUBLE:
                        no_error = append_pb_convert<uint32_t, double>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_CHAR:
                        no_error = integer_to_integer_pb_convert<uint32_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BOOLEAN:
                        no_error = integer_to_integer_pb_convert<uint32_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_DECIMAL32:
                        {
                            int64_t scale_value = value;
                            no_error = decimalv3_pb_convert<int64_t, int32_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMAL64:
                        {
                            int64_t scale_value = value;
                            no_error = decimalv3_pb_convert<int64_t, int64_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMAL128:
                        {
                            int64_t scale_value = value;
                            no_error = decimalv3_pb_convert<int64_t, int128_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMALV2:
                        no_error = decimalv2_pb_convert<uint32_t, DecimalV2Value>(cur_column, value, slot->is_nullable());
                        break;                         
                    default:
                        no_error = false;
                        unsupport_convertion = true;
                        break;
                    }
                    if (unsupport_convertion) {
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                    } else if (!no_error) {
                        error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_UINT64:
                {
                    uint64_t value = reflection->GetUInt64(*mutable_msg, field);
                    switch (slot->type().type)
                    {
                    case LogicalType::TYPE_TINYINT:
                        no_error = integer_to_integer_pb_convert<uint64_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_TINYINT:
                        no_error = integer_to_integer_pb_convert<uint64_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_SMALLINT:
                        no_error = integer_to_integer_pb_convert<uint64_t, int16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_SMALLINT:
                        no_error = integer_to_integer_pb_convert<uint64_t, uint16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_INT:
                        no_error = integer_to_integer_pb_convert<uint64_t, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_INT:
                        no_error = integer_to_integer_pb_convert<uint64_t, uint32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BIGINT:
                        no_error = integer_to_integer_pb_convert<uint64_t, int64_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_BIGINT:
                        no_error = append_pb_convert<uint64_t, uint64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_LARGEINT:
                        no_error = append_pb_convert<uint64_t, int128_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_FLOAT:
                        no_error = append_pb_convert<uint64_t, float>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DOUBLE:
                        no_error = append_pb_convert<uint64_t, double>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_CHAR:
                        no_error = integer_to_integer_pb_convert<uint64_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BOOLEAN:
                        no_error = integer_to_integer_pb_convert<uint64_t, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_DECIMAL32:
                        {
                            int128_t scale_value = value;
                            no_error = decimalv3_pb_convert<int128_t, int32_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMAL64:
                        {
                            int128_t scale_value = value;
                            no_error = decimalv3_pb_convert<int128_t, int64_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMAL128:
                        {
                            int128_t scale_value = value;
                            no_error = decimalv3_pb_convert<int128_t, int128_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMALV2:
                        no_error = decimalv2_pb_convert<uint64_t, DecimalV2Value>(cur_column, value, slot->is_nullable());
                        break;                         
                    default:
                        no_error = false;
                        unsupport_convertion = true;
                        break;
                    }
                    if (unsupport_convertion) {
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                    } else if (!no_error) {
                        error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }                
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_DOUBLE:
                {
                    double value = reflection->GetDouble(*mutable_msg, field);
                    switch (slot->type().type)
                    {
                    case LogicalType::TYPE_TINYINT:
                        no_error = float_to_integer_pb_convert<double, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_TINYINT:
                        no_error = float_to_integer_pb_convert<double, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_SMALLINT:
                        no_error = float_to_integer_pb_convert<double, int16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_SMALLINT:
                        no_error = float_to_integer_pb_convert<double, uint16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_INT:
                        no_error = float_to_integer_pb_convert<double, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_INT:
                        no_error = float_to_integer_pb_convert<double, uint32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BIGINT:
                        no_error = float_to_integer_pb_convert<double, int64_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_BIGINT:
                        no_error = float_to_integer_pb_convert<double, uint64_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_LARGEINT:
                        no_error = float_to_integer_pb_convert<double, int128_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_FLOAT:
                        no_error = append_pb_convert<double, float>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DOUBLE:
                        no_error = append_pb_convert<double, double>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_CHAR:
                        no_error = float_to_integer_pb_convert<double, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BOOLEAN:
                        no_error = float_to_integer_pb_convert<double, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_DECIMAL32:
                        // TODO: decimal_scale 
                        no_error = decimalv3_float_double_pb_convert<double, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL64:
                        // TODO: decimal_scale 
                        no_error = decimalv3_float_double_pb_convert<double, int64_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL128:
                        // TODO: decimal_scale 
                        no_error = decimalv3_float_double_pb_convert<double, int128_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMALV2:
                        no_error = decimalv2_pb_convert<double, DecimalV2Value>(cur_column, value, slot->is_nullable());
                        break;                         
                    default:
                        no_error = false;
                        unsupport_convertion = true;
                        break;
                    }
                    if (unsupport_convertion) {
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                    } else if (!no_error) {
                        error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }                     
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_FLOAT:
                {
                    float value = reflection->GetFloat(*mutable_msg, field);
                    switch (slot->type().type)
                    {
                    case LogicalType::TYPE_TINYINT:
                        no_error = float_to_integer_pb_convert<float, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_TINYINT:
                        no_error = float_to_integer_pb_convert<float, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_SMALLINT:
                        no_error = float_to_integer_pb_convert<float, int16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_SMALLINT:
                        no_error = float_to_integer_pb_convert<float, uint16_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_INT:
                        no_error = float_to_integer_pb_convert<float, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_INT:
                        no_error = float_to_integer_pb_convert<float, uint32_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BIGINT:
                        no_error = float_to_integer_pb_convert<float, int64_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_BIGINT:
                        no_error = float_to_integer_pb_convert<float, uint64_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_LARGEINT:
                        no_error = float_to_integer_pb_convert<float, int128_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_FLOAT:
                        no_error = append_pb_convert<float, float>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DOUBLE:
                        no_error = append_pb_convert<float, double>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_CHAR:
                        no_error = float_to_integer_pb_convert<float, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BOOLEAN:
                        no_error = float_to_integer_pb_convert<float, uint8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_DECIMAL32:
                        // TODO: decimal_scale 
                        no_error = decimalv3_float_double_pb_convert<float, int32_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL64:
                        // TODO: decimal_scale 
                        no_error = decimalv3_float_double_pb_convert<float, int64_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMAL128:
                        // TODO: decimal_scale 
                        no_error = decimalv3_float_double_pb_convert<float, int128_t>(cur_column, value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        break;
                    case LogicalType::TYPE_DECIMALV2:
                        no_error = decimalv2_pb_convert<float, DecimalV2Value>(cur_column, value, slot->is_nullable());
                        break;                         
                    default:
                        no_error = false;
                        unsupport_convertion = true;
                        break;
                    }
                    if (unsupport_convertion) {
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                    } else if (!no_error) {
                        error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }               
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_BOOL:
                {
                    bool value = reflection->GetBool(*mutable_msg, field);
                    switch (slot->type().type)
                    {
                    case LogicalType::TYPE_TINYINT:
                        no_error = integer_to_integer_pb_convert<uint8_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_UNSIGNED_TINYINT:
                        no_error = append_pb_convert<uint8_t, uint8_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_SMALLINT:
                        no_error = append_pb_convert<uint8_t, int16_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_UNSIGNED_SMALLINT:
                        no_error = append_pb_convert<uint8_t, uint16_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_INT:
                        no_error = append_pb_convert<uint8_t, int32_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_UNSIGNED_INT:
                        no_error = append_pb_convert<uint8_t, uint32_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_BIGINT:
                        no_error = append_pb_convert<uint8_t, int64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_UNSIGNED_BIGINT:
                        no_error = append_pb_convert<uint8_t, uint64_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_LARGEINT:
                        no_error = append_pb_convert<uint8_t, int128_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_FLOAT:
                        no_error = append_pb_convert<uint8_t, float>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DOUBLE:
                        no_error = append_pb_convert<uint8_t, double>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_CHAR:
                        no_error = integer_to_integer_pb_convert<uint8_t, int8_t>(cur_column, value, slot->is_nullable(), _strict_mode);
                        break;
                    case LogicalType::TYPE_BOOLEAN:
                        no_error = append_pb_convert<uint8_t, uint8_t>(cur_column, value, slot->is_nullable());
                        break;
                    case LogicalType::TYPE_DECIMAL32:
                        {
                            int32_t scale_value = value;
                            no_error = decimalv3_pb_convert<int32_t, int32_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMAL64:
                        {
                            int32_t scale_value = value;
                            no_error = decimalv3_pb_convert<int32_t, int64_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMAL128:
                        {
                            int32_t scale_value = value;
                            no_error = decimalv3_pb_convert<int32_t, int128_t>(cur_column, scale_value, slot->is_nullable(), _strict_mode, slot->type().scale);
                        }
                        break;
                    case LogicalType::TYPE_DECIMALV2:
                        no_error = decimalv2_pb_convert<uint8_t, DecimalV2Value>(cur_column, value, slot->is_nullable());
                        break;                         
                    default:
                        no_error = false;
                        unsupport_convertion = true;
                        break;
                    }
                    if (unsupport_convertion) {
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                    } else if (!no_error) {
                        error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }                
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_ENUM:
                {
                    const PBEnumValueDescriptor* num_descriptor = reflection->GetEnum(*mutable_msg, field);
                    std::string value = num_descriptor->name();
                    if (slot->type().type != LogicalType::TYPE_VARCHAR) {
                        no_error = false;
                        unsupport_convertion = true;
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                        break;
                    }
                    no_error = string_pb_convert(cur_column, value, slot->is_nullable(), _strict_mode, &slot->type());
                    if (!no_error) {
                        error_msg << "Value '" << value << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }               
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_STRING:
                {
                    std::string value = reflection->GetString(*mutable_msg, field);
                    if (slot->type().type != LogicalType::TYPE_VARCHAR) {
                        no_error = false;
                        unsupport_convertion = true;
                        error_msg << "Dont support " << field->type_name() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                    << "The column is '" << slot->col_name() << ".";
                        break;
                    }
                    no_error = string_pb_convert(cur_column, value, slot->is_nullable(), _strict_mode, &slot->type());
                    if (!no_error) {
                        error_msg << "Value '" << value << "' is out of range. "
                                    << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    }           
                }
                break;
            case PBFieldDescriptor::CppType::CPPTYPE_MESSAGE:
                no_error = false;
                error_msg << "Dont support Message type for protobuf data type.";
                break;
            default:
                no_error = false;
                error_msg << "Unknown protobuf data type.";
                break;
            }
            if (!no_error) {
                chunk->set_num_rows(num_rows);
                if (_counter->num_rows_filtered++ < 50) {
                    _report_error(record.to_string(), error_msg.str());
                }
                break;
            }
        } // end of for
        num_rows += no_error;
    }
    return Status::OK();
}

StatusOr<ChunkPtr> ProtobufScanner::get_next() {
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
    st = _parse_protobuf(src_chunk.get(), file);
    if (!st.ok()) {
        if (st.is_end_of_file()) {
            // do nothing
        } else if (!st.is_time_out()) {
            return st;
        }
    }
    return materialize(nullptr, src_chunk);
}

void ProtobufScanner::close() {
    FileScanner::close();
}

} // namespace starrocks
