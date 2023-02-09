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
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/compiler/parser.h>

namespace starrocks {

using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace google::protobuf::compiler;

ProtobufScanner::ProtobufScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter, serdes_t *serdes, const std::string message_type)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _serdes(serdes)
          _message_type(message_type) {
    std::call_once(_once_flag, init_pb_conver_map);
}

ProtobufScanner::ProtobufScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, const std::string schema_text, const std::string message_type)
        : FileScanner(state, profile, scan_range.params, counter),
            _scan_range(scan_range),
            _schema_text(schema_text),
            _message_type(message_type) {
    std::call_once(_once_flag, init_pb_conver_map);
}

ProtobufScanner::~ProtobufScanner() = default;

Status ProtobufScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }
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
        (*chunk)->append_column(col, slot_desc->id());
    }

    return Status::OK();
}

/*
 * 需要几个输入：
 * 1. consumer消费到的二进制流，参考Status JsonReader::_read_and_parse_json() {函数
 * 这个问题的核心是需要从consumer那里获取到唯一一个proto message的二进制流。
 * 2. serdes
 * 3. message_type
 * 这里面有一个问题，拿json举例，JsonScanner一次get_next会get多行数据，一次获取多行数据的意义
 * 是可以增强性能。所以现在的问题是，这个问题是现在考虑还是先简单写一个parse_probobuf, 我觉得先
 * 简单考虑，然后再增加这个逻辑，我们现在的首要问题是把流程走通。
 * 
 *  
 */
Status _parse_protobuf(Chunk* chunk) {
    const int capacity = _state->chunk_size();
    DCHECK_EQ(0, chunk->num_rows());
    int num_columns = chunk->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get_column_by_index(i).get();
    }
    for (size_t num_rows = chunk->num_rows(); num_rows < capacity; /**/) {
        // Step1: fetch one protobuf message 
        uint8_t* data{};
        size_t length = 0;
        auto* stream_file = down_cast<StreamLoadPipeInputStream*>(_file->stream().get());
        {
            SCOPED_RAW_TIMER(&_counter->file_read_ns);
            ASSIGN_OR_RETURN(_parser_buf, stream_file->pipe()->read());
        }
        data = reinterpret_cast<uint8_t*>(_parser_buf->ptr);
        length = _parser_buf->remaining();

        const char* schema_text = nullptr;
        if (_schema_text.size() == 0) {
            // Step2: fetch message schema ID
            uint32_t schema_id = get_schema_id(_serdes, &data, &length, _err_msg, sizeof(_err_msg));
            if (schema_id == -1) {
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << strings::Substitute("Get schema id error $0.", std::string(errstr));
                    _report_error(record.to_string(), error_msg.str());
                }
                continue;
            }

            // Step3: fetch message schema
            serdes_schema_t *schema = serdes_schema_get(_serdes, NULL, schema_id, _err_msg, sizeof(_err_msg));
            if (!schema) {
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << strings::Substitute("Failed to get schema: $0, schema_id is $1.", std::string(errstr), std::to_string(schema_id));
                    _report_error(record.to_string(), error_msg.str());
                }
                continue;
            }
            schema_text = (const char*) serdes_schema_object(schema);
            if (!schema_text) {
                if (_counter->num_rows_filtered++ < 50) {
                    std::stringstream error_msg;
                    error_msg << strings::Substitute("Failed to get schema object. schema_id is $0.", std::string(errstr));
                    _report_error(record.to_string(), error_msg.str());
                }
                continue;
            }
        } else {
            schema_text = _schema_text.c_str();
        }

        // Step4: parse pb
        google::protobuf::io::ArrayInputStream raw_input(schema_text, strlen(schema_text));
        google::protobuf::io::Tokenizer input(&raw_input, NULL);
        google::protobuf::FileDescriptorProto file_desc_proto;
        google::protobuf::compiler::Parser parser;
        if (!parser.Parse(&input, &file_desc_proto)) {
            std::cerr << "Failed to parse .proto definition:" << text;
            return -1;
        }

        if (!file_desc_proto.has_name()) {
            file_desc_proto.set_name(_message_type);
        }
        
        google::protobuf::DescriptorPool pool;
        const google::protobuf::FileDescriptor* file_desc =
        pool.BuildFile(file_desc_proto);
        if (file_desc == NULL) {
            return Status::InternalError(strings::Substitute("Cannot get file descriptor from file descriptor proto: $0", file_desc_proto.DebugString());
        }

        const google::protobuf::Descriptor* message_desc = file_desc->FindMessageTypeByName(message_type);
        if (message_desc == NULL) {
            return Status::InternalError(strings::Substitute("Cannot get message descriptor of message: $0", file_desc.DebugString());
        }

        google::protobuf::DynamicMessageFactory factory;
        const google::protobuf::Message* prototype_msg =
            factory.GetPrototype(message_desc);
        if (prototype_msg == NULL) {
            return Status::InternalError("Cannot create prototype message from message descriptor");
        }

        google::protobuf::Message* mutable_msg = prototype_msg->New();
        if (mutable_msg == NULL) {
            return Status::InternalError("Failed in prototype_msg->New(); to create mutable message");
        }

        if (!mutable_msg->ParseFromArray(data, length)) {
            return Status::InternalError("Failed to parse value in buffer");
        }

        const Reflection* reflection = mutable_msg->GetReflection();
        bool has_error = false;
        for (int i = 0; i < _src_slot_descriptors.size(); i++) {
            auto slot = _src_slot_descriptors[i];
            if (slot == nullptr) {
                continue;
            }
            const FieldDescriptor* field = message_desc->FindFieldByName(slot->col_name());

            // We dont find convert function, continue to next row.
            if (pb_conver_map.count(field->cpp_type()) == 0 ||
                    pb_conver_map[field->cpp_type()].count(slot.type().type) == 0) {
                if (!_strict_mode) {
                    _column_raw_ptrs[i]->append_nulls(1);
                    continue;
                } else {
                    chunk->set_num_rows(num_rows);
                    _counter->num_rows_filtered++;
                    if (_counter->num_rows_filtered++ < 50) {
                        std::stringstream error_msg;
                        error_msg << "Dont support " << field->TypeName() << " to " << slot->type().debug_string() << " convert for protobuf data type. "
                                << "The column is '" << slot->col_name() << ".";
                        _report_error(record.to_string(), error_msg.str());
                    }
                    has_error = true;
                    break;
                }
            }
            std::stringstream error_msg;
            PbConverter converter = pb_conver_map[field->cpp_type()][slot.type().type];
            switch (field->cpp_type()) {
                case FieldDescriptor::CppType::CPPTYPE_INT32:
                    {
                        int32_t value = reflection->GetInt32(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_INT64:
                    {
                        int64_t value = reflection->GetInt64(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_UINT32:
                    {
                        uint32_t value = reflection->GetUInt32(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }                    
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_UINT64:
                    {
                        uint64_t value = reflection->GetUInt64(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }                    
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
                    {
                        double value = reflection->GetDouble(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }                    
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_FLOAT:
                    {
                        float value = reflection->GetFloat(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }                    
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_BOOL:
                    {
                        bool value = reflection->GetBool(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }                    
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_ENUM:
                    {
                        const EnumValueDescriptor* num_descriptor = reflection->GetEnum(*mutable_msg, field);
                        std::string value = num_descriptor->name();
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << value << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }               
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_STRING:
                    {
                        std::string value = reflection->GetString(*mutable_msg, field);
                        has_error = converter.convert_func(_column_raw_ptrs[i], value, _strict_mode);
                        if (has_error) {
                            error_msg << "Value '" << std::to_string(value) << "' is out of range. "
                                      << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                        }            
                    }
                    break;
                case FieldDescriptor::CppType::CPPTYPE_MESSAGE:
                    has_error = true;
                    error_msg = "Dont support Message type for protobuf data type."
                    break;
                default:
                    has_error = true;
                    error_msg = "Unknown protobuf data type."
                    break;
            }
            if (has_error) {
                chunk->set_num_rows(num_rows);
                if (_counter->num_rows_filtered++ < 50) {
                    _report_error(record.to_string(), error_msg.str());
                }
            }
        }
        num_rows += !has_error;
    }
}

StatusOr<ChunkPtr> ProtobufScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    // 1. 第一步需要create chunk
    ChunkPtr src_chunk;
    RETURN_IF_ERROR(_create_src_chunk(&src_chunk));
    const int chunk_capacity = _state->chunk_size();
    src_chunk->reserve(chunk_capacity);
    src_chunk->set_num_rows(0);
    RETURN_IF_ERROR(_parse_protobuf(src_chunk.get()));
    return materialize(nullptr, cast_chunk);
}

void ProtobufScanner::close() {
    FileScanner::close();
}

} // namespace starrocks
