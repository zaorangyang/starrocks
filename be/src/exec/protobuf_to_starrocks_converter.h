
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

#include <unordered_map>
#include <google/protobuf/descriptor.h>
#include "types/logical_type.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/decimalv3_column.h"
#include "runtime/decimalv2_value.h"
#include "runtime/types.h"

namespace starrocks {
class Column;
struct TypeDescriptor;
class Slice;
}

namespace starrocks {

// 我们对strict mode的处理，遵循以下原则。
// 如果上层调用者开启了strict_mode：
// 数据发生溢出，此时对应的field不会append 任何东西，返回false
// 如果上层调用者未开启strict_mode：
// 数据发生溢出，此时对应的field会append null，且返回true
// 调用convert_func的调用者，如果发现函数返回false，应该放弃解析当前这一行的数据。


// just append data
template<typename PbType, typename LocalType>
bool append_pb_convert(Column* column, PbType pbData);

// string to string
bool string_pb_convert(Column* column, std::string pbData, bool strict_mode, const TypeDescriptor* type_desc);

// DECIMALV2 for Pb integer 
// DECIMALV2已经废弃不用，这个转换函数总是返回true
template<typename PbType, typename LocalType>
bool decimalv2_pb_convert(Column* column, PbType pbData);

// DECIMALV3 for Pb integer
// for TYPE_DECIMAL32, LocalType should be int32_t
// for TYPE_DECIMAL64, LocalType should be int64_t
// for TYPE_DECIMAL128, LocalType should be int128_t
template<typename PbType, typename LocalType>
bool decimalv3_pb_convert(Column* column, PbType pbData, bool strict_mode, int decimal_scale);

// DECIMALV2 for Pb float
template<typename PbType, typename LocalType>
bool decimalv2_float_pb_convert(Column* column, PbType pbData);

// DECIMALV2 for Pb double
template<typename PbType, typename LocalType>
bool decimalv2_double_pb_convert(Column* column, PbType pbData);

// DECIMALV3 for Pb float / double
template<typename PbType, typename LocalType>
bool decimalv3_float_double_pb_convert(Column* column, PbType pbData, bool strict_mode, int decimal_scale);

// Pb interger to Sr interger
template<typename PbType, typename LocalType>
bool integer_to_integer_pb_convert(Column* column, PbType pbData, bool strict_mode);

// Pb float/double to Sr interger
template<typename PbType, typename LocalType>
bool float_to_integer_pb_convert(Column* column, PbType pbData, bool strict_mode);

} // namespace starrocks