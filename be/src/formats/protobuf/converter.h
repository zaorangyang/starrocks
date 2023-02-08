
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

using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace google::protobuf::compiler;

template<typename PbType, typename SrType>
class PbConverter {
public:
    virtual ~Converter() = default;
    // 我们对strict mode的处理，遵循以下原则。
    // 如果上层调用者开启了strict_mode：
    // 数据发生溢出，此时对应的field不会append 任何东西，返回false
    // 如果上层调用者未开启strict_mode：
    // 数据发生溢出，此时对应的field会append null，且返回true
    // 调用convert_func的调用者，如果发现函数返回false，应该放弃解析当前这一行的数据。
    virtual bool convert_func(Column* column, PbType pbData, bool strict_mode = false, int decimal_scale = 0, const TypeDescriptor* type_desc = nullptr) = 0;
};

// just append data
template<typename PbType, typename LocalType>
class AppendPbConverter final : public PbConverter {
public:
    bool convert_func(Column* column, PbType pbData) {
        down_cast<FixedLengthColumn<LocalType>*>(column)->append(pbData);
        return true;
    }
};

// string to string
template<typename PbType, typename LocalType>
class StringPbConverter final : public PbConverter {
public:
    bool convert_func(Column* column, PbType pbData, bool strict_mode, int decimal_scale, const TypeDescriptor* type_desc) {
        int max_size = 0;
        if (type_desc != nullptr) {
            max_size = type_desc->len;
        }

        if (strict_mode) {
            if (UNLIKELY((pbData.size() > TypeDescriptor::MAX_VARCHAR_LENGTH) || (max_size > 0 && pbData.size() > max_size))) {
                LOG(WARNING) << "Column [" << column->get_name() << "]'s length exceed max varchar length.";
                return false;
            }
        }
        Slice s(pbData);
        down_cast<BinaryColumn*>(column)->append(s);
        return true;
    }
};

// DECIMALV2 for Pb integer
template<typename PbType, typename LocalType>
class Decimalv2PbConverter final : public PbConverter {
public:
    // DECIMALV2已经废弃不用，这个转换函数总是返回true
    void convert_func(Column* column, PbType pbData) {
        DecimalV2Value decimalv2Value(pbData, 0);
        down_cast<FixedLengthColumn<DecimalV2Value>*>(column)->append(decimalv2Value);
        return true;
    }
};

// DECIMALV3 for Pb integer
template<typename PbType, typename LocalType>
class Decimalv3PbConverter final : public PbConverter {
public:
    // for TYPE_DECIMAL32, LocalType should be int32_t
    // for TYPE_DECIMAL64, LocalType should be int64_t
    // for TYPE_DECIMAL128, LocalType should be int128_t
    bool convert_func(Column* column, PbType pbData, bool strict_mode, int decimal_scale) {
        auto decimalv3_column = down_cast<DecimalV3Column<LocalType>*>(column);
        LocalType dec_value;
        bool overflow = DecimalV3Cast::from_integer<PbType, LocalType, true>(pbData, scale, &dec_value);
        if (overflow) {
            if (strict_mode) {
                return false;
            }
            // TODO: 这里需要做测试
            column->append_nulls(1);
            return true
        }
        decimalv3_column->append(dec_value);
        return true;
    }
};

// DECIMALV2 for Pb float
template<typename PbType, typename LocalType>
class Decimalv2FloatPbConverter final : public PbConverter {
public:
    bool convert_func(Column* column, PbType pbData) {
        DDecimalV2Value decimalv2Value = assign_from_float(pbData);
        down_cast<FixedLengthColumn<DecimalV2Value>*>(column)->append(decimalv2Value);
        return true;
    }
};

// DECIMALV2 for Pb double
template<typename PbType, typename LocalType>
class Decimalv2DoublePbConverter final : public PbConverter {
public:
    bool convert_func(Column* column, PbType pbData) {
        DDecimalV2Value decimalv2Value = assign_from_double(pbData);
        down_cast<FixedLengthColumn<DecimalV2Value>*>(column)->append(decimalv2Value);
        return true;
    }
};

// DECIMALV3 for Pb float / double
template<typename PbType, typename LocalType>
class Decimalv3FloatDoublePbConverter final : public PbConverter {
public:
    bool convert_func(Column* column, PbType pbData, bool strict_mode, int decimal_scale) {
        auto decimalv3_column = down_cast<DecimalV3Column<LocalType>*>(column);
        LocalType dec_value;
        bool overflow = DecimalV3Cast::from_float<PbType, LocalType, true>(pbData, scale, &dec_value);
        if (overflow) {
            if (strict_mode) {
                return false;
            }
            // TODO: 这里需要做测试
            column->append_nulls(1);
            return true
        }
        decimalv3_column->append(dec_value);
        return true;
    }
};

// Pb interger to Sr interger
template<typename PbType, typename LocalType>
class IntegerToIntegerPbConverter final : public PbConverter {
public:
    bool convert_func(Column* column, PbType pbData, bool strict_mode) {
        auto n = implicit_cast<LocalType>(pbData)
        bool overflow = false;
        if (implicit_cast<PbType>(n) != pbData) {
            overflow = true;
        }
        if (overflow) {
            if (strict_mode) {
                return false;
            }
            // TODO: 这里需要做测试
            column->append_nulls(1);
            return true
        }
        down_cast<FixedLengthColumn<LocalType>*>(column)->append(pbData);
        return true;
    }
};

// Pb float/double to Sr interger
template<typename PbType, typename LocalType>
class FloatToIntegerPbConverter final : public PbConverter {
public:
    bool convert_func(Column* column, PbType pbData, bool strict_mode) {
        bool overflow = false;
        pbData = std::trunc(pbData);
        auto n = implicit_cast<LocalType>(pbData);
        if (implicit_cast<PbType>(n) != pbData) {
            overflow = true;
        }
        if (overflow) {
            if (strict_mode) {
                return false;
            }
            // TODO: 这里需要做测试
            column->append_nulls(1);
            return true
        }
        down_cast<FixedLengthColumn<LocalType>*>(column)->append(pbData);
        return true;
    }
};


extern const std::unordered_map<FieldDescriptor::CppType, std::unordered_map<LogicalType, PbConverter>> pb_conver_map

void init_pb_conver_map();