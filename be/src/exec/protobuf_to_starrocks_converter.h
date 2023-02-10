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
#include "runtime/decimalv3.h"

namespace starrocks {
class Column;
struct TypeDescriptor;
class Slice;
}

namespace starrocks {

// just append data
template<typename PbType, typename LocalType>
bool append_pb_convert(Column* column, PbType pbData) {
    down_cast<FixedLengthColumn<LocalType>*>(column)->append(pbData);
    return true;
}

// string to string
bool string_pb_convert(Column* column, std::string pbData, bool strict_mode, const TypeDescriptor* type_desc) {
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

// DECIMALV2 for Pb integer 
// DECIMALV2已经废弃不用，这个转换函数总是返回true
template<typename PbType, typename LocalType>
bool decimalv2_pb_convert(Column* column, PbType pbData) {
    DecimalV2Value decimalv2Value(pbData, 0);
    down_cast<FixedLengthColumn<DecimalV2Value>*>(column)->append(decimalv2Value);
    return true;
}

// DECIMALV3 for Pb integer
// for TYPE_DECIMAL32, LocalType should be int32_t
// for TYPE_DECIMAL64, LocalType should be int64_t
// for TYPE_DECIMAL128, LocalType should be int128_t
template<typename PbType, typename LocalType>
bool decimalv3_pb_convert(Column* column, PbType pbData, bool strict_mode, int decimal_scale) {
    auto decimalv3_column = down_cast<DecimalV3Column<LocalType>*>(column);
    LocalType dec_value;
    const auto scale_factor = get_scale_factor<LocalType>(decimal_scale);
    bool overflow = DecimalV3Cast::from_integer<PbType, LocalType, true>(pbData, scale_factor, &dec_value);
    if (overflow) {
        if (strict_mode) {
            return false;
        }
        // TODO: 这里需要做测试
        column->append_nulls(1);
        return true;
    }
    decimalv3_column->append(dec_value);
    return true;
}

// DECIMALV2 for Pb float
template<typename PbType, typename LocalType>
bool decimalv2_float_pb_convert(Column* column, PbType pbData) {
    DecimalV2Value decimalv2Value = assign_from_float(pbData);
    down_cast<FixedLengthColumn<DecimalV2Value>*>(column)->append(decimalv2Value);
    return true;
}

// DECIMALV2 for Pb double
template<typename PbType, typename LocalType>
bool decimalv2_double_pb_convert(Column* column, PbType pbData) {
    DecimalV2Value decimalv2Value = assign_from_double(pbData);
    down_cast<FixedLengthColumn<DecimalV2Value>*>(column)->append(decimalv2Value);
    return true;
}

// DECIMALV3 for Pb float / double
template<typename PbType, typename LocalType>
bool decimalv3_float_double_pb_convert(Column* column, PbType pbData, bool strict_mode, int decimal_scale) {
    auto decimalv3_column = down_cast<DecimalV3Column<LocalType>*>(column);
    LocalType dec_value;
    bool overflow = DecimalV3Cast::from_float<PbType, LocalType>(pbData, decimal_scale, &dec_value);
    if (overflow) {
        if (strict_mode) {
            return false;
        }
        // TODO: 这里需要做测试
        column->append_nulls(1);
        return true;
    }
    decimalv3_column->append(dec_value);
    return true;
}

// Pb interger to Sr interger
template<typename PbType, typename LocalType>
bool integer_to_integer_pb_convert(Column* column, PbType pbData, bool strict_mode) {
    auto n = implicit_cast<LocalType>(pbData);
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
        return true;
    }
    down_cast<FixedLengthColumn<LocalType>*>(column)->append(pbData);
    return true;
}

// Pb float/double to Sr interger
template<typename PbType, typename LocalType>
bool float_to_integer_pb_convert(Column* column, PbType pbData, bool strict_mode) {
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
        return true;
    }
    down_cast<FixedLengthColumn<LocalType>*>(column)->append(pbData);
    return true;
}

} // namespace starrocks