#include "formats/protobuf/protobuf_to_starrocks_converter.h"

namespace starrocks {

const std::unordered_map<FieldDescriptor::CppType, std::unordered_map<LogicalType, PbConverter>> pb_conver_map;

void init_pb_conver_map() {
    // CPPTYPE_INT32
    std::unordered_map<LogicalType, PbConverter> int32_pb_convert_map = {
        {LogicalType::TYPE_TINYINT, IntegerToIntegerPbConverter<int32_t, int8_t>()},
        {LogicalType::TYPE_UNSIGNED_TINYINT, IntegerToIntegerPbConverter<int32_t, uint8_t>()},
        {LogicalType::TYPE_SMALLINT, IntegerToIntegerPbConverter<int32_t, int16_t>()},
        {LogicalType::TYPE_UNSIGNED_SMALLINT, IntegerToIntegerPbConverter<int32_t, uint16_t>()},
        {LogicalType::TYPE_INT, AppendPbConverter<int32_t, int32_t>()},
        {LogicalType::TYPE_UNSIGNED_INT, IntegerToIntegerPbConverter<int32_t, uint32_t>()},
        {LogicalType::TYPE_BIGINT, AppendPbConverter<int32_t, int64_t>()},
        {LogicalType::TYPE_UNSIGNED_BIGINT, AppendPbConverter<int32_t, uint64_t>()},
        {LogicalType::TYPE_LARGEINT, AppendPbConverter<int32_t, int128_t>()},
        {LogicalType::TYPE_FLOAT, AppendPbConverter<int32_t, float>()},
        {LogicalType::TYPE_DOUBLE, AppendPbConverter<int32_t, double>()},
        {LogicalType::TYPE_CHAR, IntegerToIntegerPbConverter<int32_t, int8_t>()},
        {LogicalType::TYPE_BOOLEAN, IntegerToIntegerPbConverter<int32_t, uint8_t>()},
        {LogicalType::TYPE_DECIMAL32, Decimalv3PbConverter<int32_t, int32_t>()},
        {LogicalType::TYPE_DECIMAL64, Decimalv3PbConverter<int32_t, int64_t>()},
        {LogicalType::TYPE_DECIMAL128, Decimalv3PbConverter<int32_t, int128_t>()},
        {LogicalType::TYPE_DECIMALV2, Decimalv2PbConverter<int32_t, DecimalV2Value>()},
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_INT32, int32_pb_convert_map);
    // CPPTYPE_INT64
    std::unordered_map<LogicalType, PbConverter> int64_pb_convert_map = {
        {LogicalType::TYPE_TINYINT, IntegerToIntegerPbConverter<int64_t, int8_t>()},
        {LogicalType::TYPE_UNSIGNED_TINYINT, IntegerToIntegerPbConverter<int64_t, uint8_t>()},
        {LogicalType::TYPE_SMALLINT, IntegerToIntegerPbConverter<int64_t, int16_t>()},
        {LogicalType::TYPE_UNSIGNED_SMALLINT, IntegerToIntegerPbConverter<int64_t, uint16_t>()},
        {LogicalType::TYPE_INT, IntegerToIntegerPbConverter<int64_t, int32_t>()},
        {LogicalType::TYPE_UNSIGNED_INT, IntegerToIntegerPbConverter<int64_t, uint32_t>()},
        {LogicalType::TYPE_BIGINT, AppendPbConverter<int64_t, int64_t>()},
        {LogicalType::TYPE_UNSIGNED_BIGINT, AppendPbConverter<int64_t, uint64_t>()},
        {LogicalType::TYPE_LARGEINT, AppendPbConverter<int64_t, int128_t>()},
        {LogicalType::TYPE_FLOAT, AppendPbConverter<int64_t, float>()},
        {LogicalType::TYPE_DOUBLE, AppendPbConverter<int64_t, double>()},
        {LogicalType::TYPE_CHAR, IntegerToIntegerPbConverter<int64_t, int8_t>()},
        {LogicalType::TYPE_BOOLEAN, IntegerToIntegerPbConverter<int64_t, uint8_t>()},
        {LogicalType::TYPE_DECIMAL32, Decimalv3PbConverter<int64_t, int32_t>()},
        {LogicalType::TYPE_DECIMAL64, Decimalv3PbConverter<int64_t, int64_t>()},
        {LogicalType::TYPE_DECIMAL128, Decimalv3PbConverter<int64_t, int128_t>()},
        {LogicalType::TYPE_DECIMALV2, Decimalv2PbConverter<int64_t, DecimalV2Value>()},
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_INT64, int64_pb_convert_map);
    // CPPTYPE_UINT32
    std::unordered_map<LogicalType, PbConverter> uint32_pb_convert_map = {
        {LogicalType::TYPE_TINYINT, IntegerToIntegerPbConverter<uint32_t, int8_t>()},
        {LogicalType::TYPE_UNSIGNED_TINYINT, IntegerToIntegerPbConverter<uint32_t, uint8_t>()},
        {LogicalType::TYPE_SMALLINT, IntegerToIntegerPbConverter<uint32_t, int16_t>()},
        {LogicalType::TYPE_UNSIGNED_SMALLINT, IntegerToIntegerPbConverter<uint32_t, uint16_t>()},
        {LogicalType::TYPE_INT, IntegerToIntegerPbConverter<uint32_t, int32_t>()},
        {LogicalType::TYPE_UNSIGNED_INT, AppendPbConverter<uint32_t, uint32_t>()},
        {LogicalType::TYPE_BIGINT, AppendPbConverter<uint32_t, int64_t>()},
        {LogicalType::TYPE_UNSIGNED_BIGINT, AppendPbConverter<uint32_t, uint64_t>()},
        {LogicalType::TYPE_LARGEINT, AppendPbConverter<uint32_t, int128_t>()},
        {LogicalType::TYPE_FLOAT, AppendPbConverter<uint32_t, float>()},
        {LogicalType::TYPE_DOUBLE, AppendPbConverter<uint32_t, double>()},
        {LogicalType::TYPE_CHAR, IntegerToIntegerPbConverter<uint32_t, int8_t>()},
        {LogicalType::TYPE_BOOLEAN, IntegerToIntegerPbConverter<uint32_t, uint8_t>()},
        {LogicalType::TYPE_DECIMAL32, Decimalv3PbConverter<uint32_t, int32_t>()},
        {LogicalType::TYPE_DECIMAL64, Decimalv3PbConverter<uint32_t, int64_t>()},
        {LogicalType::TYPE_DECIMAL128, Decimalv3PbConverter<uint32_t, int128_t>()},
        {LogicalType::TYPE_DECIMALV2, Decimalv2PbConverter<uint32_t, DecimalV2Value>()},
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_UINT32, uint32_pb_convert_map);
    // CPPTYPE_UINT64
    std::unordered_map<LogicalType, PbConverter> uint64_pb_convert_map = {
        {LogicalType::TYPE_TINYINT, IntegerToIntegerPbConverter<uint64_t, int8_t>()},
        {LogicalType::TYPE_UNSIGNED_TINYINT, IntegerToIntegerPbConverter<uint64_t, uint8_t>()},
        {LogicalType::TYPE_SMALLINT, IntegerToIntegerPbConverter<uint64_t, int16_t>()},
        {LogicalType::TYPE_UNSIGNED_SMALLINT, IntegerToIntegerPbConverter<uint64_t, uint16_t>()},
        {LogicalType::TYPE_INT, IntegerToIntegerPbConverter<uint64_t, int32_t>()},
        {LogicalType::TYPE_UNSIGNED_INT, IntegerToIntegerPbConverter<uint64_t, uint32_t>()},
        {LogicalType::TYPE_BIGINT, IntegerToIntegerPbConverter<uint64_t, int64_t>()},
        {LogicalType::TYPE_UNSIGNED_BIGINT, AppendPbConverter<uint64_t, uint64_t>()},
        {LogicalType::TYPE_LARGEINT, AppendPbConverter<uint64_t, int128_t>()},
        {LogicalType::TYPE_FLOAT, AppendPbConverter<uint64_t, float>()},
        {LogicalType::TYPE_DOUBLE, AppendPbConverter<uint64_t, double>()},
        {LogicalType::TYPE_CHAR, IntegerToIntegerPbConverter<uint64_t, int8_t>()},
        {LogicalType::TYPE_BOOLEAN, IntegerToIntegerPbConverter<uint64_t, uint8_t>()},
        {LogicalType::TYPE_DECIMAL32, Decimalv3PbConverter<uint64_t, int32_t>()},
        {LogicalType::TYPE_DECIMAL64, Decimalv3PbConverter<uint64_t, int64_t>()},
        {LogicalType::TYPE_DECIMAL128, Decimalv3PbConverter<uint64_t, int128_t>()},
        {LogicalType::TYPE_DECIMALV2, Decimalv2PbConverter<uint64_t, DecimalV2Value>()},
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_UINT64, uint64_pb_convert_map);
    // CPPTYPE_DOUBLE
    std::unordered_map<LogicalType, PbConverter> double_pb_convert_map = {
        {LogicalType::TYPE_TINYINT, FloatToIntegerPbConverter<double, int8_t>()},
        {LogicalType::TYPE_UNSIGNED_TINYINT, FloatToIntegerPbConverter<double, uint8_t>()},
        {LogicalType::TYPE_SMALLINT, FloatToIntegerPbConverter<double, int16_t>()},
        {LogicalType::TYPE_UNSIGNED_SMALLINT, FloatToIntegerPbConverter<double, uint16_t>()},
        {LogicalType::TYPE_INT, FloatToIntegerPbConverter<double, int32_t>()},
        {LogicalType::TYPE_UNSIGNED_INT, FloatToIntegerPbConverter<double, uint32_t>()},
        {LogicalType::TYPE_BIGINT, FloatToIntegerPbConverter<double, int64_t>()},
        {LogicalType::TYPE_UNSIGNED_BIGINT, FloatToIntegerPbConverter<double, uint64_t>()},
        {LogicalType::TYPE_LARGEINT, FloatToIntegerPbConverter<double, int128_t>()},
        {LogicalType::TYPE_FLOAT, AppendPbConverter<double, float>()},
        {LogicalType::TYPE_DOUBLE, AppendPbConverter<double, double>()},
        {LogicalType::TYPE_CHAR, FloatToIntegerPbConverter<double, int8_t>()},
        {LogicalType::TYPE_BOOLEAN, FloatToIntegerPbConverter<double, uint8_t>()},
        {LogicalType::TYPE_DECIMAL32, Decimalv3FloatDoublePbConverter<double, int32_t>()},
        {LogicalType::TYPE_DECIMAL64, Decimalv3FloatDoublePbConverter<double, int64_t>()},
        {LogicalType::TYPE_DECIMAL128, Decimalv3FloatDoublePbConverter<double, int128_t>()},
        {LogicalType::TYPE_DECIMALV2, Decimalv2DoublePbConverter<double, DecimalV2Value>()},
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_DOUBLE, double_pb_convert_map);
    // CPPTYPE_FLOAT
    std::unordered_map<LogicalType, PbConverter> float_pb_convert_map = {
        {LogicalType::TYPE_TINYINT, FloatToIntegerPbConverter<float, int8_t>()},
        {LogicalType::TYPE_UNSIGNED_TINYINT, FloatToIntegerPbConverter<float, uint8_t>()},
        {LogicalType::TYPE_SMALLINT, FloatToIntegerPbConverter<float, int16_t>()},
        {LogicalType::TYPE_UNSIGNED_SMALLINT, FloatToIntegerPbConverter<float, uint16_t>()},
        {LogicalType::TYPE_INT, FloatToIntegerPbConverter<float, int32_t>()},
        {LogicalType::TYPE_UNSIGNED_INT, FloatToIntegerPbConverter<float, uint32_t>()},
        {LogicalType::TYPE_BIGINT, FloatToIntegerPbConverter<float, int64_t>()},
        {LogicalType::TYPE_UNSIGNED_BIGINT, FloatToIntegerPbConverter<float, uint64_t>()},
        {LogicalType::TYPE_LARGEINT, FloatToIntegerPbConverter<float, int128_t>()},
        {LogicalType::TYPE_FLOAT, AppendPbConverter<float, float>()},
        {LogicalType::TYPE_DOUBLE, AppendPbConverter<float, double>()},
        {LogicalType::TYPE_CHAR, FloatToIntegerPbConverter<float, int8_t>()},
        {LogicalType::TYPE_BOOLEAN, FloatToIntegerPbConverter<float, uint8_t>()},
        {LogicalType::TYPE_DECIMAL32, Decimalv3FloatDoublePbConverter<float, int32_t>()},
        {LogicalType::TYPE_DECIMAL64, Decimalv3FloatDoublePbConverter<float, int64_t>()},
        {LogicalType::TYPE_DECIMAL128, Decimalv3FloatDoublePbConverter<float, int128_t>()},
        {LogicalType::TYPE_DECIMALV2, Decimalv2FloatPbConverter<float, DecimalV2Value>()},
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_FLOAT, float_pb_convert_map);
    // CPPTYPE_BOOL
    std::unordered_map<LogicalType, PbConverter> bool_pb_convert_map = {
        {LogicalType::TYPE_TINYINT, IntegerToIntegerPbConverter<uint8_t, int8_t>()},
        {LogicalType::TYPE_UNSIGNED_TINYINT, AppendPbConverter<uint8_t, uint8_t>()},
        {LogicalType::TYPE_SMALLINT, AppendPbConverter<uint8_t, int16_t>()},
        {LogicalType::TYPE_UNSIGNED_SMALLINT, AppendPbConverter<uint8_t, uint16_t>()},
        {LogicalType::TYPE_INT, AppendPbConverter<uint8_t, int32_t>()},
        {LogicalType::TYPE_UNSIGNED_INT, AppendPbConverter<uint8_t, uint32_t>()},
        {LogicalType::TYPE_BIGINT, AppendPbConverter<uint8_t, int64_t>()},
        {LogicalType::TYPE_UNSIGNED_BIGINT, AppendPbConverter<uint8_t, uint64_t>()},
        {LogicalType::TYPE_LARGEINT, AppendPbConverter<uint8_t, int128_t>()},
        {LogicalType::TYPE_FLOAT, AppendPbConverter<uint8_t, float>()},
        {LogicalType::TYPE_DOUBLE, AppendPbConverter<uint8_t, double>()},
        {LogicalType::TYPE_CHAR, IntegerToIntegerPbConverter<uint8_t, int8_t>()},
        {LogicalType::TYPE_BOOLEAN, AppendPbConverter<uint8_t, uint8_t>()},
        {LogicalType::TYPE_DECIMAL32, Decimalv3PbConverter<uint8_t, int32_t>()},
        {LogicalType::TYPE_DECIMAL64, Decimalv3PbConverter<uint8_t, int64_t>()},
        {LogicalType::TYPE_DECIMAL128, Decimalv3PbConverter<uint8_t, int128_t>()},
        {LogicalType::TYPE_DECIMALV2, Decimalv2PbConverter<uint8_t, DecimalV2Value>()},
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_BOOL, bool_pb_convert_map);
    // CPPTYPE_ENUM
    std::unordered_map<LogicalType, PbConverter> enum_pb_convert_map = {
        {LogicalType::TYPE_VARCHAR, StringPbConverter<std::string, std::string>()}
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_ENUM, enum_pb_convert_map);
    // CPPTYPE_STRING
    std::unordered_map<LogicalType, PbConverter> string_pb_convert_map = {
        {LogicalType::TYPE_VARCHAR, StringPbConverter<std::string, std::string>()}
    };
    pb_conver_map.insert(FieldDescriptor::CppType::CPPTYPE_STRING, string_pb_convert_map);
}

} // namespace starrocks