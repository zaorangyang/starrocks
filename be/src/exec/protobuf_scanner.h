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

#include "formats/protobuf/converter.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "exec/file_scanner.h"
#include "exprs/json_functions.h"
#include "fs/fs.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "simdjson.h"
#include "util/raw_container.h"
#include "util/slice.h"
#include "util/confluent_schema_registry.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "libserdes/serdes.h"
#ifdef __cplusplus
}
#endif

namespace starrocks {

class ProtobufScanner : public FileScanner {
public:
    // TODO: 
    // 1. 记得把cpp文件加入到makefile里，和jsonscanner类似

    // ProtobufScanner被构造的上下文是什么
    // 一个导入task开始的时候，对于kafka来说，就是一批数据(流)要解析的时候

    // 这几个参数的意义
    // 不用纠结，依葫芦画瓢即可

    // 为什么每个scaner都会有open next reader的逻辑
    // 这还能是因为啥，因为scan_range记录了多个数据源呗。我建议是跟进，因为以后可能需要解析文件的pb格式

    // 还有一个东西需要考虑，avro是通过json格式中继解析的，当consuemr消费到一条数据时会通过libserdes库转换为json字符串，然后把json字符串放到队列里，
    // 接着json scanner读取队列的json字符串流，将数据解析出来。我的问题是，pbscanner应该可以读到裸的二进制流，也就是没有经过额外修饰的数据，schema的
    // 解析我们应该放到pbscanner里。而且只能这么做，不这么做你告诉我怎么做。如果这么做的话和scanner的交互应该放在代码哪个位置。
    // 我们需要把和schema交互的逻辑放在pbscanner里。这是我们的需求。
    // 
    // 而且有一个前提，libserdes库已经不能用了，那么我们应该如何和confluent registry做交互。对于这个问题，我们并没有完整的体验，因为demo里没有包含这一步。所以我们
    // 尝试写一个demo，这个demo应该包含和schema registry的交互逻辑，以及动态解析pb数据的能力。
    // 
    // 还有一个问题，scaner是负责owern和registry的交互逻辑，还是仅仅持有和registry交互的handle。这两个做法都需要我们把相关参数传给scanner，如何传。
    // 

    ProtobufScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, serdes_t *serdes, const std::string message_type);

    // 因为无法在单测里测试和schema交互的逻辑，所以这里引入一个新的构造函数用于单测
    ProtobufScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, const std::string schema_text, const std::string message_type);
    ~ProtobufScanner() override;

    // Open this scanner, will initialize information needed
    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    // Close this scanner
    void close() override;

private:
    const TBrokerScanRange& _scan_range;
    serdes_t* _serdes;
    const std::string _message_type;
    // 定义一个C风格的buffer，用于存放serdes库的错误信息
    char _err_msg[512];
    std::vector<Column*> _column_raw_ptrs;
    static std::once_flag _once_flag;
    const std::string _schema_text;
};

} // namespace starrocks
