#include <iostream>
#include <memory>

#include "exec/file_scanner.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "exec/json_scanner.h"

#include <utility>
#include <fstream>
#include "exec/json_scanner.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/defer_op.h"


namespace starrocks {

#define JSONBENCHMARK

class JsonScannerBench {
public:
    std::unique_ptr<JsonScanner> create_json_scanner(const std::vector<TypeDescriptor>& types,
                                                     const std::vector<TBrokerRangeDesc>& ranges,
                                                     const std::vector<std::string>& col_names,
                                                     BenchData& bench_data) {
        config::vector_chunk_size = 4096;
        _profile = _pool.add(new RuntimeProfile("test"));
        _counter = _pool.add(new ScannerCounter());
        _state = _pool.add(new RuntimeState(TQueryGlobals()));
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (int i = 0; i < types.size(); ++i) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(types[i]).column_name(col_names[i]).length(types[i].len).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(_state, &_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        _state->set_desc_tbl(desc_tbl);
        _state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        for (int i = 0; i < types.size(); i++) {
            params->expr_of_dest_slot[i] = TExpr();
            params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
            params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        }

        for (int i = 0; i < types.size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        TBrokerScanRange* broker_scan_range = _pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        return std::make_unique<JsonScanner>(_state, _profile, *broker_scan_range, _counter, &bench_data);
    }

private:
    RuntimeProfile* _profile = nullptr;
    ScannerCounter* _counter = nullptr;
    RuntimeState* _state = nullptr;
    ObjectPool _pool;
};

} // namespace starrocks

using namespace starrocks;

// 50GB
#define BUFFER_SIZE 53687091200

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " [file]" << " [batchrows]"<< std::endl;
        exit(1);
    }

    uint8_t* buffer = (uint8_t*)malloc(BUFFER_SIZE * sizeof(uint8_t));
    uint8_t* cur_pos = buffer;

    std::string filename = argv[1];
    std::string batch_rows_str = argv[2];
    std::stringstream stream2;
    stream2 << batch_rows_str;
    int64_t batch_rows;
    stream2 >> batch_rows;
    std::cout << "batch rows: " << batch_rows << std::endl;

	std::ifstream in(filename.c_str());
	if(!in) {
		std::cerr << "Can't open the file." << std::endl;
		return -1;
	}
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DATETIME);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(255));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_DATETIME);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_CHAR);
    types.emplace_back(TYPE_DATETIME);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TypeDescriptor::create_varchar_type(2000));
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path(filename);
    ranges.emplace_back(range);

    JsonScannerBench scanner_bench;
    BenchData bench_data;
    auto scanner = scanner_bench.create_json_scanner(types, ranges, {"CounterID", "EventDate", "UserID", "EventTime", "WatchID", "JavaEnable", "Title", "GoodEvent", "ClientIP", "RegionID", "CounterClass", "OS", "UserAgent", "URL", "Referer", "IsRefresh", "RefererCategoryID", "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth", "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor", "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor", "UserAgentMinor", "CookieEnable", "JavascriptEnable", "IsMobile", "MobilePhone", "MobilePhoneModel", "Params", "IPNetworkID", "TraficSourceID", "SearchEngineID", "SearchPhrase", "AdvEngineID", "IsArtifical", "WindowClientWidth", "WindowClientHeight", "ClientTimeZone", "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2", "SilverlightVersion3", "SilverlightVersion4", "PageCharset", "CodeVersion", "IsLink", "IsDownload", "IsNotBounce", "FUniqID", "OriginalURL", "HID", "IsOldCounter", "IsEvent", "IsParameter", "DontCountHits", "WithHash", "HitColor", "LocalEventTime", "Age", "Sex", "Income", "Interests", "Robotness", "RemoteIP", "WindowName", "OpenerName", "HistoryLength", "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction", "HTTPError", "SendTiming", "DNSTiming", "ConnectTiming", "ResponseStartTiming", "ResponseEndTiming", "FetchTiming", "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID", "ParamCurrency", "ParamCurrencyID", "OpenstatServiceName", "OpenstatCampaignID", "OpenstatAdID", "OpenstatSourceID", "UTMSource", "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm", "FromTag", "HasGCLID", "RefererHash", "URLHash", "CLID"}, bench_data);
    Status st = scanner->open();
    if (!st.ok()) {
        std::cout << "Open scanner error. status: " << st.to_string();
    }
    
    // Benchmark 2: Parsing
    auto start = std::chrono::system_clock::now();
    int64_t parsed_count = 0;

    std::string line;
    int64_t count = 0;
	while(getline(in, line)) {
        memcpy(cur_pos, line.c_str(), line.size());
        bench_data.put_row(cur_pos, line.size());
        cur_pos = cur_pos + line.size();
        count++;
        if ((count % batch_rows) == 0) {
            while (true) {
                auto ret = scanner->get_next();
                if (ret.status().ok()) {
                    parsed_count += ret.value()->num_rows();
                } else {
                    break;
                }
            }
        }
    }

    while (true) {
        auto ret = scanner->get_next();
        if (ret.status().ok()) {
            parsed_count += ret.value()->num_rows();
        } else {
            break;
        }
    }

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end - start;
    if (buffer != nullptr) {
        free(buffer);
    }
    std::cout << "Have parsed " << parsed_count << " records" << std::endl;
    std::cout << "Parsing: " << diff.count() << std::endl;

} // namespace starrocks
