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
#include <sstream>
#include "exec/avro_scanner.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/defer_op.h"
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif
#include "avro.h"
#ifdef __cplusplus
}
#endif


namespace starrocks {

#define AVROBENCHMARK

class AvroScannerBench {
public:
    std::unique_ptr<AvroScanner> create_avro_scanner(const std::vector<TypeDescriptor>& types,
                                                     const std::vector<TBrokerRangeDesc>& ranges,
                                                     const std::vector<std::string>& col_names,
                                                     AvroBenchData& bench_data) {
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
        return std::make_unique<AvroScanner>(_state, _profile, *broker_scan_range, _counter, bench_data);
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
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " [file]" << " [datarows]"<< std::endl;
        exit(1);
    }
    std::cout << "V5" << std::endl;
    std::string filename = argv[1];

    std::string schema_str("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"WatchID\",\"type\":\"string\"},{\"name\":\"JavaEnable\",\"type\":\"long\"},{\"name\":\"Title\",\"type\":\"string\"},{\"name\":\"GoodEvent\",\"type\":\"long\"},{\"name\":\"EventTime\",\"type\":\"string\"},{\"name\":\"EventDate\",\"type\":\"string\"},{\"name\":\"CounterID\",\"type\":\"long\"},{\"name\":\"ClientIP\",\"type\":\"long\"},{\"name\":\"RegionID\",\"type\":\"long\"},{\"name\":\"UserID\",\"type\":\"string\"},{\"name\":\"CounterClass\",\"type\":\"long\"},{\"name\":\"OS\",\"type\":\"long\"},{\"name\":\"UserAgent\",\"type\":\"long\"},{\"name\":\"URL\",\"type\":\"string\"},{\"name\":\"Referer\",\"type\":\"string\"},{\"name\":\"IsRefresh\",\"type\":\"long\"},{\"name\":\"RefererCategoryID\",\"type\":\"long\"},{\"name\":\"RefererRegionID\",\"type\":\"long\"},{\"name\":\"URLCategoryID\",\"type\":\"long\"},{\"name\":\"URLRegionID\",\"type\":\"long\"},{\"name\":\"ResolutionWidth\",\"type\":\"long\"},{\"name\":\"ResolutionHeight\",\"type\":\"long\"},{\"name\":\"ResolutionDepth\",\"type\":\"long\"},{\"name\":\"FlashMajor\",\"type\":\"long\"},{\"name\":\"FlashMinor\",\"type\":\"long\"},{\"name\":\"FlashMinor2\",\"type\":\"string\"},{\"name\":\"NetMajor\",\"type\":\"long\"},{\"name\":\"NetMinor\",\"type\":\"long\"},{\"name\":\"UserAgentMajor\",\"type\":\"long\"},{\"name\":\"UserAgentMinor\",\"type\":\"string\"},{\"name\":\"CookieEnable\",\"type\":\"long\"},{\"name\":\"JavascriptEnable\",\"type\":\"long\"},{\"name\":\"IsMobile\",\"type\":\"long\"},{\"name\":\"MobilePhone\",\"type\":\"long\"},{\"name\":\"MobilePhoneModel\",\"type\":\"string\"},{\"name\":\"Params\",\"type\":\"string\"},{\"name\":\"IPNetworkID\",\"type\":\"long\"},{\"name\":\"TraficSourceID\",\"type\":\"long\"},{\"name\":\"SearchEngineID\",\"type\":\"long\"},{\"name\":\"SearchPhrase\",\"type\":\"string\"},{\"name\":\"AdvEngineID\",\"type\":\"long\"},{\"name\":\"IsArtifical\",\"type\":\"long\"},{\"name\":\"WindowClientWidth\",\"type\":\"long\"},{\"name\":\"WindowClientHeight\",\"type\":\"long\"},{\"name\":\"ClientTimeZone\",\"type\":\"long\"},{\"name\":\"ClientEventTime\",\"type\":\"string\"},{\"name\":\"SilverlightVersion1\",\"type\":\"long\"},{\"name\":\"SilverlightVersion2\",\"type\":\"long\"},{\"name\":\"SilverlightVersion3\",\"type\":\"long\"},{\"name\":\"SilverlightVersion4\",\"type\":\"long\"},{\"name\":\"PageCharset\",\"type\":\"string\"},{\"name\":\"CodeVersion\",\"type\":\"long\"},{\"name\":\"IsLink\",\"type\":\"long\"},{\"name\":\"IsDownload\",\"type\":\"long\"},{\"name\":\"IsNotBounce\",\"type\":\"long\"},{\"name\":\"FUniqID\",\"type\":\"string\"},{\"name\":\"OriginalURL\",\"type\":\"string\"},{\"name\":\"HID\",\"type\":\"long\"},{\"name\":\"IsOldCounter\",\"type\":\"long\"},{\"name\":\"IsEvent\",\"type\":\"long\"},{\"name\":\"IsParameter\",\"type\":\"long\"},{\"name\":\"DontCountHits\",\"type\":\"long\"},{\"name\":\"WithHash\",\"type\":\"long\"},{\"name\":\"HitColor\",\"type\":\"string\"},{\"name\":\"LocalEventTime\",\"type\":\"string\"},{\"name\":\"Age\",\"type\":\"long\"},{\"name\":\"Sex\",\"type\":\"long\"},{\"name\":\"Income\",\"type\":\"long\"},{\"name\":\"Interests\",\"type\":\"long\"},{\"name\":\"Robotness\",\"type\":\"long\"},{\"name\":\"RemoteIP\",\"type\":\"long\"},{\"name\":\"WindowName\",\"type\":\"long\"},{\"name\":\"OpenerName\",\"type\":\"long\"},{\"name\":\"HistoryLength\",\"type\":\"long\"},{\"name\":\"BrowserLanguage\",\"type\":\"string\"},{\"name\":\"BrowserCountry\",\"type\":\"string\"},{\"name\":\"SocialNetwork\",\"type\":\"string\"},{\"name\":\"SocialAction\",\"type\":\"string\"},{\"name\":\"HTTPError\",\"type\":\"long\"},{\"name\":\"SendTiming\",\"type\":\"long\"},{\"name\":\"DNSTiming\",\"type\":\"long\"},{\"name\":\"ConnectTiming\",\"type\":\"long\"},{\"name\":\"ResponseStartTiming\",\"type\":\"long\"},{\"name\":\"ResponseEndTiming\",\"type\":\"long\"},{\"name\":\"FetchTiming\",\"type\":\"long\"},{\"name\":\"SocialSourceNetworkID\",\"type\":\"long\"},{\"name\":\"SocialSourcePage\",\"type\":\"string\"},{\"name\":\"ParamPrice\",\"type\":\"string\"},{\"name\":\"ParamOrderID\",\"type\":\"string\"},{\"name\":\"ParamCurrency\",\"type\":\"string\"},{\"name\":\"ParamCurrencyID\",\"type\":\"long\"},{\"name\":\"OpenstatServiceName\",\"type\":\"string\"},{\"name\":\"OpenstatCampaignID\",\"type\":\"string\"},{\"name\":\"OpenstatAdID\",\"type\":\"string\"},{\"name\":\"OpenstatSourceID\",\"type\":\"string\"},{\"name\":\"UTMSource\",\"type\":\"string\"},{\"name\":\"UTMMedium\",\"type\":\"string\"},{\"name\":\"UTMCampaign\",\"type\":\"string\"},{\"name\":\"UTMContent\",\"type\":\"string\"},{\"name\":\"UTMTerm\",\"type\":\"string\"},{\"name\":\"FromTag\",\"type\":\"string\"},{\"name\":\"HasGCLID\",\"type\":\"long\"},{\"name\":\"RefererHash\",\"type\":\"string\"},{\"name\":\"URLHash\",\"type\":\"string\"},{\"name\":\"CLID\",\"type\":\"long\"}]}");
    avro_schema_error_t error;
    avro_schema_t schema = NULL;
    int result = avro_schema_from_json(schema_str.c_str(), schema_str.size(), &schema, &error);
    if (result != 0) {
        std::cout << "parse schema from json error: " << avro_strerror() << std::endl;
    }
    avro_file_reader_t dbreader;
    if (avro_file_reader(filename.c_str(), &dbreader)) {
            fprintf(stderr, "Error opening file: %s\n", avro_strerror());
            exit(EXIT_FAILURE);
    }


    uint8_t* buffer = (uint8_t*)malloc(BUFFER_SIZE * sizeof(uint8_t));
    DeferOp bufferDeleter([&] {
        if (buffer != nullptr) {
            free(buffer);
        }
    });
    std::string data_rows_str = argv[2];
    std::stringstream stream;
    stream << data_rows_str;
    int64_t data_rows;
    stream >> data_rows;
    std::cout << "data rows: " << data_rows << std::endl;

    // Benchmark 1: File IO
    auto start = std::chrono::system_clock::now();
	std::ifstream in(filename.c_str());
	if(!in) {
		std::cerr << "Can't open the file." << std::endl;
		return -1;
	}

    AvroBenchData bench_data;
    for (int64_t i=0; i<data_rows; i++) {
        avro_value_iface_t  *clickbench_class = avro_generic_class_from_schema(schema);
        avro_value_t clickbench;
        avro_generic_value_new(clickbench_class, &clickbench);
        int rval;
        rval = avro_file_reader_read_value(dbreader, &clickbench);
        if (rval == 0) {
            bench_data.put_row(clickbench);
            /* We no longer need this memory */
            avro_value_iface_decref(clickbench_class);
        } else {
            std::cout << "read avro value error" << std::endl;
            exit(3);
        }
    }
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "Have read " << bench_data.get_rows() << std::endl;
    std::cout << "FIle IO: " << diff.count() << std::endl;

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

    AvroScannerBench scanner_bench;
    auto scanner = scanner_bench.create_avro_scanner(types, ranges, {"CounterID", "EventDate", "UserID", "EventTime", "WatchID", "JavaEnable", "Title", "GoodEvent", "ClientIP", "RegionID", "CounterClass", "OS", "UserAgent", "URL", "Referer", "IsRefresh", "RefererCategoryID", "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth", "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor", "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor", "UserAgentMinor", "CookieEnable", "JavascriptEnable", "IsMobile", "MobilePhone", "MobilePhoneModel", "Params", "IPNetworkID", "TraficSourceID", "SearchEngineID", "SearchPhrase", "AdvEngineID", "IsArtifical", "WindowClientWidth", "WindowClientHeight", "ClientTimeZone", "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2", "SilverlightVersion3", "SilverlightVersion4", "PageCharset", "CodeVersion", "IsLink", "IsDownload", "IsNotBounce", "FUniqID", "OriginalURL", "HID", "IsOldCounter", "IsEvent", "IsParameter", "DontCountHits", "WithHash", "HitColor", "LocalEventTime", "Age", "Sex", "Income", "Interests", "Robotness", "RemoteIP", "WindowName", "OpenerName", "HistoryLength", "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction", "HTTPError", "SendTiming", "DNSTiming", "ConnectTiming", "ResponseStartTiming", "ResponseEndTiming", "FetchTiming", "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID", "ParamCurrency", "ParamCurrencyID", "OpenstatServiceName", "OpenstatCampaignID", "OpenstatAdID", "OpenstatSourceID", "UTMSource", "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm", "FromTag", "HasGCLID", "RefererHash", "URLHash", "CLID"}, bench_data);
    Status st = scanner->open();
    if (!st.ok()) {
        std::cout << "Open scanner error. status: " << st.to_string();
    }
    
    // Benchmark 2: Parsing
    start = std::chrono::system_clock::now();
    int64_t parsed_count = 0;
    while (true) {
        auto ret = scanner->get_next();
        if (!ret.status().ok()) {
            break;
        }
        parsed_count += ret.value()->num_rows();
    }

    end = std::chrono::system_clock::now();
    diff = end - start;
    std::cout << "Have parsed " << parsed_count << " records" << std::endl;
    std::cout << "Parsing: " << diff.count() << std::endl;

    // if (!st.ok() && !st.is_end_of_file()) {
    //     std::cout << "Scanner get all error. status: " << st.to_string();

} // namespace starrocks
