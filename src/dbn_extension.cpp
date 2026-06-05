#define DUCKDB_EXTENSION_MAIN

#include "dbn_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

#include <memory>
#include <utility>

#include "databento/record.hpp"
#include "dbn_native_decoder.hpp"

namespace duckdb {

struct ReadDbnBindData : public TableFunctionData {
	std::string file_path;
};

struct ReadDbnGlobalState : public GlobalTableFunctionState {
	explicit ReadDbnGlobalState(const std::string &path)
	    : reader(std::make_unique<duckdb_dbn::DbnFileReader>(path)) {}
	std::unique_ptr<duckdb_dbn::DbnFileReader> reader;
};

static unique_ptr<FunctionData> ReadDbnBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind = make_uniq<ReadDbnBindData>();
	bind->file_path = input.inputs[0].GetValue<string>();

	names = {"ts_event",      "ts_recv", "instrument_id", "publisher_id", "price", "size",
	         "action",        "side",    "flags",         "depth",        "ts_in_delta", "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::UTINYINT,
	                LogicalType::UTINYINT,     LogicalType::INTEGER,      LogicalType::UINTEGER};
	return std::move(bind);
}

static unique_ptr<GlobalTableFunctionState> ReadDbnInitGlobal(ClientContext &context,
                                                              TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<ReadDbnBindData>();
	auto state = make_uniq<ReadDbnGlobalState>(bind.file_path);
	return std::move(state);
}

static void ReadDbnFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<ReadDbnGlobalState>();

	auto ts_event_data = FlatVector::GetData<int64_t>(output.data[0]);
	auto ts_recv_data = FlatVector::GetData<int64_t>(output.data[1]);
	auto instrument_id_data = FlatVector::GetData<uint32_t>(output.data[2]);
	auto publisher_id_data = FlatVector::GetData<uint16_t>(output.data[3]);
	auto price_data = FlatVector::GetData<double>(output.data[4]);
	auto size_data = FlatVector::GetData<uint32_t>(output.data[5]);
	auto action_data = FlatVector::GetData<string_t>(output.data[6]);
	auto side_data = FlatVector::GetData<string_t>(output.data[7]);
	auto flags_data = FlatVector::GetData<uint8_t>(output.data[8]);
	auto depth_data = FlatVector::GetData<uint8_t>(output.data[9]);
	auto ts_in_delta_data = FlatVector::GetData<int32_t>(output.data[10]);
	auto sequence_data = FlatVector::GetData<uint32_t>(output.data[11]);

	idx_t cardinality = 0;
	databento::TradeMsg trade {};
	while (cardinality < STANDARD_VECTOR_SIZE) {
		if (!state.reader->NextTrade(trade)) {
			break;
		}

		ts_event_data[cardinality] = static_cast<int64_t>(trade.hd.ts_event.time_since_epoch().count());
		ts_recv_data[cardinality] = static_cast<int64_t>(trade.ts_recv.time_since_epoch().count());
		instrument_id_data[cardinality] = trade.hd.instrument_id;
		publisher_id_data[cardinality] = trade.hd.publisher_id;
		price_data[cardinality] = static_cast<double>(trade.price) * 1e-9;
		size_data[cardinality] = trade.size;

		char action_c = static_cast<char>(trade.action);
		char side_c = static_cast<char>(trade.side);
		action_data[cardinality] = StringVector::AddString(output.data[6], &action_c, 1);
		side_data[cardinality] = StringVector::AddString(output.data[7], &side_c, 1);

		flags_data[cardinality] = trade.flags.Raw();
		depth_data[cardinality] = trade.depth;
		ts_in_delta_data[cardinality] = static_cast<int32_t>(trade.ts_in_delta.count());
		sequence_data[cardinality] = trade.sequence;
		++cardinality;
	}
	output.SetCardinality(cardinality);
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction read_dbn("read_dbn", {LogicalType::VARCHAR}, ReadDbnFunction, ReadDbnBind, ReadDbnInitGlobal);
	loader.RegisterFunction(read_dbn);
}

void DbnExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DbnExtension::Name() {
	return "dbn";
}

std::string DbnExtension::Version() const {
#ifdef EXT_VERSION_DBN
	return EXT_VERSION_DBN;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(dbn, loader) {
	duckdb::LoadInternal(loader);
}
}
