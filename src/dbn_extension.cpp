#define DUCKDB_EXTENSION_MAIN

#include "dbn_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <memory>
#include <utility>

#include "databento/enums.hpp"
#include "databento/record.hpp"
#include "databento/v1.hpp"
#include "databento/v2.hpp"
#include "dbn_emit_helpers.hpp"
#include "dbn_native_decoder.hpp"

namespace duckdb {

// ─────────────────────────────────────────────────────────────────────────────
// Shared bind data and global state. Every per-schema table function uses
// these — only the column list and scan loop differ per schema.
// ─────────────────────────────────────────────────────────────────────────────

struct ReadDbnBindData : public TableFunctionData {
	// Resolved at bind time. Always at least one entry; multiple entries
	// when the user passed a glob pattern.
	std::vector<std::string> file_paths;
};

struct ReadDbnGlobalState : public GlobalTableFunctionState {
	explicit ReadDbnGlobalState(std::vector<std::string> paths)
	    : reader(std::make_unique<duckdb_dbn::DbnFileReader>(std::move(paths))) {}
	std::unique_ptr<duckdb_dbn::DbnFileReader> reader;
};

static unique_ptr<GlobalTableFunctionState> ReadDbnInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<ReadDbnBindData>();
	return make_uniq<ReadDbnGlobalState>(bd.file_paths);
}

static std::string GetFilePathArg(TableFunctionBindInput &input) {
	return input.inputs[0].GetValue<string>();
}

// Expand a glob (or literal path) into a list of files. Throws if no file
// matches the pattern.
static std::vector<std::string> ExpandPaths(ClientContext &context, const std::string &pattern) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto opens = fs.Glob(pattern);
	std::vector<std::string> paths;
	paths.reserve(opens.size());
	for (const auto &of : opens) {
		paths.push_back(of.path);
	}
	if (paths.empty()) {
		throw IOException("dbn: no files match: " + pattern);
	}
	return paths;
}

// ─────────────────────────────────────────────────────────────────────────────
// Shorthand for the per-row int64 timestamp extraction.
// ─────────────────────────────────────────────────────────────────────────────

static inline int64_t TsToInt64(databento::UnixNanos t) {
	return static_cast<int64_t>(t.time_since_epoch().count());
}

// ═════════════════════════════════════════════════════════════════════════════
// trades — RType::Mbp0, TradeMsg (48 bytes)
// Mirrors Julia trades_to_dataframe ordering.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> TradesBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event", "ts_recv", "instrument_id", "publisher_id", "price",       "size",
	         "action",   "side",    "flags",         "depth",        "ts_in_delta", "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::UTINYINT,
	                LogicalType::UTINYINT,     LogicalType::INTEGER,      LogicalType::UINTEGER};
	return std::move(bd);
}

static void TradesScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto action_v = FlatVector::GetData<string_t>(out.data[6]);
	auto side_v = FlatVector::GetData<string_t>(out.data[7]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[8]);
	auto depth = FlatVector::GetData<uint8_t>(out.data[9]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[10]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[11]);

	databento::TradeMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbp0)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		depth[n] = rec.depth;
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbo — RType::Mbo, MboMsg (56 bytes)
// Mirrors Julia mbo_to_dataframe ordering.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> MboBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event", "ts_recv", "instrument_id", "publisher_id", "order_id",   "price",      "size",
	         "flags",    "channel_id", "action",     "side",         "ts_in_delta", "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::UBIGINT,      LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UTINYINT,     LogicalType::UTINYINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::INTEGER,
	                LogicalType::UINTEGER};
	return std::move(bd);
}

static void MboScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto order_id = FlatVector::GetData<uint64_t>(out.data[4]);
	auto price = FlatVector::GetData<double>(out.data[5]);
	auto size = FlatVector::GetData<uint32_t>(out.data[6]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[7]);
	auto chan = FlatVector::GetData<uint8_t>(out.data[8]);
	auto action_v = FlatVector::GetData<string_t>(out.data[9]);
	auto side_v = FlatVector::GetData<string_t>(out.data[10]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[11]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[12]);

	databento::MboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbo)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		order_id[n] = rec.order_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		flags[n] = rec.flags.Raw();
		chan[n] = rec.channel_id;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[9], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[10], static_cast<char>(rec.side));
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbp-1 — RType::Mbp1, Mbp1Msg (80 bytes)
// Flattens levels[0] into bid_price/ask_price/bid_size/ask_size/bid_ct/ask_ct.
// Mirrors Julia mbp1_to_dataframe (action/side last).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> Mbp1Bind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event",  "ts_recv",  "instrument_id", "publisher_id", "bid_price",
	         "ask_price", "bid_size", "ask_size",      "bid_ct",       "ask_ct",
	         "flags",     "ts_in_delta", "sequence",   "action",       "side"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::UINTEGER,     LogicalType::UTINYINT,     LogicalType::INTEGER,
	                LogicalType::UINTEGER,     LogicalType::VARCHAR,      LogicalType::VARCHAR};
	return std::move(bd);
}

static void Mbp1ScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto bid_px = FlatVector::GetData<double>(out.data[4]);
	auto ask_px = FlatVector::GetData<double>(out.data[5]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[6]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[7]);
	auto bid_ct = FlatVector::GetData<uint32_t>(out.data[8]);
	auto ask_ct = FlatVector::GetData<uint32_t>(out.data[9]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[10]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[11]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[12]);
	auto action_v = FlatVector::GetData<string_t>(out.data[13]);
	auto side_v = FlatVector::GetData<string_t>(out.data[14]);

	databento::Mbp1Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_ct[n] = L.bid_ct;
		ask_ct[n] = L.ask_ct;
		flags[n] = rec.flags.Raw();
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[13], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[14], static_cast<char>(rec.side));
		++n;
	}
	out.SetCardinality(n);
}

static void Mbp1Scan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Mbp1ScanImpl(c, input, out, databento::RType::Mbp1);
}

// tbbo shares Mbp1Msg layout + RType::Mbp1; the only on-wire difference is
// metadata.schema, which Phase 2 does not parse. read_dbn_tbbo and
// read_dbn_mbp1 therefore return identical data on either fixture; Phase 3
// will gate them via metadata.schema once we parse it.
static void TbboScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Mbp1ScanImpl(c, input, out, databento::RType::Mbp1);
}

static unique_ptr<FunctionData> TbboBind(ClientContext &c, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	return Mbp1Bind(c, input, return_types, names);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbp-10 — RType::Mbp10, Mbp10Msg (368 bytes)
// 12 base columns + 10 levels × 6 fields flattened.
// ═════════════════════════════════════════════════════════════════════════════

static std::string LevelCol(const char *prefix, int i) {
	char buf[16];
	std::snprintf(buf, sizeof(buf), "%s_%02d", prefix, i);
	return std::string(buf);
}

static unique_ptr<FunctionData> Mbp10Bind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event",    "ts_recv", "instrument_id", "publisher_id", "price", "size",
	         "action",      "side",    "flags",         "depth",        "ts_in_delta", "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::UTINYINT,
	                LogicalType::UTINYINT,     LogicalType::INTEGER,      LogicalType::UINTEGER};
	for (int i = 0; i < 10; ++i) {
		names.push_back(LevelCol("bid_px", i));
		return_types.push_back(LogicalType::DOUBLE);
		names.push_back(LevelCol("ask_px", i));
		return_types.push_back(LogicalType::DOUBLE);
		names.push_back(LevelCol("bid_sz", i));
		return_types.push_back(LogicalType::UINTEGER);
		names.push_back(LevelCol("ask_sz", i));
		return_types.push_back(LogicalType::UINTEGER);
		names.push_back(LevelCol("bid_ct", i));
		return_types.push_back(LogicalType::UINTEGER);
		names.push_back(LevelCol("ask_ct", i));
		return_types.push_back(LogicalType::UINTEGER);
	}
	return std::move(bd);
}

static void Mbp10Scan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto action_v = FlatVector::GetData<string_t>(out.data[6]);
	auto side_v = FlatVector::GetData<string_t>(out.data[7]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[8]);
	auto depth = FlatVector::GetData<uint8_t>(out.data[9]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[10]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[11]);

	double *bid_px[10], *ask_px[10];
	uint32_t *bid_sz[10], *ask_sz[10], *bid_ct[10], *ask_ct[10];
	for (int i = 0; i < 10; ++i) {
		const idx_t base = 12 + i * 6;
		bid_px[i] = FlatVector::GetData<double>(out.data[base + 0]);
		ask_px[i] = FlatVector::GetData<double>(out.data[base + 1]);
		bid_sz[i] = FlatVector::GetData<uint32_t>(out.data[base + 2]);
		ask_sz[i] = FlatVector::GetData<uint32_t>(out.data[base + 3]);
		bid_ct[i] = FlatVector::GetData<uint32_t>(out.data[base + 4]);
		ask_ct[i] = FlatVector::GetData<uint32_t>(out.data[base + 5]);
	}

	databento::Mbp10Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbp10)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		depth[n] = rec.depth;
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		for (int i = 0; i < 10; ++i) {
			const auto &L = rec.levels[i];
			bid_px[i][n] = static_cast<double>(L.bid_px) * 1e-9;
			ask_px[i][n] = static_cast<double>(L.ask_px) * 1e-9;
			bid_sz[i][n] = L.bid_sz;
			ask_sz[i][n] = L.ask_sz;
			bid_ct[i][n] = L.bid_ct;
			ask_ct[i][n] = L.ask_ct;
		}
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// bbo-1s / bbo-1m — BboMsg (80 bytes), RType::Bbo1S / Bbo1M.
// No action/depth/ts_in_delta. levels[0] flattened.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> BboBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event",  "ts_recv",  "instrument_id", "publisher_id", "price",
	         "size",      "side",     "flags",         "bid_price",    "ask_price",
	         "bid_size",  "ask_size", "bid_ct",        "ask_ct",       "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::UTINYINT,     LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::UINTEGER};
	return std::move(bd);
}

static void BboScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto side_v = FlatVector::GetData<string_t>(out.data[6]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[7]);
	auto bid_px = FlatVector::GetData<double>(out.data[8]);
	auto ask_px = FlatVector::GetData<double>(out.data[9]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[10]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[11]);
	auto bid_ct = FlatVector::GetData<uint32_t>(out.data[12]);
	auto ask_ct = FlatVector::GetData<uint32_t>(out.data[13]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[14]);

	databento::BboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		side_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_ct[n] = L.bid_ct;
		ask_ct[n] = L.ask_ct;
		seq[n] = rec.sequence;
		++n;
	}
	out.SetCardinality(n);
}

static void Bbo1sScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	BboScanImpl(c, input, out, databento::RType::Bbo1S);
}
static void Bbo1mScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	BboScanImpl(c, input, out, databento::RType::Bbo1M);
}

// ═════════════════════════════════════════════════════════════════════════════
// cbbo-1s — CbboMsg (80 bytes), RType::Cbbo1S.
// Uses ConsolidatedBidAskPair: bid_pb/ask_pb (publisher bitmask) instead of bid_ct/ask_ct.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> CbboBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event", "ts_recv",   "instrument_id", "publisher_id", "price",    "size",   "side",   "flags",
	         "bid_price","ask_price", "bid_size",      "ask_size",     "bid_pb",   "ask_pb"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::UTINYINT,     LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::USMALLINT};
	return std::move(bd);
}

static void CbboScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto side_v = FlatVector::GetData<string_t>(out.data[6]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[7]);
	auto bid_px = FlatVector::GetData<double>(out.data[8]);
	auto ask_px = FlatVector::GetData<double>(out.data[9]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[10]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[11]);
	auto bid_pb = FlatVector::GetData<uint16_t>(out.data[12]);
	auto ask_pb = FlatVector::GetData<uint16_t>(out.data[13]);

	databento::CbboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		side_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_pb[n] = L.bid_pb;
		ask_pb[n] = L.ask_pb;
		++n;
	}
	out.SetCardinality(n);
}

static void Cbbo1sScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	CbboScanImpl(c, input, out, databento::RType::Cbbo1S);
}
static void Cbbo1mScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	CbboScanImpl(c, input, out, databento::RType::Cbbo1M);
}

// ═════════════════════════════════════════════════════════════════════════════
// cmbp-1 — Cmbp1Msg (80 bytes), RType::Cmbp1.
// Like Mbp1 but with ConsolidatedBidAskPair (bid_pb/ask_pb).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> Cmbp1Bind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event",  "ts_recv",  "instrument_id", "publisher_id", "price",
	         "size",      "action",   "side",          "flags",        "ts_in_delta",
	         "bid_price", "ask_price","bid_size",      "ask_size",     "bid_pb",
	         "ask_pb"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::UTINYINT,
	                LogicalType::INTEGER,      LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::USMALLINT,
	                LogicalType::USMALLINT};
	return std::move(bd);
}

static void Cmbp1ScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto action_v = FlatVector::GetData<string_t>(out.data[6]);
	auto side_v = FlatVector::GetData<string_t>(out.data[7]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[8]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[9]);
	auto bid_px = FlatVector::GetData<double>(out.data[10]);
	auto ask_px = FlatVector::GetData<double>(out.data[11]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[12]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[13]);
	auto bid_pb = FlatVector::GetData<uint16_t>(out.data[14]);
	auto ask_pb = FlatVector::GetData<uint16_t>(out.data[15]);

	databento::Cmbp1Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_pb[n] = L.bid_pb;
		ask_pb[n] = L.ask_pb;
		++n;
	}
	out.SetCardinality(n);
}

static void Cmbp1Scan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Cmbp1ScanImpl(c, input, out, databento::RType::Cmbp1);
}
static void TcbboScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Cmbp1ScanImpl(c, input, out, databento::RType::Tcbbo);
}

// ═════════════════════════════════════════════════════════════════════════════
// ohlcv-{1s,1m,1h,1d} — OhlcvMsg (56 bytes), RType::Ohlcv1{S,M,H,D}.
// 8 columns: ts_event, instrument_id, publisher_id, open, high, low, close, volume.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> OhlcvBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event", "instrument_id", "publisher_id", "open", "high", "low", "close", "volume"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER, LogicalType::USMALLINT, LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::DOUBLE,   LogicalType::DOUBLE,    LogicalType::UBIGINT};
	return std::move(bd);
}

static void OhlcvScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[1]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[2]);
	auto open_ = FlatVector::GetData<double>(out.data[3]);
	auto high = FlatVector::GetData<double>(out.data[4]);
	auto low = FlatVector::GetData<double>(out.data[5]);
	auto close = FlatVector::GetData<double>(out.data[6]);
	auto volume = FlatVector::GetData<uint64_t>(out.data[7]);

	databento::OhlcvMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		open_[n] = static_cast<double>(rec.open) * 1e-9;
		high[n] = static_cast<double>(rec.high) * 1e-9;
		low[n] = static_cast<double>(rec.low) * 1e-9;
		close[n] = static_cast<double>(rec.close) * 1e-9;
		volume[n] = rec.volume;
		++n;
	}
	out.SetCardinality(n);
}

static void Ohlcv1sScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1S);
}
static void Ohlcv1mScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1M);
}
static void Ohlcv1hScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1H);
}
static void Ohlcv1dScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1D);
}
static void OhlcvEodScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::OhlcvEod);
}

// ═════════════════════════════════════════════════════════════════════════════
// status — StatusMsg (40 bytes), RType::Status.
// action/reason/trading_event as USMALLINT raw enums (forward-compat).
// is_trading/is_quoting/is_short_sell_restricted as 1-char VARCHAR (TriState).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> StatusBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event",       "ts_recv",        "instrument_id",            "publisher_id", "action",
	         "reason",         "trading_event",  "is_trading",               "is_quoting",   "is_short_sell_restricted"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::USMALLINT,    LogicalType::USMALLINT,
	                LogicalType::USMALLINT,    LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::VARCHAR};
	return std::move(bd);
}

static void StatusScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto action = FlatVector::GetData<uint16_t>(out.data[4]);
	auto reason = FlatVector::GetData<uint16_t>(out.data[5]);
	auto trading_event = FlatVector::GetData<uint16_t>(out.data[6]);
	auto is_trading = FlatVector::GetData<string_t>(out.data[7]);
	auto is_quoting = FlatVector::GetData<string_t>(out.data[8]);
	auto is_ssr = FlatVector::GetData<string_t>(out.data[9]);

	databento::StatusMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Status)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		action[n] = static_cast<uint16_t>(rec.action);
		reason[n] = static_cast<uint16_t>(rec.reason);
		trading_event[n] = static_cast<uint16_t>(rec.trading_event);
		is_trading[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.is_trading));
		is_quoting[n] = duckdb_dbn::EmitChar1(out.data[8], static_cast<char>(rec.is_quoting));
		is_ssr[n] = duckdb_dbn::EmitChar1(out.data[9], static_cast<char>(rec.is_short_sell_restricted));
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// imbalance — ImbalanceMsg (112 bytes), RType::Imbalance.
// 23 columns; mirrors the wire layout (Julia's mapper has known bugs).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> ImbalanceBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event",
	         "ts_recv",
	         "instrument_id",
	         "publisher_id",
	         "ref_price",
	         "auction_time",
	         "cont_book_clr_price",
	         "auct_interest_clr_price",
	         "ssr_filling_price",
	         "ind_match_price",
	         "upper_collar",
	         "lower_collar",
	         "paired_qty",
	         "total_imbalance_qty",
	         "market_imbalance_qty",
	         "unpaired_qty",
	         "auction_type",
	         "side",
	         "auction_status",
	         "freeze_status",
	         "num_extensions",
	         "unpaired_side",
	         "significant_imbalance"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::TIMESTAMP_NS,
	                LogicalType::DOUBLE,       LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::UINTEGER,     LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::UTINYINT,     LogicalType::UTINYINT,     LogicalType::UTINYINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR};
	return std::move(bd);
}

static void ImbalanceScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto ref_px = FlatVector::GetData<double>(out.data[4]);
	auto auc_time = FlatVector::GetData<int64_t>(out.data[5]);
	auto cont_clr = FlatVector::GetData<double>(out.data[6]);
	auto auct_int_clr = FlatVector::GetData<double>(out.data[7]);
	auto ssr_fill = FlatVector::GetData<double>(out.data[8]);
	auto ind_match = FlatVector::GetData<double>(out.data[9]);
	auto upper_collar = FlatVector::GetData<double>(out.data[10]);
	auto lower_collar = FlatVector::GetData<double>(out.data[11]);
	auto paired_qty = FlatVector::GetData<uint32_t>(out.data[12]);
	auto total_imb = FlatVector::GetData<uint32_t>(out.data[13]);
	auto mkt_imb = FlatVector::GetData<uint32_t>(out.data[14]);
	auto unpaired_qty = FlatVector::GetData<uint32_t>(out.data[15]);
	auto auc_type = FlatVector::GetData<string_t>(out.data[16]);
	auto side_v = FlatVector::GetData<string_t>(out.data[17]);
	auto auc_status = FlatVector::GetData<uint8_t>(out.data[18]);
	auto freeze = FlatVector::GetData<uint8_t>(out.data[19]);
	auto num_ext = FlatVector::GetData<uint8_t>(out.data[20]);
	auto unp_side = FlatVector::GetData<string_t>(out.data[21]);
	auto sig_imb = FlatVector::GetData<string_t>(out.data[22]);

	databento::ImbalanceMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Imbalance)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		ref_px[n] = static_cast<double>(rec.ref_price) * 1e-9;
		auc_time[n] = TsToInt64(rec.auction_time);
		cont_clr[n] = static_cast<double>(rec.cont_book_clr_price) * 1e-9;
		auct_int_clr[n] = static_cast<double>(rec.auct_interest_clr_price) * 1e-9;
		ssr_fill[n] = static_cast<double>(rec.ssr_filling_price) * 1e-9;
		ind_match[n] = static_cast<double>(rec.ind_match_price) * 1e-9;
		upper_collar[n] = static_cast<double>(rec.upper_collar) * 1e-9;
		lower_collar[n] = static_cast<double>(rec.lower_collar) * 1e-9;
		paired_qty[n] = rec.paired_qty;
		total_imb[n] = rec.total_imbalance_qty;
		mkt_imb[n] = rec.market_imbalance_qty;
		unpaired_qty[n] = rec.unpaired_qty;
		auc_type[n] = duckdb_dbn::EmitChar1(out.data[16], rec.auction_type);
		side_v[n] = duckdb_dbn::EmitChar1(out.data[17], static_cast<char>(rec.side));
		auc_status[n] = rec.auction_status;
		freeze[n] = rec.freeze_status;
		num_ext[n] = rec.num_extensions;
		unp_side[n] = duckdb_dbn::EmitChar1(out.data[21], static_cast<char>(rec.unpaired_side));
		sig_imb[n] = duckdb_dbn::EmitChar1(out.data[22], rec.significant_imbalance);
		++n;
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// statistics — RType::Statistics, StatMsg.
// Version-aware: v1/v2 StatMsg is 64 bytes with int32 quantity; v3 is 80 bytes
// with int64 quantity. Canonical exposed type is BIGINT — v1/v2 quantity is
// sign-extended, with the kUndefStatQuantityV1 sentinel (INT32_MAX) mapped
// to the v3 sentinel value (INT64_MAX).
// ════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> StatisticsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event",   "ts_recv",       "ts_ref",      "instrument_id", "publisher_id",
	         "price",      "quantity",      "sequence",    "ts_in_delta",   "stat_type",
	         "channel_id", "update_action", "stat_flags"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS,
	                LogicalType::UINTEGER,     LogicalType::USMALLINT,    LogicalType::DOUBLE,
	                LogicalType::BIGINT,       LogicalType::UINTEGER,     LogicalType::INTEGER,
	                LogicalType::USMALLINT,    LogicalType::USMALLINT,    LogicalType::UTINYINT,
	                LogicalType::UTINYINT};
	return std::move(bd);
}

static void StatisticsScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto ts_ref = FlatVector::GetData<int64_t>(out.data[2]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[3]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[4]);
	auto price = FlatVector::GetData<double>(out.data[5]);
	auto quantity = FlatVector::GetData<int64_t>(out.data[6]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[7]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[8]);
	auto stat_type = FlatVector::GetData<uint16_t>(out.data[9]);
	auto chan = FlatVector::GetData<uint16_t>(out.data[10]);
	auto update_action = FlatVector::GetData<uint8_t>(out.data[11]);
	auto stat_flags = FlatVector::GetData<uint8_t>(out.data[12]);

	const auto version = st.reader->Version();
	const bool use_v1 = (version == 1 || version == 2);

	idx_t n = 0;
	if (use_v1) {
		databento::v1::StatMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Statistics)) {
			ts_event[n] = TsToInt64(rec.hd.ts_event);
			ts_recv[n] = TsToInt64(rec.ts_recv);
			ts_ref[n] = TsToInt64(rec.ts_ref);
			instr[n] = rec.hd.instrument_id;
			pub[n] = rec.hd.publisher_id;
			price[n] = static_cast<double>(rec.price) * 1e-9;
			// Sign-extend quantity, mapping the v1 UNDEF sentinel (INT32_MAX) to
			// the v3 sentinel (INT64_MAX) so consumers can treat the column uniformly.
			if (rec.quantity == std::numeric_limits<std::int32_t>::max()) {
				quantity[n] = std::numeric_limits<std::int64_t>::max();
			} else {
				quantity[n] = static_cast<int64_t>(rec.quantity);
			}
			seq[n] = rec.sequence;
			ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
			stat_type[n] = static_cast<uint16_t>(rec.stat_type);
			chan[n] = rec.channel_id;
			update_action[n] = static_cast<uint8_t>(rec.update_action);
			stat_flags[n] = rec.stat_flags;
			++n;
		}
	} else {
		databento::StatMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Statistics)) {
			ts_event[n] = TsToInt64(rec.hd.ts_event);
			ts_recv[n] = TsToInt64(rec.ts_recv);
			ts_ref[n] = TsToInt64(rec.ts_ref);
			instr[n] = rec.hd.instrument_id;
			pub[n] = rec.hd.publisher_id;
			price[n] = static_cast<double>(rec.price) * 1e-9;
			quantity[n] = rec.quantity;
			seq[n] = rec.sequence;
			ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
			stat_type[n] = static_cast<uint16_t>(rec.stat_type);
			chan[n] = rec.channel_id;
			update_action[n] = static_cast<uint8_t>(rec.update_action);
			stat_flags[n] = rec.stat_flags;
			++n;
		}
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// definition — RType::InstrumentDef, InstrumentDefMsg.
// Version-aware: v1 = 360 bytes, v2 = 400 bytes, v3 = 520 bytes. Canonical
// column layout is the union of all fields seen across versions. Fields not
// present in the source file's DBN version are emitted as SQL NULL — the
// 8 leg_* fields and a few v3 additions are NULL for v1/v2 sources; the
// 4 deprecated fields (trading_reference_price, trading_reference_date,
// md_security_trading_status, settl_price_type) are NULL for v3 sources.
// ════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> DefinitionBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event", "ts_recv", "instrument_id", "publisher_id",
	         "raw_symbol", "instrument_class", "security_type", "exchange",
	         "asset", "cfi", "currency", "settl_currency",
	         "secsubtype", "group", "underlying", "strike_price_currency",
	         "unit_of_measure", "expiration", "activation", "min_price_increment",
	         "display_factor", "high_limit_price", "low_limit_price", "max_price_variation",
	         "strike_price", "unit_of_measure_qty", "min_price_increment_amount", "price_ratio",
	         "raw_instrument_id", "underlying_id", "inst_attrib_value", "market_depth_implied",
	         "market_depth", "market_segment_id", "max_trade_vol", "min_lot_size",
	         "min_lot_size_block", "min_lot_size_round_lot", "min_trade_vol", "contract_multiplier",
	         "decay_quantity", "original_contract_size", "appl_id", "maturity_year",
	         "decay_start_date", "channel_id", "match_algorithm", "main_fraction",
	         "price_display_format", "sub_fraction", "underlying_product", "security_update_action",
	         "maturity_month", "maturity_day", "maturity_week", "user_defined_instrument",
	         "contract_multiplier_unit", "flow_schedule_type", "tick_rule", "leg_count",
	         "leg_index", "leg_instrument_id", "leg_raw_symbol", "leg_instrument_class",
	         "leg_side", "leg_price", "leg_delta", "leg_ratio_price_numerator",
	         "leg_ratio_price_denominator", "leg_ratio_qty_numerator", "leg_ratio_qty_denominator", "leg_underlying_id",
	         "trading_reference_price", "trading_reference_date", "md_security_trading_status", "settl_price_type"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP_NS,
	                LogicalType::TIMESTAMP_NS, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::UBIGINT, LogicalType::UINTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::INTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::UINTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::SMALLINT, LogicalType::USMALLINT, LogicalType::USMALLINT,
	                LogicalType::USMALLINT, LogicalType::VARCHAR, LogicalType::UTINYINT,
	                LogicalType::UTINYINT, LogicalType::UTINYINT, LogicalType::UTINYINT,
	                LogicalType::VARCHAR, LogicalType::UTINYINT, LogicalType::UTINYINT,
	                LogicalType::UTINYINT, LogicalType::VARCHAR, LogicalType::TINYINT,
	                LogicalType::TINYINT, LogicalType::UTINYINT, LogicalType::USMALLINT,
	                LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::UINTEGER,
	                LogicalType::DOUBLE, LogicalType::USMALLINT, LogicalType::UTINYINT,
	                LogicalType::UTINYINT};
	return std::move(bd);
}

static void DefinitionScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto version = st.reader->Version();
	idx_t n = 0;
	if (version >= 3) {
		databento::InstrumentDefMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::InstrumentDef)) {
			FlatVector::GetData<int64_t>(out.data[0])[n] = static_cast<int64_t>((rec.hd.ts_event).time_since_epoch().count());
			FlatVector::GetData<int64_t>(out.data[1])[n] = static_cast<int64_t>((rec.ts_recv).time_since_epoch().count());
			FlatVector::GetData<uint32_t>(out.data[2])[n] = rec.hd.instrument_id;
			FlatVector::GetData<uint16_t>(out.data[3])[n] = rec.hd.publisher_id;
			FlatVector::GetData<string_t>(out.data[4])[n] = duckdb_dbn::EmitCstr(out.data[4], rec.RawSymbol(), 64);
			FlatVector::GetData<string_t>(out.data[5])[n] = duckdb_dbn::EmitChar1(out.data[5], static_cast<char>(rec.instrument_class));
			FlatVector::GetData<string_t>(out.data[6])[n] = duckdb_dbn::EmitCstr(out.data[6], rec.SecurityType(), 7);
			FlatVector::GetData<string_t>(out.data[7])[n] = duckdb_dbn::EmitCstr(out.data[7], rec.Exchange(), 5);
			FlatVector::GetData<string_t>(out.data[8])[n] = duckdb_dbn::EmitCstr(out.data[8], rec.Asset(), 11);
			FlatVector::GetData<string_t>(out.data[9])[n] = duckdb_dbn::EmitCstr(out.data[9], rec.Cfi(), 7);
			FlatVector::GetData<string_t>(out.data[10])[n] = duckdb_dbn::EmitCstr(out.data[10], rec.Currency(), 4);
			FlatVector::GetData<string_t>(out.data[11])[n] = duckdb_dbn::EmitCstr(out.data[11], rec.SettlCurrency(), 4);
			FlatVector::GetData<string_t>(out.data[12])[n] = duckdb_dbn::EmitCstr(out.data[12], rec.SecSubType(), 6);
			FlatVector::GetData<string_t>(out.data[13])[n] = duckdb_dbn::EmitCstr(out.data[13], rec.Group(), 21);
			FlatVector::GetData<string_t>(out.data[14])[n] = duckdb_dbn::EmitCstr(out.data[14], rec.Underlying(), 21);
			FlatVector::GetData<string_t>(out.data[15])[n] = duckdb_dbn::EmitCstr(out.data[15], rec.StrikePriceCurrency(), 4);
			FlatVector::GetData<string_t>(out.data[16])[n] = duckdb_dbn::EmitCstr(out.data[16], rec.UnitOfMeasure(), 31);
			FlatVector::GetData<int64_t>(out.data[17])[n] = static_cast<int64_t>((rec.expiration).time_since_epoch().count());
			FlatVector::GetData<int64_t>(out.data[18])[n] = static_cast<int64_t>((rec.activation).time_since_epoch().count());
			FlatVector::GetData<double>(out.data[19])[n] = static_cast<double>(rec.min_price_increment) * 1e-9;
			FlatVector::GetData<double>(out.data[20])[n] = static_cast<double>(rec.display_factor) * 1e-9;
			FlatVector::GetData<double>(out.data[21])[n] = static_cast<double>(rec.high_limit_price) * 1e-9;
			FlatVector::GetData<double>(out.data[22])[n] = static_cast<double>(rec.low_limit_price) * 1e-9;
			FlatVector::GetData<double>(out.data[23])[n] = static_cast<double>(rec.max_price_variation) * 1e-9;
			FlatVector::GetData<double>(out.data[24])[n] = static_cast<double>(rec.strike_price) * 1e-9;
			FlatVector::GetData<double>(out.data[25])[n] = static_cast<double>(rec.unit_of_measure_qty) * 1e-9;
			FlatVector::GetData<double>(out.data[26])[n] = static_cast<double>(rec.min_price_increment_amount) * 1e-9;
			FlatVector::GetData<double>(out.data[27])[n] = static_cast<double>(rec.price_ratio) * 1e-9;
			FlatVector::GetData<uint64_t>(out.data[28])[n] = rec.raw_instrument_id;
			FlatVector::GetData<uint32_t>(out.data[29])[n] = rec.underlying_id;
			FlatVector::GetData<int32_t>(out.data[30])[n] = rec.inst_attrib_value;
			FlatVector::GetData<int32_t>(out.data[31])[n] = rec.market_depth_implied;
			FlatVector::GetData<int32_t>(out.data[32])[n] = rec.market_depth;
			FlatVector::GetData<uint32_t>(out.data[33])[n] = rec.market_segment_id;
			FlatVector::GetData<uint32_t>(out.data[34])[n] = rec.max_trade_vol;
			FlatVector::GetData<int32_t>(out.data[35])[n] = rec.min_lot_size;
			FlatVector::GetData<int32_t>(out.data[36])[n] = rec.min_lot_size_block;
			FlatVector::GetData<int32_t>(out.data[37])[n] = rec.min_lot_size_round_lot;
			FlatVector::GetData<uint32_t>(out.data[38])[n] = rec.min_trade_vol;
			FlatVector::GetData<int32_t>(out.data[39])[n] = rec.contract_multiplier;
			FlatVector::GetData<int32_t>(out.data[40])[n] = rec.decay_quantity;
			FlatVector::GetData<int32_t>(out.data[41])[n] = rec.original_contract_size;
			FlatVector::GetData<int16_t>(out.data[42])[n] = rec.appl_id;
			FlatVector::GetData<uint16_t>(out.data[43])[n] = rec.maturity_year;
			FlatVector::GetData<uint16_t>(out.data[44])[n] = rec.decay_start_date;
			FlatVector::GetData<uint16_t>(out.data[45])[n] = rec.channel_id;
			FlatVector::GetData<string_t>(out.data[46])[n] = duckdb_dbn::EmitChar1(out.data[46], static_cast<char>(rec.match_algorithm));
			FlatVector::GetData<uint8_t>(out.data[47])[n] = rec.main_fraction;
			FlatVector::GetData<uint8_t>(out.data[48])[n] = rec.price_display_format;
			FlatVector::GetData<uint8_t>(out.data[49])[n] = rec.sub_fraction;
			FlatVector::GetData<uint8_t>(out.data[50])[n] = rec.underlying_product;
			FlatVector::GetData<string_t>(out.data[51])[n] = duckdb_dbn::EmitChar1(out.data[51], static_cast<char>(rec.security_update_action));
			FlatVector::GetData<uint8_t>(out.data[52])[n] = rec.maturity_month;
			FlatVector::GetData<uint8_t>(out.data[53])[n] = rec.maturity_day;
			FlatVector::GetData<uint8_t>(out.data[54])[n] = rec.maturity_week;
			FlatVector::GetData<string_t>(out.data[55])[n] = duckdb_dbn::EmitChar1(out.data[55], static_cast<char>(rec.user_defined_instrument));
			FlatVector::GetData<int8_t>(out.data[56])[n] = static_cast<int8_t>(rec.contract_multiplier_unit);
			FlatVector::GetData<int8_t>(out.data[57])[n] = static_cast<int8_t>(rec.flow_schedule_type);
			FlatVector::GetData<uint8_t>(out.data[58])[n] = rec.tick_rule;
			FlatVector::GetData<uint16_t>(out.data[59])[n] = rec.leg_count;
			FlatVector::GetData<uint16_t>(out.data[60])[n] = rec.leg_index;
			FlatVector::GetData<uint32_t>(out.data[61])[n] = rec.leg_instrument_id;
			FlatVector::GetData<string_t>(out.data[62])[n] = duckdb_dbn::EmitCstr(out.data[62], rec.LegRawSymbol(), 64);
			FlatVector::GetData<string_t>(out.data[63])[n] = duckdb_dbn::EmitChar1(out.data[63], static_cast<char>(rec.leg_instrument_class));
			FlatVector::GetData<string_t>(out.data[64])[n] = duckdb_dbn::EmitChar1(out.data[64], static_cast<char>(rec.leg_side));
			FlatVector::GetData<double>(out.data[65])[n] = static_cast<double>(rec.leg_price) * 1e-9;
			FlatVector::GetData<double>(out.data[66])[n] = static_cast<double>(rec.leg_delta) * 1e-9;
			FlatVector::GetData<int32_t>(out.data[67])[n] = rec.leg_ratio_price_numerator;
			FlatVector::GetData<int32_t>(out.data[68])[n] = rec.leg_ratio_price_denominator;
			FlatVector::GetData<int32_t>(out.data[69])[n] = rec.leg_ratio_qty_numerator;
			FlatVector::GetData<int32_t>(out.data[70])[n] = rec.leg_ratio_qty_denominator;
			FlatVector::GetData<uint32_t>(out.data[71])[n] = rec.leg_underlying_id;
			FlatVector::Validity(out.data[72]).SetInvalid(n);
			FlatVector::Validity(out.data[73]).SetInvalid(n);
			FlatVector::Validity(out.data[74]).SetInvalid(n);
			FlatVector::Validity(out.data[75]).SetInvalid(n);
			++n;
		}
	} else if (version == 2) {
		databento::v2::InstrumentDefMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::InstrumentDef)) {
			FlatVector::GetData<int64_t>(out.data[0])[n] = static_cast<int64_t>((rec.hd.ts_event).time_since_epoch().count());
			FlatVector::GetData<int64_t>(out.data[1])[n] = static_cast<int64_t>((rec.ts_recv).time_since_epoch().count());
			FlatVector::GetData<uint32_t>(out.data[2])[n] = rec.hd.instrument_id;
			FlatVector::GetData<uint16_t>(out.data[3])[n] = rec.hd.publisher_id;
			FlatVector::GetData<string_t>(out.data[4])[n] = duckdb_dbn::EmitCstr(out.data[4], rec.RawSymbol(), 64);
			FlatVector::GetData<string_t>(out.data[5])[n] = duckdb_dbn::EmitChar1(out.data[5], static_cast<char>(rec.instrument_class));
			FlatVector::GetData<string_t>(out.data[6])[n] = duckdb_dbn::EmitCstr(out.data[6], rec.SecurityType(), 7);
			FlatVector::GetData<string_t>(out.data[7])[n] = duckdb_dbn::EmitCstr(out.data[7], rec.Exchange(), 5);
			FlatVector::GetData<string_t>(out.data[8])[n] = duckdb_dbn::EmitCstr(out.data[8], rec.Asset(), 11);
			FlatVector::GetData<string_t>(out.data[9])[n] = duckdb_dbn::EmitCstr(out.data[9], rec.Cfi(), 7);
			FlatVector::GetData<string_t>(out.data[10])[n] = duckdb_dbn::EmitCstr(out.data[10], rec.Currency(), 4);
			FlatVector::GetData<string_t>(out.data[11])[n] = duckdb_dbn::EmitCstr(out.data[11], rec.SettlCurrency(), 4);
			FlatVector::GetData<string_t>(out.data[12])[n] = duckdb_dbn::EmitCstr(out.data[12], rec.SecSubType(), 6);
			FlatVector::GetData<string_t>(out.data[13])[n] = duckdb_dbn::EmitCstr(out.data[13], rec.Group(), 21);
			FlatVector::GetData<string_t>(out.data[14])[n] = duckdb_dbn::EmitCstr(out.data[14], rec.Underlying(), 21);
			FlatVector::GetData<string_t>(out.data[15])[n] = duckdb_dbn::EmitCstr(out.data[15], rec.StrikePriceCurrency(), 4);
			FlatVector::GetData<string_t>(out.data[16])[n] = duckdb_dbn::EmitCstr(out.data[16], rec.UnitOfMeasure(), 31);
			FlatVector::GetData<int64_t>(out.data[17])[n] = static_cast<int64_t>((rec.expiration).time_since_epoch().count());
			FlatVector::GetData<int64_t>(out.data[18])[n] = static_cast<int64_t>((rec.activation).time_since_epoch().count());
			FlatVector::GetData<double>(out.data[19])[n] = static_cast<double>(rec.min_price_increment) * 1e-9;
			FlatVector::GetData<double>(out.data[20])[n] = static_cast<double>(rec.display_factor) * 1e-9;
			FlatVector::GetData<double>(out.data[21])[n] = static_cast<double>(rec.high_limit_price) * 1e-9;
			FlatVector::GetData<double>(out.data[22])[n] = static_cast<double>(rec.low_limit_price) * 1e-9;
			FlatVector::GetData<double>(out.data[23])[n] = static_cast<double>(rec.max_price_variation) * 1e-9;
			FlatVector::GetData<double>(out.data[24])[n] = static_cast<double>(rec.strike_price) * 1e-9;
			FlatVector::GetData<double>(out.data[25])[n] = static_cast<double>(rec.unit_of_measure_qty) * 1e-9;
			FlatVector::GetData<double>(out.data[26])[n] = static_cast<double>(rec.min_price_increment_amount) * 1e-9;
			FlatVector::GetData<double>(out.data[27])[n] = static_cast<double>(rec.price_ratio) * 1e-9;
			FlatVector::GetData<uint64_t>(out.data[28])[n] = static_cast<uint64_t>(rec.raw_instrument_id);
			FlatVector::GetData<uint32_t>(out.data[29])[n] = rec.underlying_id;
			FlatVector::GetData<int32_t>(out.data[30])[n] = rec.inst_attrib_value;
			FlatVector::GetData<int32_t>(out.data[31])[n] = rec.market_depth_implied;
			FlatVector::GetData<int32_t>(out.data[32])[n] = rec.market_depth;
			FlatVector::GetData<uint32_t>(out.data[33])[n] = rec.market_segment_id;
			FlatVector::GetData<uint32_t>(out.data[34])[n] = rec.max_trade_vol;
			FlatVector::GetData<int32_t>(out.data[35])[n] = rec.min_lot_size;
			FlatVector::GetData<int32_t>(out.data[36])[n] = rec.min_lot_size_block;
			FlatVector::GetData<int32_t>(out.data[37])[n] = rec.min_lot_size_round_lot;
			FlatVector::GetData<uint32_t>(out.data[38])[n] = rec.min_trade_vol;
			FlatVector::GetData<int32_t>(out.data[39])[n] = rec.contract_multiplier;
			FlatVector::GetData<int32_t>(out.data[40])[n] = rec.decay_quantity;
			FlatVector::GetData<int32_t>(out.data[41])[n] = rec.original_contract_size;
			FlatVector::GetData<int16_t>(out.data[42])[n] = rec.appl_id;
			FlatVector::GetData<uint16_t>(out.data[43])[n] = rec.maturity_year;
			FlatVector::GetData<uint16_t>(out.data[44])[n] = rec.decay_start_date;
			FlatVector::GetData<uint16_t>(out.data[45])[n] = rec.channel_id;
			FlatVector::GetData<string_t>(out.data[46])[n] = duckdb_dbn::EmitChar1(out.data[46], static_cast<char>(rec.match_algorithm));
			FlatVector::GetData<uint8_t>(out.data[47])[n] = rec.main_fraction;
			FlatVector::GetData<uint8_t>(out.data[48])[n] = rec.price_display_format;
			FlatVector::GetData<uint8_t>(out.data[49])[n] = rec.sub_fraction;
			FlatVector::GetData<uint8_t>(out.data[50])[n] = rec.underlying_product;
			FlatVector::GetData<string_t>(out.data[51])[n] = duckdb_dbn::EmitChar1(out.data[51], static_cast<char>(rec.security_update_action));
			FlatVector::GetData<uint8_t>(out.data[52])[n] = rec.maturity_month;
			FlatVector::GetData<uint8_t>(out.data[53])[n] = rec.maturity_day;
			FlatVector::GetData<uint8_t>(out.data[54])[n] = rec.maturity_week;
			FlatVector::GetData<string_t>(out.data[55])[n] = duckdb_dbn::EmitChar1(out.data[55], static_cast<char>(rec.user_defined_instrument));
			FlatVector::GetData<int8_t>(out.data[56])[n] = static_cast<int8_t>(rec.contract_multiplier_unit);
			FlatVector::GetData<int8_t>(out.data[57])[n] = static_cast<int8_t>(rec.flow_schedule_type);
			FlatVector::GetData<uint8_t>(out.data[58])[n] = rec.tick_rule;
			FlatVector::Validity(out.data[59]).SetInvalid(n);
			FlatVector::Validity(out.data[60]).SetInvalid(n);
			FlatVector::Validity(out.data[61]).SetInvalid(n);
			FlatVector::Validity(out.data[62]).SetInvalid(n);
			FlatVector::Validity(out.data[63]).SetInvalid(n);
			FlatVector::Validity(out.data[64]).SetInvalid(n);
			FlatVector::Validity(out.data[65]).SetInvalid(n);
			FlatVector::Validity(out.data[66]).SetInvalid(n);
			FlatVector::Validity(out.data[67]).SetInvalid(n);
			FlatVector::Validity(out.data[68]).SetInvalid(n);
			FlatVector::Validity(out.data[69]).SetInvalid(n);
			FlatVector::Validity(out.data[70]).SetInvalid(n);
			FlatVector::Validity(out.data[71]).SetInvalid(n);
			FlatVector::GetData<double>(out.data[72])[n] = static_cast<double>(rec.trading_reference_price) * 1e-9;
			FlatVector::GetData<uint16_t>(out.data[73])[n] = rec.trading_reference_date;
			FlatVector::GetData<uint8_t>(out.data[74])[n] = rec.md_security_trading_status;
			FlatVector::GetData<uint8_t>(out.data[75])[n] = rec.settl_price_type;
			++n;
		}
	} else {
		databento::v1::InstrumentDefMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::InstrumentDef)) {
			FlatVector::GetData<int64_t>(out.data[0])[n] = static_cast<int64_t>((rec.hd.ts_event).time_since_epoch().count());
			FlatVector::GetData<int64_t>(out.data[1])[n] = static_cast<int64_t>((rec.ts_recv).time_since_epoch().count());
			FlatVector::GetData<uint32_t>(out.data[2])[n] = rec.hd.instrument_id;
			FlatVector::GetData<uint16_t>(out.data[3])[n] = rec.hd.publisher_id;
			FlatVector::GetData<string_t>(out.data[4])[n] = duckdb_dbn::EmitCstr(out.data[4], rec.RawSymbol(), 64);
			FlatVector::GetData<string_t>(out.data[5])[n] = duckdb_dbn::EmitChar1(out.data[5], static_cast<char>(rec.instrument_class));
			FlatVector::GetData<string_t>(out.data[6])[n] = duckdb_dbn::EmitCstr(out.data[6], rec.SecurityType(), 7);
			FlatVector::GetData<string_t>(out.data[7])[n] = duckdb_dbn::EmitCstr(out.data[7], rec.Exchange(), 5);
			FlatVector::GetData<string_t>(out.data[8])[n] = duckdb_dbn::EmitCstr(out.data[8], rec.Asset(), 11);
			FlatVector::GetData<string_t>(out.data[9])[n] = duckdb_dbn::EmitCstr(out.data[9], rec.Cfi(), 7);
			FlatVector::GetData<string_t>(out.data[10])[n] = duckdb_dbn::EmitCstr(out.data[10], rec.Currency(), 4);
			FlatVector::GetData<string_t>(out.data[11])[n] = duckdb_dbn::EmitCstr(out.data[11], rec.SettlCurrency(), 4);
			FlatVector::GetData<string_t>(out.data[12])[n] = duckdb_dbn::EmitCstr(out.data[12], rec.SecSubType(), 6);
			FlatVector::GetData<string_t>(out.data[13])[n] = duckdb_dbn::EmitCstr(out.data[13], rec.Group(), 21);
			FlatVector::GetData<string_t>(out.data[14])[n] = duckdb_dbn::EmitCstr(out.data[14], rec.Underlying(), 21);
			FlatVector::GetData<string_t>(out.data[15])[n] = duckdb_dbn::EmitCstr(out.data[15], rec.StrikePriceCurrency(), 4);
			FlatVector::GetData<string_t>(out.data[16])[n] = duckdb_dbn::EmitCstr(out.data[16], rec.UnitOfMeasure(), 31);
			FlatVector::GetData<int64_t>(out.data[17])[n] = static_cast<int64_t>((rec.expiration).time_since_epoch().count());
			FlatVector::GetData<int64_t>(out.data[18])[n] = static_cast<int64_t>((rec.activation).time_since_epoch().count());
			FlatVector::GetData<double>(out.data[19])[n] = static_cast<double>(rec.min_price_increment) * 1e-9;
			FlatVector::GetData<double>(out.data[20])[n] = static_cast<double>(rec.display_factor) * 1e-9;
			FlatVector::GetData<double>(out.data[21])[n] = static_cast<double>(rec.high_limit_price) * 1e-9;
			FlatVector::GetData<double>(out.data[22])[n] = static_cast<double>(rec.low_limit_price) * 1e-9;
			FlatVector::GetData<double>(out.data[23])[n] = static_cast<double>(rec.max_price_variation) * 1e-9;
			FlatVector::GetData<double>(out.data[24])[n] = static_cast<double>(rec.strike_price) * 1e-9;
			FlatVector::GetData<double>(out.data[25])[n] = static_cast<double>(rec.unit_of_measure_qty) * 1e-9;
			FlatVector::GetData<double>(out.data[26])[n] = static_cast<double>(rec.min_price_increment_amount) * 1e-9;
			FlatVector::GetData<double>(out.data[27])[n] = static_cast<double>(rec.price_ratio) * 1e-9;
			FlatVector::GetData<uint64_t>(out.data[28])[n] = static_cast<uint64_t>(rec.raw_instrument_id);
			FlatVector::GetData<uint32_t>(out.data[29])[n] = rec.underlying_id;
			FlatVector::GetData<int32_t>(out.data[30])[n] = rec.inst_attrib_value;
			FlatVector::GetData<int32_t>(out.data[31])[n] = rec.market_depth_implied;
			FlatVector::GetData<int32_t>(out.data[32])[n] = rec.market_depth;
			FlatVector::GetData<uint32_t>(out.data[33])[n] = rec.market_segment_id;
			FlatVector::GetData<uint32_t>(out.data[34])[n] = rec.max_trade_vol;
			FlatVector::GetData<int32_t>(out.data[35])[n] = rec.min_lot_size;
			FlatVector::GetData<int32_t>(out.data[36])[n] = rec.min_lot_size_block;
			FlatVector::GetData<int32_t>(out.data[37])[n] = rec.min_lot_size_round_lot;
			FlatVector::GetData<uint32_t>(out.data[38])[n] = rec.min_trade_vol;
			FlatVector::GetData<int32_t>(out.data[39])[n] = rec.contract_multiplier;
			FlatVector::GetData<int32_t>(out.data[40])[n] = rec.decay_quantity;
			FlatVector::GetData<int32_t>(out.data[41])[n] = rec.original_contract_size;
			FlatVector::GetData<int16_t>(out.data[42])[n] = rec.appl_id;
			FlatVector::GetData<uint16_t>(out.data[43])[n] = rec.maturity_year;
			FlatVector::GetData<uint16_t>(out.data[44])[n] = rec.decay_start_date;
			FlatVector::GetData<uint16_t>(out.data[45])[n] = rec.channel_id;
			FlatVector::GetData<string_t>(out.data[46])[n] = duckdb_dbn::EmitChar1(out.data[46], static_cast<char>(rec.match_algorithm));
			FlatVector::GetData<uint8_t>(out.data[47])[n] = rec.main_fraction;
			FlatVector::GetData<uint8_t>(out.data[48])[n] = rec.price_display_format;
			FlatVector::GetData<uint8_t>(out.data[49])[n] = rec.sub_fraction;
			FlatVector::GetData<uint8_t>(out.data[50])[n] = rec.underlying_product;
			FlatVector::GetData<string_t>(out.data[51])[n] = duckdb_dbn::EmitChar1(out.data[51], static_cast<char>(rec.security_update_action));
			FlatVector::GetData<uint8_t>(out.data[52])[n] = rec.maturity_month;
			FlatVector::GetData<uint8_t>(out.data[53])[n] = rec.maturity_day;
			FlatVector::GetData<uint8_t>(out.data[54])[n] = rec.maturity_week;
			FlatVector::GetData<string_t>(out.data[55])[n] = duckdb_dbn::EmitChar1(out.data[55], static_cast<char>(rec.user_defined_instrument));
			FlatVector::GetData<int8_t>(out.data[56])[n] = static_cast<int8_t>(rec.contract_multiplier_unit);
			FlatVector::GetData<int8_t>(out.data[57])[n] = static_cast<int8_t>(rec.flow_schedule_type);
			FlatVector::GetData<uint8_t>(out.data[58])[n] = rec.tick_rule;
			FlatVector::Validity(out.data[59]).SetInvalid(n);
			FlatVector::Validity(out.data[60]).SetInvalid(n);
			FlatVector::Validity(out.data[61]).SetInvalid(n);
			FlatVector::Validity(out.data[62]).SetInvalid(n);
			FlatVector::Validity(out.data[63]).SetInvalid(n);
			FlatVector::Validity(out.data[64]).SetInvalid(n);
			FlatVector::Validity(out.data[65]).SetInvalid(n);
			FlatVector::Validity(out.data[66]).SetInvalid(n);
			FlatVector::Validity(out.data[67]).SetInvalid(n);
			FlatVector::Validity(out.data[68]).SetInvalid(n);
			FlatVector::Validity(out.data[69]).SetInvalid(n);
			FlatVector::Validity(out.data[70]).SetInvalid(n);
			FlatVector::Validity(out.data[71]).SetInvalid(n);
			FlatVector::GetData<double>(out.data[72])[n] = static_cast<double>(rec.trading_reference_price) * 1e-9;
			FlatVector::GetData<uint16_t>(out.data[73])[n] = rec.trading_reference_date;
			FlatVector::GetData<uint8_t>(out.data[74])[n] = rec.md_security_trading_status;
			FlatVector::GetData<uint8_t>(out.data[75])[n] = rec.settl_price_type;
			++n;
		}
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// Polymorphic read_dbn(path) — probes metadata.schema at bind time and
// dispatches to the right per-schema bind/scan pair.
// ════════════════════════════════════════════════════════════════════════════

struct SchemaHandler {
	const char *display_name;
	table_function_bind_t bind;
	table_function_t scan;
};

static const SchemaHandler *LookupSchemaHandler(databento::Schema s) {
	switch (s) {
	case databento::Schema::Trades: {
		static const SchemaHandler h = {"trades", TradesBind, TradesScan};
		return &h;
	}
	case databento::Schema::Mbo: {
		static const SchemaHandler h = {"mbo", MboBind, MboScan};
		return &h;
	}
	case databento::Schema::Mbp1: {
		static const SchemaHandler h = {"mbp-1", Mbp1Bind, Mbp1Scan};
		return &h;
	}
	case databento::Schema::Mbp10: {
		static const SchemaHandler h = {"mbp-10", Mbp10Bind, Mbp10Scan};
		return &h;
	}
	case databento::Schema::Tbbo: {
		static const SchemaHandler h = {"tbbo", TbboBind, TbboScan};
		return &h;
	}
	case databento::Schema::Bbo1S: {
		static const SchemaHandler h = {"bbo-1s", BboBind, Bbo1sScan};
		return &h;
	}
	case databento::Schema::Bbo1M: {
		static const SchemaHandler h = {"bbo-1m", BboBind, Bbo1mScan};
		return &h;
	}
	case databento::Schema::Cbbo1S: {
		static const SchemaHandler h = {"cbbo-1s", CbboBind, Cbbo1sScan};
		return &h;
	}
	case databento::Schema::Cmbp1: {
		static const SchemaHandler h = {"cmbp-1", Cmbp1Bind, Cmbp1Scan};
		return &h;
	}
	case databento::Schema::Ohlcv1S: {
		static const SchemaHandler h = {"ohlcv-1s", OhlcvBind, Ohlcv1sScan};
		return &h;
	}
	case databento::Schema::Ohlcv1M: {
		static const SchemaHandler h = {"ohlcv-1m", OhlcvBind, Ohlcv1mScan};
		return &h;
	}
	case databento::Schema::Ohlcv1H: {
		static const SchemaHandler h = {"ohlcv-1h", OhlcvBind, Ohlcv1hScan};
		return &h;
	}
	case databento::Schema::Ohlcv1D: {
		static const SchemaHandler h = {"ohlcv-1d", OhlcvBind, Ohlcv1dScan};
		return &h;
	}
	case databento::Schema::Status: {
		static const SchemaHandler h = {"status", StatusBind, StatusScan};
		return &h;
	}
	case databento::Schema::Imbalance: {
		static const SchemaHandler h = {"imbalance", ImbalanceBind, ImbalanceScan};
		return &h;
	}
	case databento::Schema::Statistics: {
		static const SchemaHandler h = {"statistics", StatisticsBind, StatisticsScan};
		return &h;
	}
	case databento::Schema::Definition: {
		static const SchemaHandler h = {"definition", DefinitionBind, DefinitionScan};
		return &h;
	}
	case databento::Schema::Cbbo1M: {
		static const SchemaHandler h = {"cbbo-1m", CbboBind, Cbbo1mScan};
		return &h;
	}
	case databento::Schema::Tcbbo: {
		static const SchemaHandler h = {"tcbbo", Cmbp1Bind, TcbboScan};
		return &h;
	}
	case databento::Schema::OhlcvEod: {
		static const SchemaHandler h = {"ohlcv-eod", OhlcvBind, OhlcvEodScan};
		return &h;
	}
	default:
		return nullptr;
	}
}

static const char *SchemaToCstr(databento::Schema s) {
	switch (s) {
	case databento::Schema::Mbo:        return "mbo";
	case databento::Schema::Mbp1:       return "mbp-1";
	case databento::Schema::Mbp10:      return "mbp-10";
	case databento::Schema::Tbbo:       return "tbbo";
	case databento::Schema::Trades:     return "trades";
	case databento::Schema::Ohlcv1S:    return "ohlcv-1s";
	case databento::Schema::Ohlcv1M:    return "ohlcv-1m";
	case databento::Schema::Ohlcv1H:    return "ohlcv-1h";
	case databento::Schema::Ohlcv1D:    return "ohlcv-1d";
	case databento::Schema::Definition: return "definition";
	case databento::Schema::Statistics: return "statistics";
	case databento::Schema::Status:     return "status";
	case databento::Schema::Imbalance:  return "imbalance";
	case databento::Schema::OhlcvEod:   return "ohlcv-eod";
	case databento::Schema::Cmbp1:      return "cmbp-1";
	case databento::Schema::Cbbo1S:     return "cbbo-1s";
	case databento::Schema::Cbbo1M:     return "cbbo-1m";
	case databento::Schema::Bbo1S:      return "bbo-1s";
	case databento::Schema::Bbo1M:      return "bbo-1m";
	default:                            return "unknown";
	}
}

static const char *STypeToCstr(databento::SType s) {
	switch (s) {
	case databento::SType::InstrumentId:  return "instrument_id";
	case databento::SType::RawSymbol:     return "raw_symbol";
	case databento::SType::Smart:         return "smart";
	case databento::SType::Continuous:    return "continuous";
	case databento::SType::Parent:        return "parent";
	case databento::SType::NasdaqSymbol:  return "nasdaq_symbol";
	case databento::SType::CmsSymbol:     return "cms_symbol";
	case databento::SType::Isin:          return "isin";
	case databento::SType::UsCode:        return "us_code";
	case databento::SType::BbgCompId:     return "bbg_comp_id";
	case databento::SType::BbgCompTicker: return "bbg_comp_ticker";
	case databento::SType::Figi:          return "figi";
	case databento::SType::FigiTicker:    return "figi_ticker";
	default:                              return "unknown";
	}
}

struct ReadDbnBindDataPolymorphic : public ReadDbnBindData {
	table_function_t dispatched_scan = nullptr;
};

static unique_ptr<FunctionData> ReadDbnBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	const auto pattern = GetFilePathArg(input);
	auto paths = ExpandPaths(context, pattern);
	duckdb_dbn::DbnFileReader probe(paths.front());
	const auto &md = probe.GetMetadata();
	if (!md.schema.has_value()) {
		throw InvalidInputException("dbn: file '%s' has no schema in metadata (mixed-schema file); "
		                            "polymorphic read_dbn() can't dispatch — use a specific "
		                            "read_dbn_<schema>() function instead.",
		                            paths.front());
	}
	const auto *handler = LookupSchemaHandler(*md.schema);
	if (handler == nullptr) {
		throw InvalidInputException(
		    "dbn: file '%s' has schema '%s', which the polymorphic read_dbn() dispatcher "
		    "does not yet recognize. File a bug.",
		    paths.front(), SchemaToCstr(*md.schema));
	}
	(void)handler->bind(context, input, return_types, names);
	auto bd = make_uniq<ReadDbnBindDataPolymorphic>();
	bd->file_paths = std::move(paths);
	bd->dispatched_scan = handler->scan;
	return std::move(bd);
}

static void ReadDbnScan(ClientContext &ctx, TableFunctionInput &input, DataChunk &out) {
	auto &bd = input.bind_data->Cast<ReadDbnBindDataPolymorphic>();
	bd.dispatched_scan(ctx, input, out);
}

// ════════════════════════════════════════════════════════════════════════════
// dbn_metadata(path) — single-row introspection helper.
// ════════════════════════════════════════════════════════════════════════════

struct DbnMetadataBindData : public TableFunctionData {
	std::string file_path;
};

struct DbnMetadataGlobalState : public GlobalTableFunctionState {
	bool emitted = false;
};

static unique_ptr<FunctionData> DbnMetadataBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<DbnMetadataBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"version",  "dataset",  "schema",   "start_ts", "end_ts",
	         "limit",    "stype_in", "stype_out","ts_out",   "symbol_cstr_len"};
	return_types = {LogicalType::UTINYINT,    LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UBIGINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::BOOLEAN,
	                LogicalType::UINTEGER};
	return std::move(bd);
}

static unique_ptr<GlobalTableFunctionState> DbnMetadataInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<DbnMetadataGlobalState>();
}

static void DbnMetadataScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &state = input.global_state->Cast<DbnMetadataGlobalState>();
	if (state.emitted) {
		out.SetCardinality(0);
		return;
	}
	state.emitted = true;

	auto &bd = input.bind_data->Cast<DbnMetadataBindData>();
	duckdb_dbn::DbnFileReader reader(bd.file_path);
	const auto &md = reader.GetMetadata();

	FlatVector::GetData<uint8_t>(out.data[0])[0] = md.version;
	FlatVector::GetData<string_t>(out.data[1])[0] =
	    StringVector::AddString(out.data[1], md.dataset.data(), md.dataset.size());
	if (md.schema.has_value()) {
		const char *s = SchemaToCstr(*md.schema);
		FlatVector::GetData<string_t>(out.data[2])[0] = StringVector::AddString(out.data[2], s);
	} else {
		FlatVector::Validity(out.data[2]).SetInvalid(0);
	}
	FlatVector::GetData<int64_t>(out.data[3])[0] = md.start_ns;
	FlatVector::GetData<int64_t>(out.data[4])[0] = md.end_ns;
	FlatVector::GetData<uint64_t>(out.data[5])[0] = md.limit;
	if (md.stype_in.has_value()) {
		const char *s = STypeToCstr(*md.stype_in);
		FlatVector::GetData<string_t>(out.data[6])[0] = StringVector::AddString(out.data[6], s);
	} else {
		FlatVector::Validity(out.data[6]).SetInvalid(0);
	}
	{
		const char *s = STypeToCstr(md.stype_out);
		FlatVector::GetData<string_t>(out.data[7])[0] = StringVector::AddString(out.data[7], s);
	}
	FlatVector::GetData<bool>(out.data[8])[0] = md.ts_out;
	FlatVector::GetData<uint32_t>(out.data[9])[0] = static_cast<uint32_t>(md.symbol_cstr_len);
	out.SetCardinality(1);
}

// ════════════════════════════════════════════════════════════════════════════
// dbn_records(path) — raw rtype-agnostic view, for mixed-schema files where
// metadata.schema is NULL (system messages mixed with data records, etc.).
// Single-file (no glob). Emits header fields + record body as BLOB.
// ════════════════════════════════════════════════════════════════════════════

struct DbnRecordsBindData : public TableFunctionData {
	std::string file_path;
};

struct DbnRecordsGlobalState : public GlobalTableFunctionState {
	explicit DbnRecordsGlobalState(const std::string &path)
	    : reader(std::make_unique<duckdb_dbn::DbnFileReader>(path)) {}
	std::unique_ptr<duckdb_dbn::DbnFileReader> reader;
};

static unique_ptr<FunctionData> DbnRecordsBind(ClientContext &, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<DbnRecordsBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event", "rtype", "length", "publisher_id", "instrument_id", "body"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::UTINYINT,  LogicalType::UTINYINT,
	                LogicalType::USMALLINT,    LogicalType::UINTEGER,  LogicalType::BLOB};
	return std::move(bd);
}

static unique_ptr<GlobalTableFunctionState> DbnRecordsInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<DbnRecordsBindData>();
	return make_uniq<DbnRecordsGlobalState>(bd.file_path);
}

static void DbnRecordsScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<DbnRecordsGlobalState>();
	auto ts_event_v = FlatVector::GetData<int64_t>(out.data[0]);
	auto rtype_v = FlatVector::GetData<uint8_t>(out.data[1]);
	auto length_v = FlatVector::GetData<uint8_t>(out.data[2]);
	auto pub_v = FlatVector::GetData<uint16_t>(out.data[3]);
	auto instr_v = FlatVector::GetData<uint32_t>(out.data[4]);
	auto body_v = FlatVector::GetData<string_t>(out.data[5]);

	alignas(8) std::byte buf[duckdb_dbn::DbnFileReader::kMaxRecordLen];
	databento::RecordHeader hdr {};
	std::size_t rec_len = 0;
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextRecordRaw(buf, &hdr, &rec_len, nullptr)) {
		ts_event_v[n] = static_cast<int64_t>(hdr.ts_event.time_since_epoch().count());
		rtype_v[n] = static_cast<uint8_t>(hdr.rtype);
		length_v[n] = hdr.length;
		pub_v[n] = hdr.publisher_id;
		instr_v[n] = hdr.instrument_id;
		body_v[n] = StringVector::AddStringOrBlob(out.data[5], reinterpret_cast<const char *>(buf), rec_len);
		++n;
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// Replacement scan — `SELECT * FROM 'foo.dbn'` rewrites to read_dbn('foo.dbn').
// `.dbn.zst` is NOT matched here: DuckDB's built-in parse-time path intercepts
// any `.zst` suffix and routes it through the parquet extension before any
// replacement scan runs. For compressed files, call read_dbn('foo.dbn.zst')
// directly — Phase 3-B's ZstdInput handles decompression transparently.
// ════════════════════════════════════════════════════════════════════════════

static unique_ptr<TableRef> ReadDbnReplacement(ClientContext &, ReplacementScanInput &input,
                                               optional_ptr<ReplacementScanData>) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!StringUtil::EndsWith(table_name, ".dbn")) {
		return nullptr;
	}
	auto fn = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	fn->function = make_uniq<FunctionExpression>("read_dbn", std::move(children));
	return std::move(fn);
}

// ════════════════════════════════════════════════════════════════════════════
// Registration
// ════════════════════════════════════════════════════════════════════════════

static void Register(ExtensionLoader &loader, const char *name, table_function_bind_t bind, table_function_t scan) {
	TableFunction f(name, {LogicalType::VARCHAR}, scan, bind, ReadDbnInitGlobal);
	loader.RegisterFunction(f);
}

static void LoadInternal(ExtensionLoader &loader) {
	Register(loader, "read_dbn", ReadDbnBind, ReadDbnScan);
	Register(loader, "read_dbn_trades", TradesBind, TradesScan);
	Register(loader, "read_dbn_mbo", MboBind, MboScan);
	Register(loader, "read_dbn_mbp1", Mbp1Bind, Mbp1Scan);
	Register(loader, "read_dbn_mbp10", Mbp10Bind, Mbp10Scan);
	Register(loader, "read_dbn_bbo_1s", BboBind, Bbo1sScan);
	Register(loader, "read_dbn_bbo_1m", BboBind, Bbo1mScan);
	Register(loader, "read_dbn_cbbo_1s", CbboBind, Cbbo1sScan);
	Register(loader, "read_dbn_cmbp1", Cmbp1Bind, Cmbp1Scan);
	Register(loader, "read_dbn_tbbo", TbboBind, TbboScan);
	Register(loader, "read_dbn_ohlcv_1s", OhlcvBind, Ohlcv1sScan);
	Register(loader, "read_dbn_ohlcv_1m", OhlcvBind, Ohlcv1mScan);
	Register(loader, "read_dbn_ohlcv_1h", OhlcvBind, Ohlcv1hScan);
	Register(loader, "read_dbn_ohlcv_1d", OhlcvBind, Ohlcv1dScan);
	Register(loader, "read_dbn_status", StatusBind, StatusScan);
	Register(loader, "read_dbn_imbalance", ImbalanceBind, ImbalanceScan);
	Register(loader, "read_dbn_statistics", StatisticsBind, StatisticsScan);
	Register(loader, "read_dbn_definition", DefinitionBind, DefinitionScan);
	Register(loader, "read_dbn_cbbo_1m", CbboBind, Cbbo1mScan);
	Register(loader, "read_dbn_ohlcv_eod", OhlcvBind, OhlcvEodScan);
	Register(loader, "read_dbn_tcbbo", Cmbp1Bind, TcbboScan);

	TableFunction mf("dbn_metadata", {LogicalType::VARCHAR}, DbnMetadataScan, DbnMetadataBind, DbnMetadataInitGlobal);
	loader.RegisterFunction(mf);

	TableFunction rf("dbn_records", {LogicalType::VARCHAR}, DbnRecordsScan, DbnRecordsBind, DbnRecordsInitGlobal);
	loader.RegisterFunction(rf);

	DBConfig::GetConfig(loader.GetDatabaseInstance()).replacement_scans.emplace_back(ReadDbnReplacement);
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
