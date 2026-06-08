# Standalone generator for test_data.tcbbo_symbols.dbn — a single DBN stream
# that interleaves SymbolMappingMsg records with TCBBOMsg market data, the way
# a live OPRA capture does. It exercises read_dbn_tcbbo(..., symbols := true):
#   * resolution of instrument_id → stype_out_symbol,
#   * SQL NULL for an instrument with no mapping seen yet,
#   * remap mid-stream (stream order IS the validity window).
#
# Kept separate from generate_fixtures.jl so regenerating this one fixture
# doesn't rewrite the others. Run from repo root with the DBN.jl writer project
# (same as generate_fixtures.jl):
#   julia --project=C:/Users/tbeas/Documents/GitHub/DatabentoBinaryEncoding.jl \
#         tools/gen_tcbbo_symbols.jl
#
# Stream order (what the reader sees top-to-bottom):
#   1. map 5482 → "OPT5482"
#   2. map 6001 → "OPT6001"
#   3. tcbbo 5482            → resolves "OPT5482"
#   4. tcbbo 6001            → resolves "OPT6001"
#   5. tcbbo 9999            → NULL (never mapped)
#   6. map 5482 → "OPT5482_B" (remap)
#   7. tcbbo 5482            → resolves "OPT5482_B" (the newer mapping)

using DatabentoBinaryEncoding: RecordHeader, BidAskPair, Metadata,
    TCBBOMsg, SymbolMappingMsg, RType, Schema, SType, Action, Side, write_dbn

const REPO = dirname(@__DIR__)
const DATA = joinpath(REPO, "test", "data")
const T0 = 1609160400000000000
const DATASET = "GLBX.MDP3"
const PUB_ID = UInt16(1)

base_metadata(schema) = Metadata(
    UInt8(3), DATASET, schema, T0, T0 + 39_600_000_000_000, nothing,
    SType.RAW_SYMBOL, SType.RAW_SYMBOL, false,
    String[], String[], String[], Tuple{String,String,Int64,Int64}[],
)

tcbbo_len() = UInt8(div(sizeof(TCBBOMsg), 4))
const MAP_LEN = UInt8(44) # canonical 176-byte SymbolMappingMsg (see generate_fixtures.jl)

mapmsg(iid, ts, out_sym) = SymbolMappingMsg(
    RecordHeader(MAP_LEN, RType.SYMBOL_MAPPING_MSG, PUB_ID, UInt32(iid), ts),
    SType.RAW_SYMBOL, "RAW$(iid)",
    SType.INSTRUMENT_ID, out_sym,
    ts, ts + 86_400_000_000_000,
)

tcbbomsg(iid, ts) = TCBBOMsg(
    RecordHeader(tcbbo_len(), RType.TCBBO_MSG, PUB_ID, UInt32(iid), ts),
    Int64(3720_250_000_000), UInt32(20),
    Action.TRADE, Side.ASK, UInt8(129), UInt8(0),
    ts + 250_000, Int32(15_000), UInt32(2_000_001),
    BidAskPair(Int64(3720_250_000_000), Int64(3720_500_000_000),
               UInt32(26), UInt32(7), UInt32(16), UInt32(6)),
)

records = Any[
    mapmsg(5482, T0,                 "OPT5482"),
    mapmsg(6001, T0,                 "OPT6001"),
    tcbbomsg(5482, T0 + 1_000_000_000),
    tcbbomsg(6001, T0 + 2_000_000_000),
    tcbbomsg(9999, T0 + 3_000_000_000),
    mapmsg(5482, T0 + 4_000_000_000, "OPT5482_B"),
    tcbbomsg(5482, T0 + 5_000_000_000),
]

out_path = joinpath(DATA, "test_data.tcbbo_symbols.dbn")
write_dbn(out_path, base_metadata(Schema.TCBBO), records)
println("  ok   test_data.tcbbo_symbols.dbn (", length(records), " records)")
