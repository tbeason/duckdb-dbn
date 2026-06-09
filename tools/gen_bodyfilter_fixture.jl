# Standalone generator for test_data.tcbbo.bodyfilter.dbn — a 5000-record TCBBO
# fixture that triggers the empty-mid-stream-chunk body-filter bug.
#
# The scan emits chunks of STANDARD_VECTOR_SIZE (2048) records. `side` is laid
# out so a `WHERE side='A'` / `WHERE side='B'` body filter fully rejects a whole
# *intermediate* chunk while matches exist on both sides of it:
#   records    0..2047  -> side 'A'   (chunk 0: all 'A')
#   records 2048..4095  -> side 'B'   (chunk 1: all 'B'  -> rejected by side='A')
#   records 4096..4999  -> side 'A'   (chunk 2: 904 'A')
# So WHERE side='A' must return 2952 and WHERE side='B' must return 2048. Before
# the fix, a body filter that slices chunk 1 to zero rows made the scan return a
# premature empty chunk (PhysicalTableScan -> FINISHED), silently truncating the
# result (side='A' -> 2048, side='B' -> 0). All fixtures shipped were <2048 rows,
# so this never surfaced.
#
# Run from repo root with the DBN.jl writer project (same as generate_fixtures.jl):
#   julia --project=C:/Users/tbeas/Documents/GitHub/DatabentoBinaryEncoding.jl \
#         tools/gen_bodyfilter_fixture.jl

using DatabentoBinaryEncoding: RecordHeader, BidAskPair, Metadata,
    TCBBOMsg, RType, Schema, SType, Action, Side, write_dbn

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

function rec(i, side)
    ts = T0 + i * 1_000_000
    TCBBOMsg(
        RecordHeader(tcbbo_len(), RType.TCBBO_MSG, PUB_ID, UInt32(5482), ts),
        Int64(3720_250_000_000), UInt32(20),
        Action.TRADE, side, UInt8(129), UInt8(0),
        ts + 250_000, Int32(15_000), UInt32(2_000_000 + i),
        BidAskPair(Int64(3720_250_000_000), Int64(3720_500_000_000),
                   UInt32(26), UInt32(7), UInt32(16), UInt32(6)),
    )
end

const N = 5000
records = TCBBOMsg[]
for i in 0:(N - 1)
    s = (2048 <= i < 4096) ? Side.BID : Side.ASK   # 'B' only in the middle chunk
    push!(records, rec(i, s))
end

out_path = joinpath(DATA, "test_data.tcbbo.bodyfilter.dbn")
write_dbn(out_path, base_metadata(Schema.TCBBO), records)
println("  ok   test_data.tcbbo.bodyfilter.dbn (", length(records), " records)")
