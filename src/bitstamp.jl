# using Pkg
# Pkg.add(["DataFrames", "DataFramesMeta", "CSV"])
# Pkg.add(PackageSpec(url="https://github.com/FNj/Hose.jl.git", rev="indexing"))

lib_load_time = @elapsed using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase, Dates
# import Base.Meta.@dump
const dir = @__DIR__

file_load_compilation_time = @elapsed CSV.File("data/bitstamp.csv", missingstring="NA") |> DataFrame
file_load_time = @elapsed const bitstamp = CSV.File("data/bitstamp.csv", missingstring="NA") |> DataFrame

size(bitstamp, 1) / 227496
# names(hflights)
# head(hflights)
# describe(hflights)

# macro sample(df, n::Int)
#     esc(:($df[sample(1:nrow($df), $n), :]))
# end

# macro sample(df, q::Real)
#     esc(:($df[sample(1:nrow($df), round(Int, $q * nrow($df))), :]))
# end

const bitstamp_dt = @hose bitstamp |> @transform(Datetime = Dates.unix2datetime.(:Timestamp)) |> @transform(YearMonth = Dates.yearmonth.(:Datetime))

# describe(bitstamp_dt)

# describe(bitstamp_dt)
# @time @hose bitstamp_dt |> @where(first.(:YearMonth) .< 2018)

function process(df)
    @hose df |>
    @by(:YearMonth, AvgClose = mean(:Close), MaxHigh = maximum(:High))
end

compile_time = @elapsed process(bitstamp_dt[1:5, :])
const N = 10
times = zeros(N)
# GC.enable(false)
# for i=1:N
#     GC.gc()
#     sleep(0.01)
#     times[i] = @elapsed process(hflights)
# end
# GC.enable(true)
#
# mean_time_no_GC = mean(times)

for i=1:N
    times[i] = @elapsed process(bitstamp_dt)
end

mean_time = mean(times)

# process_strrev(df) = @hose df |> @transform(TailNum2=reverse.(:TailNum))
#
# string_reversal_compile_time = @elapsed process_strrev(hflights)
# string_reversal_time = @elapsed process_strrev(hflights)

println("""Stats:
- Libraries loading time: $lib_load_time s,
- File loading compilation time: $file_load_compilation_time s,
- File loading time: $file_load_time s,
- Group by compilation time: $compile_time s,
- Group by execution time: $mean_time s.""")

# - Mean execution time after compilation (without GC): $mean_time_no_GC s.
