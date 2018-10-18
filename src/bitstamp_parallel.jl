# using Pkg
# Pkg.add(["DataFrames", "DataFramesMeta", "CSV"])
# Pkg.add(PackageSpec(url="https://github.com/FNj/Hose.jl.git", rev="indexing"))
using Distributed
addprocs(3)
lib_load_time = @elapsed @everywhere using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase, Dates, Distributed
# import Base.Meta.@dump
const dir = @__DIR__

file_load_compilation_time = @elapsed @everywhere CSV.File("data/bitstamp.csv", missingstring="NA") |> DataFrame
file_load_time = @elapsed @everywhere const bitstamp = CSV.File("data/bitstamp.csv", missingstring="NA") |> DataFrame

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

@everywhere const bitstamp_dt = @hose bitstamp |> @transform(Datetime = Dates.unix2datetime.(:Timestamp)) |> @transform(YearMonth = Dates.yearmonth.(:Datetime), Year = Dates.year.(:Datetime), Month = Dates.month.(:Datetime))

# describe(bitstamp_dt)

# describe(bitstamp_dt)
# @time @hose bitstamp_dt |> @where(first.(:YearMonth) .< 2018)

@everywhere function process(df)
    @hose df |>
    @by(:YearMonth, AvgClose = mean(:Close), MaxHigh = maximum(:High))
end

@everywhere function parallel_process(df)
    # worker_procs = length(procs()) - 1
    f1 = @spawn process(@hose df |> @where(:Year.<= 2013))
    f2 = @spawn process(@hose df |> @where(2013 .< :Year .<= 2015))
    f3 = @spawn process(@hose df |> @where(:Year .>= 2016))
    vcat(fetch(f1), fetch(f2), fetch(f3))
end

compile_time = @elapsed process(bitstamp_dt[1:5, :])
parallel_process_compile_time = @elapsed parallel_process(bitstamp_dt)
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
