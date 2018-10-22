# using Pkg
# Pkg.add(["DataFrames", "DataFramesMeta", "CSV"])
# Pkg.add(PackageSpec(url="https://github.com/FNj/Hose.jl.git", rev="indexing"))

lib_load_time = @elapsed using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase, Dates
# import Base.Meta.@dump
# const dir = @__DIR__

optionally_convert_to_df(df::DataFrame) = df
optionally_convert_to_df(df) = df |> DataFrame

file_load_compilation_time = @elapsed CSV.File("data/bitstamp.csv", missingstring="NA") |> optionally_convert_to_df
file_load_time = @elapsed const bitstamp = CSV.File("data/bitstamp.csv", missingstring="NA") |> optionally_convert_to_df


file_load_compilation_time -= file_load_time

# size(bitstamp, 1) / 227496
# names(hflights)
# head(hflights)
# describe(hflights)

# macro sample(df, n::Int)
#     esc(:($df[sample(1:nrow($df), $n), :]))
# end

# macro sample(df, q::Real)
#     esc(:($df[sample(1:nrow($df), round(Int, $q * nrow($df))), :]))
# end

function preprocess(df)
    @hose df |> @transform(Datetime = Dates.unix2datetime.(:Timestamp)) |> @transform(YearMonth = Dates.yearmonth.(:Datetime))
end

preprocess_compilation_time = @elapsed preprocess(bitstamp)
preprocess_exec_time = @elapsed const bitstamp_dt = preprocess(bitstamp)
preprocess_compilation_time -= preprocess_exec_time

# describe(bitstamp_dt)

# describe(bitstamp_dt)
# @time @hose bitstamp_dt |> @where(first.(:YearMonth) .< 2018)

function process(df)
    @hose df |>
    @by(:YearMonth, AvgClose = mean(:Close), MaxHigh = maximum(:High))
end

process_compile_time = @elapsed process(bitstamp_dt[1:5, :])
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

process_exec_time = mean(times)
process_compile_time -= process_exec_time
# process_strrev(df) = @hose df |> @transform(TailNum2=reverse.(:TailNum))
#
# string_reversal_compile_time = @elapsed process_strrev(hflights)
# string_reversal_time = @elapsed process_strrev(hflights)

println("""Stats:
- Libraries loading time: $lib_load_time s,
- File loading compilation time: $file_load_compilation_time s,
- File loading time: $file_load_time s,
- Preprocessing compilation time: $preprocess_compilation_time s,
- Preprocessing execution time: $preprocess_exec_time s,
- Group by compilation time: $process_compile_time s,
- Group by execution time: $process_exec_time s.""")

using Dates
task_names = vcat(["Library loading"], fill("File loading", 2), fill("Preprocessing", 2), fill("Processing", 2))
task_types = vcat(["Loading"], repeat(["Compilation", "Execution"], 3))
task_exec_times = [lib_load_time, file_load_compilation_time, file_load_time, preprocess_compilation_time, preprocess_exec_time, process_compile_time, process_exec_time]
stats_df = DataFrame(DateTime=fill(Dates.now(), length(task_names)), Language=fill("Julia", length(task_names)), Dataset=fill("Bitstamp", length(task_names)), TaskNames=task_names, TaskTypes=task_types, TaskExecTimes=task_exec_times)

try
    vcat(CSV.read("logs/log.csv") |> DataFrame, stats_df)
catch
    stats_df
end |> CSV.write("logs/log.csv")
