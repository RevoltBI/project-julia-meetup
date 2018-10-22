# using Pkg
# Pkg.add(["DataFrames", "DataFramesMeta", "CSV"])
# Pkg.add(PackageSpec(url="https://github.com/FNj/Hose.jl.git", rev="indexing"))

lib_load_time = @elapsed using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase
# import Base.Meta.@dump
const dir = @__DIR__

optionally_convert_to_df(df::DataFrame) = df
optionally_convert_to_df(df) = df |> DataFrame

file_load_compilation_time = @elapsed CSV.File("data/hflights.csv", missingstring="NA") |> optionally_convert_to_df
file_load_time = @elapsed const hflights = CSV.File("data/hflights.csv", missingstring="NA") |> optionally_convert_to_df
file_load_compilation_time -= file_load_time

# size(hflights)
# names(hflights)
# head(hflights)
# describe(hflights)

# macro sample(df, n::Int)
#     esc(:($df[sample(1:nrow($df), $n), :]))
# end

# macro sample(df, q::Real)
#     esc(:($df[sample(1:nrow($df), round(Int, $q * nrow($df))), :]))
# end

function process(df)
    @hose df |>
    @transform(Speed = :Distance ./ :AirTime .* 60) |>
    @select(:Month, :ArrDelay, :Speed) |>
    dropmissing |>
    @by(:Month, AvgDelay = mean(:ArrDelay), MaxSpeed = maximum(:Speed))
end

compile_time = @elapsed process(hflights[1:5, :])
const N = 20
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
    times[i] = @elapsed process(hflights)
end

mean_time = mean(times)
compile_time -= mean_time

process_strrev(df) = @hose df |> @transform(TailNum2=reverse.(:TailNum))

string_reversal_compile_time = @elapsed process_strrev(hflights)
string_reversal_time = @elapsed process_strrev(hflights)

string_reversal_compile_time -= string_reversal_time

println("""Stats:
- Libraries loading time: $lib_load_time s,
- File loading compilation time: $file_load_compilation_time s,
- File loading time: $file_load_time s,
- Group by compilation time: $compile_time s,
- Group by execution time: $mean_time s,
- String reversal compilation time: $string_reversal_compile_time s,
- String reversal execution time: $string_reversal_time s.""")

using Dates
task_names = vcat(["Library loading"], fill("File loading", 2), fill("Split-apply-combine", 2), fill("String reversal", 2))
task_types = vcat(["Loading"], repeat(["Compilation", "Execution"], 3))
task_exec_times = [lib_load_time, file_load_compilation_time, file_load_time, compile_time, mean_time, string_reversal_compile_time, string_reversal_time]
stats_df = DataFrame(DateTime=fill(Dates.now(), length(task_names)), Language=fill("Julia", length(task_names)), Dataset=fill("HFlights", length(task_names)), TaskNames=task_names, TaskTypes=task_types, TaskExecTimes=task_exec_times)

try
    vcat(CSV.read("logs/log.csv") |> DataFrame, stats_df)
catch
    stats_df
end |> CSV.write("logs/log.csv")
