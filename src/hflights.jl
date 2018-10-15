# using Pkg
# Pkg.add(["DataFrames", "DataFramesMeta", "CSV"])
# Pkg.add(PackageSpec(url="https://github.com/FNj/Hose.jl.git", rev="indexing"))

lib_load_time = @elapsed using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase
# import Base.Meta.@dump
const dir = @__DIR__

file_load_time = @elapsed const hflights = CSV.File(dir * "/../data/hflights.csv", missingstring="NA") |> DataFrame

# size(hflights)
# names(hflights)
# head(hflights)
# describe(hflights)

macro sample(df, n::Int)
    esc(:($df[sample(1:nrow($df), $n), :]))
end

# macro sample(df, q::Real)
#     esc(:($df[sample(1:nrow($df), round(Int, $q * nrow($df))), :]))
# end

function process(df)
    @hose df |>
    # @sample(.4) |>
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

println("""Stats:
- Libraries loading time: $lib_load_time s,
- CSV file loading time: $file_load_time s,
- Compilation time: $compile_time s,
- Mean execution time after compilation (with GC): $mean_time s.""")

# - Mean execution time after compilation (without GC): $mean_time_no_GC s.
