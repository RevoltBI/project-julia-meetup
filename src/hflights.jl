# using Pkg
# Pkg.add(["DataFrames", "DataFramesMeta", "CSV"])
# Pkg.add(PackageSpec(url="https://github.com/FNj/Hose.jl.git", rev="indexing"))

using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase
# import Base.Meta.@dump
dir = @__DIR__

hflights = CSV.File(dir * "/../data/hflights.csv", missingstring="NA") |> DataFrame

# size(hflights)
# names(hflights)
# head(hflights)
# describe(hflights)

# macro sample(df, n::Int)
#     esc(:($df[sample(1:nrow($df), $n), :]))
# end
#
# macro sample(df, q::Real)
#     esc(:($df[sample(1:nrow($df), round(Int, $q * nrow($df))), :]))
# end

@hose hflights |>
    # @sample(.4) |>
    @transform(Speed = :Distance ./ :AirTime .* 60) |>
    @select(:Month, :ArrDelay, :Speed) |>
    dropmissing |>
    @by(:Month, AvgDelay = mean(:ArrDelay), MaxSpeed = maximum(:Speed))

@time @hose hflights |>
    # @sample(.4) |>
    @transform(Speed = :Distance ./ :AirTime .* 60) |>
    @select(:Month, :ArrDelay, :Speed) |>
    dropmissing |>
    @by(:Month, AvgDelay = mean(:ArrDelay), MaxSpeed = maximum(:Speed))
