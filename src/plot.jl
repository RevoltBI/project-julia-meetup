using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase
using Plots, PlotThemes, StatPlots

theme(:dark)

optionally_convert_to_df(df::DataFrame) = df
optionally_convert_to_df(df) = df |> DataFrame

log_df = CSV.File("data/hflights.csv", missingstring="NA") |> optionally_convert_to_df
