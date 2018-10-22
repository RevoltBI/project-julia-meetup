using DataFrames, CSV, DataFramesMeta, Hose, Statistics, StatsBase
using Plots, PlotThemes, StatPlots

theme(:dark)

optionally_convert_to_df(df::DataFrame) = df
optionally_convert_to_df(df) = df |> DataFrame

log_df = CSV.File("logs/log.csv") |> optionally_convert_to_df


bs = @hose log_df |>
@where(:Dataset .== "Bitstamp", :TaskTypes .== "Execution") |>
@select(:Language, :TaskNames, :TaskTypes, :TaskExecTimes) |>
@by([:Language, :TaskNames], TotalTime = sum(:TaskExecTimes))

@df bs groupedbar(:Language, :TotalTime, group = :TaskNames,
                  ylabel = "Time (s)", xticks = :all,
                  bar_position = :stack, legend=:topright, size=(400,600))
savefig("img/bitstamp_exec.pdf")

hfl = @hose log_df |>
@where(:Dataset .== "HFlights", :TaskTypes .== "Execution") |>
@select(:Language, :TaskNames, :TaskTypes, :TaskExecTimes) |>
@by([:Language, :TaskNames], TotalTime = sum(:TaskExecTimes))

@df hfl groupedbar(:Language, :TotalTime, group = :TaskNames,
                  ylabel = "Time (s)", xticks = :all,
                  bar_position = :stack, legend=:topleft, size=(400,600))
savefig("img/hflights_exec.pdf")

overhead = @hose log_df |>
@where(:TaskTypes .!= "Execution") |>
@select(:Dataset, :Language, :TaskNames, :TaskTypes, :TaskExecTimes) |>
@by([:Language, :TaskTypes], TotalTime = sum(:TaskExecTimes)) |>
@transform(TaskTypeLanguage = :TaskTypes .* " in " .* :Language)

@df overhead groupedbar(:Language, :TotalTime, group = :TaskTypes,
                  ylabel = "Time (s)", xticks = :all,
                  bar_position = :stack, legend=:topright, size=(400,600))
savefig("img/overhead.pdf")
# execs = @hose log_df |>
# @where(:TaskTypes .== "Execution") |>
# @select(:Dataset, :Language, :TaskNames, :TaskTypes, :TaskExecTimes) |>
# @by([:Dataset, :Language, :TaskNames], TotalTime = sum(:TaskExecTimes)) |>
# @transform(DatasetLanguage = :Dataset .* " by " .* :Language)
#
# Plots.reset_defaults()
# @df execs groupedbar(:DatasetLanguage, :TotalTime, group = :TaskNames,
#                   ylabel = "Time (s)", xticks = :all,
#                   bar_position = :stack, legend=:topright, size=(2000,1200))
# png("img/exec.png")
