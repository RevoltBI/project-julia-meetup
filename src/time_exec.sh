#!/usr/bin/env bash
python3 src/hflights.py > logs/hflights.py.log
Rscript src/hflights.r > logs/hflights.r.log
julia src/hflights.jl > logs/hflights.jl.log
python3 src/bitstamp.py > logs/bitstamp.py.log
Rscript src/bitstamp.r > logs/bitstamp.r.log
julia src/bitstamp.jl > logs/bitstamp.jl.log

julia src/plot.jl
