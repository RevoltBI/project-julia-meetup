#!/usr/bin/env bash
python3 src/hflights.py > logs/hflights.py.log
Rscript src/hflights.r > logs/hflights.r.log
julia src/hflights.jl > logs/hflights.jl.log
