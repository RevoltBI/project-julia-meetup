import pandas as pd
from dfply import *
import time

hflights = pd.read_csv("data/hflights.csv")

@dfpipe
def dropna(df, *args, **kwargs):
    return df.dropna()

N = 10
start_time = time.time()

for i in range(N):
    (hflights >>
        mutate(Speed=X.Distance / X.AirTime) >>
        select(X.Month, X.ArrDelay, X.Speed) >>
        dropna() >>
        group_by(X.Month) >>
        mutate(AvgDelay = mean(X.ArrDelay), MaxSpeed = colmax(X.Speed)) >>
        select(X.Month, X.AvgDelay, X.MaxSpeed) >> head(1))

print("Execution time: %s s." % ((time.time() - start_time) / N))
# @by(:Month, AvgDelay = mean(:ArrDelay), MaxSpeed = maximum(:Speed)))
