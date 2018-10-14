import pandas as pd
from dfply import *
import time

hflights = pd.read_csv("data/hflights.csv")

@dfpipe
def dropna(df, *args, **kwargs):
    return df.dropna()

start_time = time.time()

(hflights >>
    mutate(Speed=X.Distance / X.AirTime) >>
    select(X.Month, X.ArrDelay, X.Speed) >>
    dropna() >>
    group_by(X.Month) >>
    mutate(AvgDelay = mean(X.ArrDelay), MaxSpeed = colmax(X.Speed)) >>
    select(X.Month, X.AvgDelay, X.MaxSpeed) >> head(1))

print("--- %s seconds ---" % (time.time() - start_time))
# @by(:Month, AvgDelay = mean(:ArrDelay), MaxSpeed = maximum(:Speed)))
