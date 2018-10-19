import time
start_time = time.time()
from datetime import datetime
import pandas as pd
from dfply import *
print("Libraries loading time: %s s." % ((time.time() - start_time)))

start_time = time.time()
bitstamp = pd.read_csv("data/bitstamp.csv")
print("Data loading time: %s s." % ((time.time() - start_time)))

start_time = time.time()
bitstamp['Datetime'] = pd.to_datetime(bitstamp['Timestamp'], unit='s')
bitstamp['YearMonth'] = bitstamp['Datetime'].map(lambda x: x.strftime('%Y-%m'))
print("Preprocessing time: %s s." % ((time.time() - start_time)))

N = 10
start_time = time.time()
for i in range(N):
    (bitstamp >>
        group_by(X.YearMonth) >>
        summarize(AvgClose = mean(X.Close), MaxHigh = colmax(X.High)))
print("Processing time: %s s." % ((time.time() - start_time) / N))
