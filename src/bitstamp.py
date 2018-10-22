import time
start_time = time.time()
from datetime import datetime
import pandas as pd
from dfply import *
lib_load_time = time.time() - start_time
print("Libraries loading time: %s s." % lib_load_time)


start_time = time.time()
bitstamp = pd.read_csv("data/bitstamp.csv")
file_load_time = time.time() - start_time
print("File loading time: %s s." % file_load_time)

start_time = time.time()
bitstamp['Datetime'] = pd.to_datetime(bitstamp['Timestamp'], unit='s')
bitstamp['YearMonth'] = bitstamp['Datetime'].map(lambda x: x.strftime('%Y-%m'))
preprocess_exec_time = time.time() - start_time
print("Preprocessing time: %s s." % preprocess_exec_time)

N = 10
start_time = time.time()
for i in range(N):
    (bitstamp >>
        group_by(X.YearMonth) >>
        summarize(AvgClose = mean(X.Close), MaxHigh = colmax(X.High)))
exec_time = (time.time() - start_time) / N
print("Processing time: %s s." % exec_time)

task_names = ["Library loading"] + ["File loading"] + ["Split-apply-combine"] + ["String reversal"]
task_types = ["Loading"] + ["Execution"] * 3
task_exec_times = [lib_load_time, file_load_time, preprocess_exec_time, exec_time]
new_df = pd.DataFrame({"DateTime":[datetime.now().isoformat()]*len(task_names), "Language":["Python"]*len(task_names), "Dataset":["Bitstamp"]*len(task_names), "TaskNames":task_names, "TaskTypes":task_types, "TaskExecTimes":task_exec_times})
try:
    out_df = pd.concat([pd.read_csv("logs/log.csv"), new_df], sort=False)
except Exception as e:
    out_df = new_df

out_df.to_csv("logs/log.csv", index=False)
