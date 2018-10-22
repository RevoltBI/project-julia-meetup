import time
start_time = time.time()
import pandas as pd
from dfply import *
lib_load_time = time.time() - start_time
print("Libraries loading time: %s s." % lib_load_time)


start_time = time.time()
hflights = pd.read_csv("data/hflights.csv")
file_load_time = time.time() - start_time
print("Data loading time: %s s." % file_load_time)

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
        summarize(AvgDelay = mean(X.ArrDelay), MaxSpeed = colmax(X.Speed)))

exec_time = (time.time() - start_time) / N
print("Mean group by execution time: %s s." % exec_time)

start_time = time.time()
hflights["RevTailNum"] = hflights["TailNum"].apply(lambda s: str(s)[::-1])
string_reversal_time = time.time() - start_time
print("String reversal execution time: %s s." % string_reversal_time)

from datetime import datetime

task_names = ["Library loading"] + ["File loading"] + ["Split-apply-combine"] + ["String reversal"]
task_types = ["Loading"] + ["Execution"] * 3
task_exec_times = [lib_load_time, file_load_time, exec_time, string_reversal_time]
new_df = pd.DataFrame({"DateTime":[datetime.now().isoformat()]*len(task_names), "Language":["Python"]*len(task_names), "Dataset":["HFlights"]*len(task_names), "TaskNames":task_names, "TaskTypes":task_types, "TaskExecTimes":task_exec_times})
try:
    out_df = pd.concat([pd.read_csv("logs/log.csv"), new_df], sort=False)
except Exception as e:
    out_df = new_df

out_df.to_csv("logs/log.csv", index=False)
