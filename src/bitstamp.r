print("Libraries loading time: ")
Packages <- c("dplyr", "readr", "anytime")
libraries_loading_time <-system.time(
  suppressMessages(lapply(Packages, library, character.only = TRUE)))
print(libraries_loading_time)

print("Data loading time: ")
data_loading_time <- system.time(suppressMessages(bitstamp <- read_csv("data/bitstamp.csv")))

print(data_loading_time)

print("Preprocessing time: ")
preprocessing_time <- system.time(bitstamp_dt <- bitstamp %>%
                    mutate(Datetime = anytime(Timestamp)) %>%
                    mutate(YearMonth = format(Datetime, "%Y-%m")))
print(preprocessing_time)

N = 10

time_s = system.time(for (i in 1:N){
  t <- bitstamp_dt %>%
    group_by(YearMonth) %>%
    summarise(AvgClose = mean(Close), MaxHigh = max(High))})
print("Processing time: ")
processing_time <- time_s / N
print(processing_time)

suppressMessages(log <- read_csv("logs/log.csv"))
DateTime <- Sys.time() %>% as.POSIXct
log <- log %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "Bitstamp", "TaskNames" = "Library loading", "TaskTypes" = "Loading", "TaskExecTimes" = libraries_loading_time[['elapsed']])) %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "Bitstamp", "TaskNames" = "File loading", "TaskTypes" = "Execution", "TaskExecTimes" = data_loading_time[['elapsed']])) %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "Bitstamp", "TaskNames" = "Preprocessing", "TaskTypes" = "Execution", "TaskExecTimes" = preprocessing_time[['elapsed']])) %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "Bitstamp", "TaskNames" = "Processing", "TaskTypes" = "Execution", "TaskExecTimes" = processing_time[['elapsed']]))
write_csv(log, "logs/log.csv")
