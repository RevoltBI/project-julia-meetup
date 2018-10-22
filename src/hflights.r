print("Libraries loading time: ")
Packages <- c("dplyr", "readr")
libraries_loading_time <-system.time(
  suppressMessages(lapply(Packages, library, character.only = TRUE)))
print(libraries_loading_time)

print("Data loading time: ")
suppressMessages(data_loading_time <- system.time(hflights <- read_csv("data/hflights.csv")))
print(data_loading_time)

fctr.cols <- sapply(hflights, is.factor)
hflights[, fctr.cols] <- sapply(hflights[, fctr.cols], as.character)
N = 10
time_s = system.time(for (i in 1:N){
  t <- hflights %>%
    # @sample(.4) %>%
    mutate(Speed = Distance / AirTime * 60) %>%
    select(Month, ArrDelay, Speed) %>%
    na.omit() %>%
    group_by(Month) %>%
    summarise(AvgDelay = mean(ArrDelay), MaxSpeed = max(Speed))})
print("Group by time: ")
group_by_time <- time_s / N
print(group_by_time)

strReverse <- function(x)
  sapply(lapply(strsplit(x, NULL), rev), paste, collapse="")

print("String reverse time: ")
string_reverse_time <- system.time(hflights %>% mutate(TailNumRev = strReverse(TailNum)))
print(string_reverse_time)

suppressMessages(log <- read_csv("logs/log.csv"))
DateTime <- Sys.time() %>% as.POSIXct
log <- log %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "HFlights", "TaskNames" = "Library loading", "TaskTypes" = "Loading", "TaskExecTimes" = libraries_loading_time[['elapsed']])) %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "HFlights", "TaskNames" = "File loading", "TaskTypes" = "Execution", "TaskExecTimes" = data_loading_time[['elapsed']])) %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "HFlights", "TaskNames" = "Split-apply-combine", "TaskTypes" = "Execution", "TaskExecTimes" = group_by_time[['elapsed']])) %>%
  bind_rows(list("DateTime" = DateTime, "Language" = "R", "Dataset" = "HFlights", "TaskNames" = "String reversal", "TaskTypes" = "Execution", "TaskExecTimes" = string_reverse_time[['elapsed']]))
write_csv(log, "logs/log.csv")
