print("Library loading time: ")
Packages <- c("dplyr", "readr", "anytime")
print(system.time(
  suppressMessages(lapply(Packages, library, character.only = TRUE))))
print("Data loading time: ")
print(system.time(suppressMessages(bitstamp <- read_csv("data/bitstamp.csv"))))

bitstamp_dt <- bitstamp %>%
                mutate(Datetime = anytime(Timestamp)) %>%
                mutate(YearMonth = format(Datetime, "%Y-%m"))

N = 10
time_s = system.time(for (i in 1:N){
  t <- bitstamp_dt %>%
      group_by(YearMonth) %>%
      summarise(AvgClose = mean(Close), MaxHigh = max(High))})
print("Group by time: ")
print(time_s / N)
