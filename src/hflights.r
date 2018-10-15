suppressMessages(library(dplyr))

hflights <- read.csv("data/hflights.csv")

N = 10
start.time <- Sys.time()
for (i in 1:N){
t <- hflights %>%
    # @sample(.4) %>%
    mutate(Speed = Distance / AirTime * 60) %>%
    select(Month, ArrDelay, Speed) %>%
    na.omit() %>%
    group_by(Month) %>%
    summarise(AvgDelay = mean(ArrDelay), MaxSpeed = max(Speed))}
end.time <- Sys.time()
time.taken <- (end.time - start.time) / N
time.taken
