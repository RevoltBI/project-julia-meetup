suppressMessages(library(dplyr))

hflights <- read.csv("data/hflights.csv")


start.time <- Sys.time()
t <- hflights %>%
    # @sample(.4) %>%
    mutate(Speed = Distance / AirTime * 60) %>%
    select(Month, ArrDelay, Speed) %>%
    na.omit() %>%
    group_by(Month) %>%
    summarise(AvgDelay = mean(ArrDelay), MaxSpeed = max(Speed))
end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
