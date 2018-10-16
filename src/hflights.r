suppressMessages(library(dplyr))

hflights <- read.csv("data/hflights.csv")
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
print(time_s / N)

strReverse <- function(x)
    sapply(lapply(strsplit(x, NULL), rev), paste, collapse="")

print("String reverse time: ")
print(system.time(hflights %>% mutate(TailNumRev = strReverse(TailNum))))
