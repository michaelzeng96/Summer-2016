x <- 1:24
y <- c()
for(i in 1:24){
  y[i] <- vectorOfProbabilites[i]
}

plot(x,y, xlab="hours of the day",ylab="accpetance rate")