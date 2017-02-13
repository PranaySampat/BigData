library(nycflights13)
library(maps)
library(dplyr)
library(geosphere)
transaction<- read.csv("C:/Users/priya/Desktop/trans.csv")
map("world", regions= c("india"), fill=T, col="white", bg="grey15" ,ylim=c(7.0,37.0), xlim=c(67.0,98.0))
#create basemap

#overlay transactions
points(transaction$Transaction.Long,transaction$Transaction.Lat, pch=3, cex=0.5, col="chocolate1")
text(transaction$Transaction.Long,transaction$Transaction.Lat,transaction$Transaction.Location,pos = 4,col = "red", cex = 0.4)
points(transaction$L.Transaction.Long,transaction$L.Transaction.Lat, pch=3, cex=0.5, col="chocolate1")
text(transaction$L.Transaction.Long,transaction$L.Transaction.Lat,transaction$BlackListedWithdrawal,pos = 4,col = "red", cex = 0.4)

for (i in (1:dim(transaction)[1])) { 
  inter <- gcIntermediate(c(transaction$Transaction.Long[i],transaction$Transaction.Lat[i]), c(transaction$L.Transaction.Long[i],transaction$L.Transaction.Lat[i]), n=200)
  lines(inter, lwd=0.5, col="turquoise2")    
}


