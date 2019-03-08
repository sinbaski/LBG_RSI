rm(list=ls());
library(RMySQL);
library(seasonal);

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");

rs <- dbSendQuery(
    database,
    paste(
        "select count(*) from uk_rsi_standard_reporting_period",
        "where year(mon) >= 1992"
    )
);
N <- fetch(rs)[1, 1];
dbClearResult(rs);

phase.shift <- matrix(NA, nrow=N, ncol=12);

for (i in 1:12) {
    rs <- dbSendQuery(
        database,
        paste(
            ## "select mon, (case month(mon) when", i,
            ## "then datediff(mon, d1) else 0 end) as shift",
            ## "from uk_rsi_standard_reporting_period",
            ## "where year(mon) >= 1992",
            ## "order by mon"
            "select mon, ",
            "(case month(mon) when ", i,
            "then datediff(mon, d1) - avgs.mean else 0 end) as shift",
            "from uk_rsi_standard_reporting_period as srp",
            "join (",
            "select",
            "month(mon) as mth,",
            "(avg(datediff(mon, d1))) as mean",
            "from uk_rsi_standard_reporting_period",
            "where year(mon) >= 1992",
            "group by month(mon)",
            ") as avgs",
            "on month(srp.mon) = avgs.mth",
            "where year(mon) >= 1992",
            "order by srp.mon;"
        )
    );
    data <- fetch(rs);
    dbClearResult(rs);
    phase.shift[, i] <- data$shift;
}

## rs <- dbSendQuery(
##     database,
##     paste(
##         "select",
##         "month(mon) as mth,",
##         "(avg(datediff(mon, d1))) as mean",
##         "from uk_rsi_standard_reporting_period",
##         "where year(mon) >= 1992",
##         "group by month(mon)"
##     )
## );
## monthly.means <- fetch(rs)$mean;
## data <- sapply(1:12, FUN=function(i) phase.shift[, i] - monthly.means[i]);
dbDisconnect(database);
