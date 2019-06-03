rm(list=ls());
source("./librsi.r");

to.forecast <- "2019-04-01";
summary <- summarize.forecast(to.forecast, FALSE);
changes <- summary[, "adjusted"]/summary[, "prev_adjusted"] - 1;
C <- sprintf("%.1f\\%%", changes * 100);
D <- summary[, "weight"]/sum(summary[, "weight"]);
idx <- order(D, decreasing=TRUE);
D <- sprintf("%.1f\\%%", 100 * D);

cbind(summary[, c("sector", "group", "weight")], D, C)[idx, ]

## adjusted <- corrected.forecast(to.forecast);
adjusted <- predict.adjusted(to.forecast, FALSE);
A <- adjusted[lookback.period, 1]/adjusted[lookback.period - 1, 1] - 1;

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");
rs <- dbSendQuery(
    database,
    paste(
        "select distinct mon_pub as M from uk_rsi_food",
        "order by mon_pub desc limit 20;"
    )
);
days <- fetch(rs)$M;
dbClearResult(rs);
dbDisconnect(database);

overall <- unlist(lapply(days, FUN=function(s) cmpt.index(s, s)));

df <- data.frame(months=days, value=overall);

## results <- matrix(NA, length(days), 4);


## for (d in 1:length(days)) {
##     overall <- c(0, 0);
##     for (i in 1:length(tables)) {
##         rs <- dbSendQuery(
##             database,
##             paste(
##                 "select category, weight from uk_rsi_sector_weights",
##                 sprintf("where grp = '%s'", tables[i]),
##                 sprintf("and mon_pub = date_add('%s', interval -1 month);", days[d])
##             )
##         );
##         df <- fetch(rs);
##         dbClearResult(rs);

##         rs <- dbSendQuery(
##             database,
##             paste(
##                 "select", paste(paste(df$category, "_f", sep=""), collapse=", "),
##                 "from", tables[i],
##                 sprintf("where mon = '%s'", days[d]),
##                 sprintf("and mon_pub = date_add('%s', interval -1 month)", days[d])
##             )
##         );
##         df2 <- fetch(rs);
##         dbClearResult(rs);
##         overall[1] <- overall[1] + sum(df2 * df$weight);
##         overall[2] <- overall[2] + sum(df$weight);
##     }
##     overall <- overall[1]/overall[2];

##     rs <- dbSendQuery(
##         database,
##         paste(
##             "select mon, unadjusted from rsi_history",
##             "where mon_pub = date_add('", days[d], "', interval -1 month)",
##             sprintf("and mon < '%s'", days[d]),
##             "order by mon desc limit 107"
##         )
##     );
##     df <- fetch(rs);
##     dbClearResult(rs);

##     unadjusted <- rev(c(overall, df$unadjusted));
##     unadjusted.ts <- ts(
##         data = unadjusted,
##         start=c(as.numeric(substr(tail(df$mon, n=1), start=1, stop=4)),
##                 as.numeric(substr(tail(df$mon, n=1), start=6, stop=7))),
##         frequency=12
##     );
##     ## For seasonal adjustment:
##     ## seasonal MA: s3x9
##     ## trend MA: Henderson 9
##     ## use 108 months, i.e. 9 years data
##     out <- seas(
##         unadjusted.ts,
##         transform.function="log",
##         regression.aictest="user",
##         regression.user=sprintf("x%d", 1:dim(explanatory)[2]),
##         regression.data=as.vector(t(explanatory)),
##         regression.start=gsub(
##             "-", ".",
##             substr(srp.days[1], start=1, stop=7)
##         ),
##         x11.seasonalma="s3x9",
##         x11.trendma=9
##     );
##     adjusted <- tail(out$data[, "final"], 1);

##     rs <- dbSendQuery(
##         database,
##         sprintf(
##             "select unadjusted, adjusted from rsi_history where mon_pub='%s' and mon_pub = mon",
##             days[d]
##         )
##     );
##     reported <- as.matrix(fetch(rs));
##     dbClearResult(rs);
##     results[d, ] <- c(reported[1, 1], reported[1, 2], overall, adjusted);
## }
## dbDisconnect(database);

## N <- dim(results)[1];
## J <- 18:N;

## D <- cbind(results[, 3]/results[, 1] - 1, results[, 4]/results[, 2] - 1);
## A <- apply(D[J, ], MARGIN=2, FUN=mean);
## ## apply(D[J, ], MARGIN=2, FUN=function(x) mean(abs(x)));
## unadjusted <- results[J, 3] / (1 + A[1]);
## adjusted <- results[J, 4] / (1 + A[2]);

## ## mean(unadjusted/results[J, 1] - 1)
## ## mean(adjusted/results[J, 2] - 1)

## apply(D[J, ], MARGIN=2, FUN=sd);
## c(sd(unadjusted/results[J, 1] - 1), sd(adjusted/results[J, 2] - 1))

## pdf("f45cef54.pdf")
## par(mfrow=c(2, 1), cex.axis=0.65);
## plot(
##     J, results[J, 1],
##     ylim=c(min(c(unadjusted, adjusted)), max(c(unadjusted, adjusted))),
##     type="b", col="#000000",
##     xaxt='n', xlab="", ylab=""
## );
## lines(J, unadjusted, type="b", col="#0000FF");
## axis(side=1, at=J, labels=substr(days[J],1 ,7), las=2);
## legend("topleft", legend=c("reported", "forecasted"),
##        col=c("#000000", "#0000FF"), lwd=c(1,1));

## plot(
##     J, results[J, 2],
##     ylim=c(min(c(unadjusted, adjusted)), max(c(unadjusted, adjusted))),
##     type="b", col="#000000",
##     xaxt='n', xlab="", ylab=""
## );
## lines(J, adjusted, type="b", col="#0000FF");
## axis(side=1, at=J, labels=substr(days[J], 1, 7), las=2);
## legend("topleft", legend=c("reported", "forecasted"),
##        col=c("#000000", "#0000FF"), lwd=c(1,1));
## dev.off();



## database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
##                      dbname='LBG', host="localhost");
## rs <- dbSendQuery(
##     database,
##     "select distinct category, grp from uk_rsi_sector_weights order by grp"
## );
## df <- fetch(rs);
## dbClearResult(rs);

## ## performance <- matrix(NA, dim(df)[1], 2);
## performance <- data.frame(mean=rep(NA, dim(df)[1]), sd=rep(NA, dim(df)[1]));
## for (i in 1:dim(df)[1]) {
##     rs <- dbSendQuery(
##         database,
##         paste(
##             sprintf("select avg(A.%1$s_f/B.%1$s - 1) as M, std(A.%1$s_f/B.%1$s - 1) as S", df$category[i]),
##             sprintf("from %1$s as A join %1$s as B", df$grp[i]),
##             "on B.mon = A.mon",
##             "and A.mon = date_add(A.mon_pub, interval 1 month)",
##             "and B.mon = B.mon_pub",
##             "where B.mon >= '2018-03-01';"
##         )
##     )
##     df2 <- fetch(rs);
##     dbClearResult(rs);
##     performance[i, ] <- df2;
## }
## performance <- cbind(sector=df$category, group=df$grp, performance);
## A <- unlist(lapply(1:dim(performance)[1], FUN=function(i) {
##     sprintf(
##         "%s, %s, %3.1f%%, % 3.1f%%",
##         performance[i, 1], performance[i, 2], performance[i, 3] * 100, performance[i, 4] * 100
##     )
##     ## sprintf("%s & %.3f & %.3f \\", performance[i, 1], performance[i, 2], performance[i, 3])
## }));






