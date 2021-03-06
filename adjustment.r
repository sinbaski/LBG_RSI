rm(list=ls());
library(RMySQL);
library(seasonal);

srp.shift <- function(d1, d2) {
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");

    rs <- dbSendQuery(
        database,
        paste(
            "select count(*) as n from uk_rsi_standard_reporting_period",
            "where mon between '", d1, "' and '", d2, "'"
        )
    );
    N <- fetch(rs)$n;
    dbClearResult(rs);

    phase.shift <- matrix(NA, nrow=N, ncol=12);

    for (i in 1:12) {
        rs <- dbSendQuery(
            database,
            paste(
                "select mon, ",
                "(case month(mon) when ", i,
                "then datediff(mon, d1) - avgs.mean else 0 end) as shift",
                "from uk_rsi_standard_reporting_period as srp",
                "join (",
                "select",
                "month(mon) as mth,",
                "(avg(datediff(mon, d1))) as mean",
                "from uk_rsi_standard_reporting_period",
                "where year(mon) >= 1986",
                "group by month(mon)",
                ") as avgs",
                "on month(srp.mon) = avgs.mth",
                "where mon between '", d1, "' and '", d2, "'",
                "order by mon"
            )
        );
        data <- fetch(rs);
        dbClearResult(rs);
        phase.shift[, i] <- data$shift;
    }
    dbDisconnect(database);
    return(phase.shift);
}

bh.effect <- function(d1, d2) {
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");

    rs <- dbSendQuery(
        database,
        paste(
            "select count(*) as n from uk_rsi_standard_reporting_period",
            "where mon between '", d1, "' and '", d2, "'"
        )
    );
    N <- fetch(rs)$n;
    dbClearResult(rs);

    bh <- matrix(0, N, 2);
    rs <- dbSendQuery(
        database,
        paste(
            "select T1.mon, T2.mth, T1.bh - T2.mean as val",
            "from bank_holiday_effects as T1 join",
            "(",
            "	select month(mon) as mth, avg(bh) as mean",
            "       from bank_holiday_effects",
            "	where year(mon) >= 1986",
            "	group by month(mon)",
            ") as T2",
            "on month(T1.mon) = T2.mth",
            "where mon between '", d1, "' and '", d2, "'",
            "order by T1.mon"
        )
    );
    data <- fetch(rs);
    dbClearResult(rs);

    may <- data$mth %in% c(5, 6);
    bh[may, 1] <- data$val[may];
    august <- data$mth %in% c(8, 9);
    bh[august, 2] <- data$val[august];

    dbDisconnect(database);
    return(bh);
}

easter.effect <- function(d1, d2)
{
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");

    rs <- dbSendQuery(
        database,
        paste(
            "select count(*) as n from uk_rsi_standard_reporting_period",
            "where mon between '", d1, "' and '", d2, "'"
        )
    );
    N <- fetch(rs)$n;
    dbClearResult(rs);

    A <- matrix(0, N, 3);
    for (i in 1:3) {
        rs <- dbSendQuery(
            database,
            "drop view if exists temp;"
        );
        dbClearResult(rs);

        rs <- dbSendQuery(
            database,
            paste(
                "create view temp as ",
                "select mon, category,",
                "case",
                "when month(mon) = 3 and category = ", i, " then 0.8",
                "when month(mon) = 4 and category = ", i, " then -1",
                "else 0",
                "end as val",
                "from uk_rsi_standard_reporting_period as T1",
                "join (",
                "select year(mon) as Y,",
                "case datediff(easter_sunday(year(mon)), d1) div 7",
                "when -2 then 1",
                "when -1 then 1",
                "when 0 then 2",
                "when 1 then 3",
                "when 2 then 4",
                "when 3 then 4",
                "end as category",
                "from uk_rsi_standard_reporting_period",
                "where month(mon) = 4",
                ") as T2",
                "on year(mon) = Y"
            )
        );
        dbClearResult(rs);

        rs <- dbSendQuery(
            database,
            paste(
                "select mon, val - mean as V",
                "from temp as T1 join (",
                "     select month(mon) as mth, avg(val) as mean",
                "     from temp",
                "     where year(mon) >= 1986",
                "     group by month(mon)",
                ") as T2",
                "on month(mon) = mth",
                "where mon between '", d1, "' and '", d2, "'",
                "order by mon;"
            )
        );
        data <- fetch(rs);
        dbClearResult(rs);
        A[, i] <- data$V;
    }
    dbDisconnect(database);
    return(A);
}


database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");

rs <- dbSendQuery(
    database,
    "select min(mon), max(mon) from uk_rsi_standard_reporting_period;"
);
days <- fetch(rs);
dbClearResult(rs);

phase.shift <- srp.shift(days[1], days[2]);
bh <- bh.effect(days[1], days[2]);
easter <- easter.effect(days[1], days[2]);
explanatory <- cbind(phase.shift[, c(1:2, 10:12)], bh, easter);
## explanatory <- cbind(phase.shift, bh, easter);

sma <- c("s3x1", "s3x3", "s3x5", "s3x9", "s3x15", "stable", "x11default");
trendma <- c(9, 13, 23);

rs <- dbSendQuery(
    database,
    paste(
        "select distinct mon_pub from rsi_history",
        "where year(mon_pub) < year(curdate())"
    )
);
mon.pub <- fetch(rs)[[1]];
dbClearResult(rs);

trendma <- seq(from=5, to=31, by=2);
## scores <- matrix(NA, nrow=length(sma), ncol=length(trendma));
scores <- matrix(NA, nrow=1, ncol=length(trendma));
## for (a in 1:length(sma)) {
for (a in 4) {
    for (b in 1:length(trendma)) {
        errors <- rep(NA, length(mon.pub) - 1);
        for (i in 1:length(errors)) {
            stmt <- paste(
                "select mon, unadjusted, adjusted from rsi_history",
                "where mon_pub = '", mon.pub[i], "'",
                "order by mon"
            );
            rs <- dbSendQuery(database, stmt);
            series <- fetch(rs);
            dbClearResult(rs);

            unadjusted <- ts(
                data = series$unadjusted,
                start=c(as.numeric(substr(series$mon[1], start=1, stop=4)),
                        as.numeric(substr(series$mon[1], start=6, stop=7))),
                frequency=12
            );
            out <- seas(
                unadjusted,
                transform.function="log",
                regression.aictest="user",
                ## regression.aictest="user,td",
                regression.user=sprintf("x%d", 1:dim(explanatory)[2]),
                regression.data=as.vector(t(explanatory)),
                regression.start=gsub(
                    "-", ".",
                    substr(days[1], start=1, stop=7)
                ),
                x11.seasonalma=sma[a],
                x11.trendma=trendma[b]
            );
            rs <- dbSendQuery(
                database,
                paste(
                    "select adjusted from rsi_history",
                    "where mon = '", mon.pub[i], "'",
                    "and mon_pub = '", mon.pub[i], "'"
                )
            );
            adjusted <- fetch(rs)[[1]];
            errors[i] <- tail(out$data[, "final"], 1)/adjusted - 1;
            ## errors <- tail(out$data[, "final"], 1)/adjusted - 1;
            dbClearResult(rs);
        }
        scores[1, b] <- mean(abs(errors));
   }
}

## M <- matrix(NA, length(mon.pub)-1, 2);
M <- rep(NA, length(mon.pub));
for (n in seq(from=96, by=12, to=240)) {
    V <- matrix(NA, length(mon.pub), 2);
    for (i in 1:length(mon.pub)) {
        stmt <- paste(
            "select mon, unadjusted, adjusted from rsi_history",
            "where mon_pub = '", mon.pub[i], "'",
            "order by mon desc limit ", n
        );
        rs <- dbSendQuery(database, stmt);
        series <- fetch(rs);
        dbClearResult(rs);

        N <- dim(series)[1];
        unadjusted <- ts(
            data = rev(series[, "unadjusted"]),
            start=c(as.numeric(substr(series[N, "mon"], start=1, stop=4)),
                    as.numeric(substr(series[N, "mon"], start=6, stop=7))),
            frequency=12
        );
        out <- seas(
            unadjusted,
            transform.function="log",
            regression.aictest="user",
            regression.user=sprintf("x%d", 1:dim(explanatory)[2]),
            regression.data=as.vector(t(explanatory)),
            regression.start=gsub(
                "-", ".",
                substr(days[1], start=1, stop=7)
            ),
            x11.seasonalma="s3x9",
            x11.trendma=9
        );
        V[i, ] <- c(tail(out$data[, "final"], 1), series[1, "adjusted"]);
        dbClearResult(rs);
    }
    M <- cbind(M, V);
}
M <- M[, -1];

results <- unlist(lapply(
    seq(from=1, by=2, to=dim(M)[2]),
    FUN=function(i) mean(abs(M[, i] - M[, i+1]))
));
which.min(unlist(results));
dbDisconnect(database);

