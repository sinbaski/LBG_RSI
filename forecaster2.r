rm(list=ls());
library(sets)
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
srp.days <- fetch(rs);
dbClearResult(rs);

phase.shift <- srp.shift(srp.days[1], srp.days[2]);
bh <- bh.effect(srp.days[1], srp.days[2]);
easter <- easter.effect(srp.days[1], srp.days[2]);
explanatory <- cbind(phase.shift[, c(1:2, 10:12)], bh, easter);

## the.day <- "2018-12-01";

rs <- dbSendQuery(
    database,
    paste(
        "   select mon from uk_rsi_food",
        "   where mon = date_add(mon_pub, interval 1 month)",
        "   and mon in (",
        "   select distinct mon from uk_rsi_food",
        "   where mon = mon_pub",
        "   );"
    )
);
days <- fetch(rs)$mon;

rs <- dbSendQuery(
    database,
    "select distinct grp from uk_rsi_sector_weights"
);
tables <- fetch(rs)$grp;
dbClearResult(rs);

results <- matrix(NA, length(days), 2);
for (d in 1:length(days)) {
    overall <- c(0, 0);
    for (i in 1:length(tables)) {
        rs <- dbSendQuery(
            database,
            paste(
                "select category, weight from uk_rsi_sector_weights",
                sprintf("where grp = '%s'", tables[i]),
                sprintf("and mon_pub = date_add('%s', interval -1 month);", days[d])
            )
        );
        df <- fetch(rs);
        dbClearResult(rs);

        rs <- dbSendQuery(
            database,
            paste(
                "select", paste(paste(df$category, "_f", sep=""), collapse=", "),
                "from", tables[i],
                sprintf("where mon = '%s'", days[d]),
                sprintf("and mon_pub = date_add('%s', interval -1 month)", days[d])
            )
        );
        df2 <- fetch(rs);
        dbClearResult(rs);
        overall[1] <- overall[1] + sum(df2 * df$weight);
        overall[2] <- overall[2] + sum(df$weight);
    }
    overall <- overall[1]/overall[2];

    rs <- dbSendQuery(
        database,
        paste(
            "select mon, unadjusted from rsi_history",
            "where mon_pub = date_add('", days[d], "', interval -1 month)",
            sprintf("and mon < '%s'", days[d]),
            "order by mon desc limit 107"
        )
    );
    df <- fetch(rs);
    dbClearResult(rs);

    unadjusted <- rev(c(overall, df$unadjusted));
    unadjusted.ts <- ts(
        data = unadjusted,
        start=c(as.numeric(substr(tail(df$mon, n=1), start=1, stop=4)),
                as.numeric(substr(tail(df$mon, n=1), start=6, stop=7))),
        frequency=12
    );
    ## For seasonal adjustment:
    ## seasonal MA: s3x9
    ## trend MA: Henderson 9
    ## use 108 months, i.e. 9 years data
    out <- seas(
        unadjusted.ts,
        transform.function="log",
        regression.aictest="user",
        regression.user=sprintf("x%d", 1:dim(explanatory)[2]),
        regression.data=as.vector(t(explanatory)),
        regression.start=gsub(
            "-", ".",
            substr(srp.days[1], start=1, stop=7)
        ),
        x11.seasonalma="s3x9",
        x11.trendma=9
    );
    adjusted <- tail(out$data[, "final"], 1);

    rs <- dbSendQuery(
        database,
        sprintf(
            "select adjusted from rsi_history where mon_pub='%s' and mon_pub = mon",
            days[d]
        )
    );
    reported <- fetch(rs)$adjusted;
    dbClearResult(rs);
    results[d, ] <- c(reported, adjusted);
}
dbDisconnect(database);

deviations <- results[, 2] - results[, 1];

plot(1:dim(results)[1], results[, 1],
     ylim=c(min(results), max(results)),
     type="b", col="#FF0000");
lines(1:dim(results)[1], results[, 2], type="b", col="#0000FF");





