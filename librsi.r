rm(list=ls());
library(sets)
library(RMySQL);
library(seasonal);

lookback.period <- 108;
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

cmpt.index <- function(the.day, pub.day, excl.fuel=FALSE)
{
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");

    rs <- dbSendQuery(
        database,
        paste(
            "select distinct grp from uk_rsi_sector_weights",
            if (excl.fuel) "where grp != 'uk_rsi_fuel';" else ";"
        )
    );
    tables <- fetch(rs)$grp;
    dbClearResult(rs);

    overall <- c(0, 0);
    for (i in 1:length(tables)) {
        rs <- dbSendQuery(
            database,
            paste(
                "select category, weight from uk_rsi_sector_weights",
                sprintf("where grp = '%s'", tables[i]),
                sprintf("and mon_pub = '%s';", pub.day)
            )
        );
        df <- fetch(rs);
        dbClearResult(rs);

        rs <- dbSendQuery(
            database,
            paste(
                "select",
                paste(df$category, collapse=", "),
                "from", tables[i],
                sprintf("where mon = '%s' and mon_pub = '%s'", the.day, pub.day)
            )
        );
        df2 <- fetch(rs);
        dbClearResult(rs);
        overall[1] <- overall[1] + sum(df2 * df$weight);
        overall[2] <- overall[2] + sum(df$weight);
    }
    dbDisconnect(database);
    return(overall[1]/overall[2]);
}

adjust.seasonally <- function(series, start)
{
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");

    rs <- dbSendQuery(
        database,
        "select min(mon), max(mon) from uk_rsi_standard_reporting_period;"
    );
    srp.days <- fetch(rs);
    dbClearResult(rs);
    dbDisconnect(database);

    phase.shift <- srp.shift(srp.days[1], srp.days[2]);
    bh <- bh.effect(srp.days[1], srp.days[2]);
    easter <- easter.effect(srp.days[1], srp.days[2]);
    explanatory <- cbind(phase.shift[, c(1:2, 10:12)], bh, easter);

    unadjusted.ts <- ts(
        data = series,
        start=c(as.numeric(substr(start, start=1, stop=4)),
                as.numeric(substr(start, start=6, stop=7))),
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

    return(out$data[, "final"]);
}

predict.adjusted <- function(days, excl.fuel)
{
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");

    rs <- dbSendQuery(
        database,
        paste(
            "select distinct grp from uk_rsi_sector_weights",
            if (excl.fuel) "where grp != 'uk_rsi_fuel';" else ";"
        )
    );
    tables <- fetch(rs)$grp;
    dbClearResult(rs);

    adjusted.values <- matrix(NA, lookback.period, length(days));
    for (d in 1:length(days)) {
        overall <- c(0, 0);
        for (i in 1:length(tables)) {
            rs <- dbSendQuery(
                database,
                paste(
                    "select category, weight from uk_rsi_sector_weights",
                    sprintf("where grp = '%s'", tables[i]),
                    sprintf(
                        "and mon_pub = date_add('%s', interval -1 month);",
                        days[d]
                    )
                )
            );
            df <- fetch(rs);
            dbClearResult(rs);

            rs <- dbSendQuery(
                database,
                paste(
                    "select",
                    paste(paste(df$category, "_f", sep=""), collapse=", "),
                    "from", tables[i],
                    sprintf("where mon = '%s'", days[d]),
                    sprintf(
                        "and mon_pub = date_add('%s', interval -1 month)",
                        days[d]
                    )
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
                "select distinct mon from uk_rsi_food",
                sprintf(paste(
                    "where mon >= date_add('%1$s', interval -%2$d month)",
                    "and mon < '%1$s'"
                ), days[d], lookback.period - 1)
            )
        );
        df <- fetch(rs);
        dbClearResult(rs);

        unadjusted <- unlist(
            lapply(
                1:dim(df)[1],
                FUN=function(i) {
                    cmpt.index(df$mon[i], tail(df$mon, 1), excl.fuel);
                }
            )
        );
        unadjusted <- c(unadjusted, overall);
        adjusted.values[, d] <- adjust.seasonally(unadjusted, df$mon[1]);
    }
    dbDisconnect(database);
    return(adjusted.values);
}

summarize.forecast <- function(to.forecast, excl.fuel)
{
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");
    rs <- dbSendQuery(
        database,
        paste(
            "select distinct grp from uk_rsi_sector_weights",
            if (excl.fuel) "where grp != 'uk_rsi_fuel';" else ";"
        )
    );
    tables <- fetch(rs)$grp;
    dbClearResult(rs);

    rs <- dbSendQuery(
        database,
        "select count(distinct category) as n from uk_rsi_sector_weights;"
    );
    n <- fetch(rs)$n;
    dbClearResult(rs);

    summary <- as.data.frame(matrix(NA, 1, 7));
    names(summary) <- c("sector", "group", "weight",
                        "unadjusted", "prev_unadjusted",
                        "adjusted", "prev_adjusted");
    for (i in 1:length(tables)) {
        rs <- dbSendQuery(
            database,
            paste(
                "select category, weight from uk_rsi_sector_weights",
                sprintf("where grp = '%s'", tables[i]),
                sprintf(
                    "and mon_pub = date_add('%s', interval -1 month);",
                    to.forecast
                )
            )
        );
        df <- fetch(rs);
        dbClearResult(rs);

        rs <- dbSendQuery(
            database,
            paste(
                "select",
                paste(paste(df$category, "_f", sep=""), collapse=", "),
                "from", tables[i],
                sprintf("where mon = '%s'", to.forecast),
                sprintf(
                    "and mon_pub = date_add('%s', interval -1 month)",
                    to.forecast
                )
            )
        );
        df2 <- fetch(rs);
        dbClearResult(rs);
        names(df2) <- df$category;

        rs <- dbSendQuery(
            database,
            paste(
                "select", paste(df$category, collapse=", "),
                "from", tables[i],
                sprintf(
                    "where mon_pub = date_add('%s', interval -1 month)",
                    to.forecast
                ),
                sprintf("and mon >= date_add('%s', interval -%d month)",
                        to.forecast, lookback.period - 1),
                sprintf("and mon < '%s'", to.forecast),
                "order by mon;"
            )
        );
        df3 <- fetch(rs);
        dbClearResult(rs);
        rs <- dbSendQuery(
            database,
            sprintf("select date_add('%s', interval -%d month) as M;",
                    to.forecast, dim(df3)[1])
        );
        start <- fetch(rs)$M;
        dbClearResult(rs);
        df3 <- rbind(df3, df2);

        N <- dim(df3)[1];
        X <- matrix(NA, dim(df3)[1], dim(df3)[2]);
        for (k in 1:dim(df3)[2]) {
            X[, k] <- adjust.seasonally(df3[, k], start);
        }

        A <- matrix(NA, nrow=length(df$category), ncol=5);
        A[, 1] <- df$weight;
        A[, 2] <- t(df2);
        A[, 3] <- t(df3[N-1, ]);
        A[, 4] <- t(X[N, ]);
        A[, 5] <- t(X[N - 1, ]);
        A <- as.data.frame(A);
        names(A) <- c("weight", "unadjusted", "prev_unadjusted",
                      "adjusted", "prev_adjusted");
        T <- data.frame(
            sector = df$category,
            group = rep(substr(tables[i], 8, nchar(tables[i])),
                      length(df$weight))
        );
        T <- cbind(T, A);
        summary <- rbind(summary, T);
    }
    summary <- tail(summary, n=-1);
    row.names(summary) <- 1:dim(summary)[1];
    dbDisconnect(database);
    return(summary);
}
