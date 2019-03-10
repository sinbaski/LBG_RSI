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
                "where year(mon) >= 1992",
                "group by month(mon)",
                ") as avgs",
                "on month(srp.mon) = avgs.mth",
                "where mon between '", d1, "' and '", d2, "'",
                "order by mon"
            )
        );
        data <- fetch(rs);
        dbClearResult(rs);
        phase.shift[, i] <- rev(data$shift);
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
            "	where year(mon) >= 1992",
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

easter.effect <- function(d1, d2) {
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
    paste(
        "select",
        "T1.mon, T1.overall as A, T2.adjusted as B",
        "from uk_rsi_overall_unadjusted as T1 join",
        "uk_rsi_adjusted_overall as T2",
        ## "T1.mon, T1.non_specialized_food as A, T2.non_specialized_food as B",
        ## "from uk_rsi_food as T1 join",
        ## "uk_rsi_adjusted as T2",
        "on T1.mon = T2.mon",
        ## "where T1.non_specialized_food is not NULL",
        "where T1.overall is not NULL",
        "order by T1.mon desc",
        "limit 120"
    )
);
rsi.food <- fetch(rs);
dbClearResult(rs);
dbDisconnect(database);


X <- rev(log(rsi.food$A));
N <- length(X);
phase.shift <- srp.shift(rsi.food$mon[N], rsi.food$mon[1]);
bh <- bh.effect(rsi.food$mon[N], rsi.food$mon[1]);
easter <- easter.effect(rsi.food$mon[N], rsi.food$mon[1]);

explanatory <- cbind(phase.shift, bh, easter);



mdl <- lm(X~explanatory);

R <- residuals(mdl);

food <- ts(
    data=residuals(mdl),
    start=c(as.numeric(substr(rsi.food$mon[N], start=1, stop=4)),
            as.numeric(substr(rsi.food$mon[N], start=6, stop=7))),
    frequency=12
);

sma <- c("s3x1", "s3x3", "s3x5", "s3x9", "s3x15", "stable", "x11default");
trendma <- c(9, 13, 23);

scores <- matrix(NA, nrow=length(sma), ncol=length(trendma));
reported <- rev(rsi.food$B);
for (a in 1:length(sma)) {
    for (b in 1:length(trendma)) {
        out <- seas(
            food,
            transform.function="none",
            x11.mode="add",
            regression.aictest="(td)",
            x11.seasonalma=sma[a],
            x11.trendma=trendma[b]
        );
        x <- coef(mdl)[1] + explanatory %*% coef(mdl)[-1] + out$data[, "final"];
        ## x <- explanatory %*% coef(mdl) + out$data[, "final"];
        adjusted <- exp(x);
        s <- tail(x, n=-1) - head(log(reported), n=-1);
        s <- s - diff(log(reported));
        scores[a, b] <- mean(abs(s));
    }
}

out <- seas(
    food,
    transform.function="none",
    x11.mode="add",
    regression.aictest="(td)",
    x11.seasonalma=sma[2],
    x11.trendma=trendma[3]
);

x <- coef(mdl)[1] + explanatory %*% coef(mdl)[-1] + out$data[, "final"];
## x <- explanatory %*% coef(mdl) + out$data[, "final"];
adjusted <- exp(x);
Y <- cbind(adjusted, rev(rsi.food$B));
Z <- tail(x, n=-1) - head(log(reported), n=-1);
Z <- cbind(Z, diff(log(reported)));
tail(Z);

