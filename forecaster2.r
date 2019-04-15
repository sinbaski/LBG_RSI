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

the.day <- "2018-12-01";

stmt <- paste(
    "select",
    "non_specialized_food_f,",
    "specialist_food_f,",
    "drinks_tobacco_f,",
    "non_specialized_non_food_f,",
    "textiles_f,",
    "clothing_f,",
    "footwear_f,",
    "furniture_f,",
    "electrical_f,",
    "hardware_f,",
    "music_f,",
    "pharmaceutical_etc_f,",
    "books_etc_f,",
    "floor_cover_f,",
    "computer_telcomm_f,",
    "other_specialized_f,",
    "mail_order_f,",
    "other_non_store_f,",
    "fuel_f",
    "from UK_RSI_food",
    "join UK_RSI_non_specialized_non_food",
    "join UK_RSI_textiles_etc",
    "join UK_RSI_household_goods",
    "join UK_RSI_pharmaceutical_etc",
    "join UK_RSI_books_etc",
    "join UK_RSI_floor_cover",
    "join UK_RSI_computer_telcomm",
    "join UK_RSI_other_specialized",
    "join UK_RSI_non_store",
    "join UK_RSI_fuel",
    "on",
    "UK_RSI_food.mon = UK_RSI_non_specialized_non_food.mon",
    "and UK_RSI_food.mon = UK_RSI_textiles_etc.mon",
    "and UK_RSI_food.mon = UK_RSI_household_goods.mon",
    "and UK_RSI_food.mon = UK_RSI_pharmaceutical_etc.mon",
    "and UK_RSI_food.mon = UK_RSI_books_etc.mon",
    "and UK_RSI_food.mon = UK_RSI_floor_cover.mon",
    "and UK_RSI_food.mon = UK_RSI_computer_telcomm.mon",
    "and UK_RSI_food.mon = UK_RSI_other_specialized.mon",
    "and UK_RSI_food.mon = UK_RSI_non_store.mon",
    "and UK_RSI_food.mon = UK_RSI_fuel.mon",
    sprintf("where UK_RSI_food.mon = '%s';", the.day)
);
rs <- dbSendQuery(database, stmt);
forecast <- as.vector(fetch(rs));
dbClearResult(rs);

## Get the weights of the sectors
sector.weights <- rep(NA, length(forecast));
columns <- attributes(forecast)$names;
for (j in 1:length(columns)) {
    columns[j] <- substr(columns[j], start=1, stop=nchar(columns[j])-2);
    stmt <- paste(
        "select weight",
        "from uk_rsi_sector_weights",
        sprintf("where category='%s'", columns[j])
    );
    rs <- dbSendQuery(database, stmt);
    sector.weights[j] <- fetch(rs)$weight;
}
overall <- sum(sector.weights * forecast)/sum(sector.weights);

## For seasonal adjustment:
## seasonal MA: s3x9
## trend MA: Henderson 9
## use 108 months, i.e. 9 years data


rs <- dbSendQuery(
    database,
    paste(
        "select mon, unadjusted from rsi_history",
        "where mon_pub = date_add('", the.day, "', interval -1 month)",
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
out <- seas(
    unadjusted.ts,
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
adjusted <- out$data[, "final"];

rs <- dbSendQuery(
    database,
    paste(
        "select mon, unadjusted, adjusted from rsi_history",
        "where mon_pub='", the.day, "' order by mon desc",
        "limit 2"
    )
);
reported <- fetch(rs);
dbClearResult(rs);
dbDisconnect(database);
