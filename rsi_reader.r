rm(list=ls());
library(RMySQL);
library(openxlsx);

get.col.num <- function(label)
{
    str <- label;
    m <- 1;
    x <- 0;
    while (nchar(str) > 0) {
        n <- nchar(str);
        x <- x + (utf8ToInt(substr(str, n, n)) - 64) * m;
        m <- m * 26;
        if (n > 1) {
            str <- substr(str, 1, n-1);
        } else {
            break;
        }
    }
    return(x);
}

make.date.str <- function(date.str)
{
    mon <- "";
    str <- substr(date.str, 6, 8);
    if (str == "Jan") {
        mon <- "01";
    } else if (str == "Feb") {
        mon <- "02";
    } else if (str == "Mar") {
        mon <- "03";
    } else if (str == "Apr") {
        mon <- "04";
    } else if (str == "May") {
        mon <- "05";
    } else if (str == "Jun") {
        mon <- "06";
    } else if (str == "Jul") {
        mon <- "07";
    } else if (str == "Aug") {
        mon <- "08";
    } else if (str == "Sep") {
        mon <- "09";
    } else if (str == "Oct") {
        mon <- "10";
    } else if (str == "Nov") {
        mon <- "11";
    } else if (str == "Dec") {
        mon <- "12";
    } else {
        mon <- "00";
    }
    return(paste(substr(date.str, 1, 4), mon, "01", sep="-"));
}

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");
idx1 <- 10;
sheet <- 21;
for (v in 35:47) {
    datafile <- sprintf("data/rsi-v%d.xlsx", v);
    data <- read.xlsx(datafile, sheet=sheet, startRow=idx1, cols=1, colNames=FALSE);
    idx2 <- min(which(!grepl('[0-9]{4} [A-Z][a-z]{2}', data$X1))) + idx1 - 2;
    dates <- data$X1[1:(idx2 - idx1 + 1)];
    dates <- unlist(lapply(dates, make.date.str));
    D <- tail(dates, 1);

    rs <- dbSendQuery(database,  "select distinct grp as G from uk_rsi_data_sheets");
    tables <- fetch(rs);
    dbClearResult(rs);
    for (i in 1:dim(tables)[1]) {
        rs <- dbSendQuery(
            database,
            sprintf(
                "select sector, col from uk_rsi_data_sheets where grp = '%s' order by col",
                tables$G[i]
            )
        );
        sectors <- fetch(rs);
        dbClearResult(rs);

        columns <- unlist(lapply(sectors$col, get.col.num));
        J <- order(columns);
        columns <- columns[J];
        sec <- sectors$sector[J];

        data <- read.xlsx(
            datafile, sheet=21, rows=idx1:idx2, cols=columns,
            skipEmptyRows=FALSE, colNames=FALSE
        );
        L <- lapply(1:dim(data)[1], FUN=function(j) {
            paste("(", sprintf("'%s', '%s',", dates[j], D), paste(data[j, ], collapse=","), ")")
        });
        L <- gsub(intToUtf8(160), "NULL", L);
        stmt <- paste(L, collapse=",");
        stmt <- paste(
            "insert into", tables$G[i], "(mon, mon_pub,", paste(sec, collapse=","),
            ") values", stmt
        );
        rs <- dbSendQuery(database, stmt);
        dbClearResult(rs);
    }

    rs <- dbSendQuery(
        database,
        "select grp, sector, col from uk_rsi_data_sheets"
    );
    df <- fetch(rs);
    dbClearResult(rs);

    columns <- unlist(lapply(df$col, get.col.num));
    P <- order(columns);
    columns <- columns[P];
    sectors <- df$sector[P];
    groups <- df$grp[P];

    df2 <- read.xlsx(
        datafile, sheet=sheet, rows=8,
        cols=columns, colNames=FALSE
    );
    L <- unlist(lapply(df2, FUN=function(s) gsub("\\(£([0-9]+),?([0-9]+)m\\)", "\\1\\2", x=s)));

    for (i in  1:length(columns)) {
        rs <- dbSendQuery(
            database,
            paste(
                "insert into uk_rsi_sector_weights (category, grp, weight, mon_pub) values",
                sprintf("('%s', '%s', %s, '%s');", sectors[i], groups[i], L[i], D)
            )
        );
        dbClearResult(rs);
    }
}
dbDisconnect(database);

