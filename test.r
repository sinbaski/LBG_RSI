rm(list=ls());
require(openxlsx);
require(RMySQL);
library(gdata);

convert.date.str <- function(str1)
{
    if (str1 == "Jan") return("01");
    if (str1 == "Feb") return("02");
    if (str1 == "Mar") return("03");
    if (str1 == "Apr") return("04");
    if (str1 == "May") return("05");
    if (str1 == "Jun") return("06");
    if (str1 == "Jul") return("07");
    if (str1 == "Aug") return("08");
    if (str1 == "Sep") return("09");
    if (str1 == "Oct") return("10");
    if (str1 == "Nov") return("11");
    if (str1 == "Dec") return("12");
}

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");

sheet1 <- 15;
sheet2 <- 21;

## sheet1 <- 19;
## sheet2 <- 25;

## sheet1 <- 18;
## sheet2 <- 24;

for (i in 20:23) {
    df1 <- read.xls(
        sprintf("data/rsi-v%d.xls", i),
        sheet=sheet1, perl="C:/cygwin64/bin/perl",
        blank.lines.skip=FALSE, header=FALSE, skip=0, nrows=5
    );

    df1 <- read.xls(
        sprintf("data/rsi-v%d.xls", i),
        sheet=sheet1, perl="C:/cygwin64/bin/perl",
        blank.lines.skip=FALSE, header=FALSE, skip=10
    );
    idx1 <- which(!grepl("[0-9]{4} [A-Z]{1}[a-z]{2}", df1$V1))[1] - 1;
    df1 <- df1[1:idx1, 1:2];

    df2 <- read.xls(
        sprintf("data/rsi-v%d.xls", i),
        sheet=sheet2, perl="C:/cygwin64/bin/perl",
        blank.lines.skip=FALSE, header=FALSE, skip=0, nrows=5
    );
    df2 <- read.xls(
        sprintf("data/rsi-v%d.xls", i),
        sheet=sheet2, perl="C:/cygwin64/bin/perl",
        blank.lines.skip=FALSE, header=FALSE, skip=9
    );
    idx2 <- which(!grepl("[0-9]{4} [A-Z]{1}[a-z]{2}", df2$V1))[1] - 1;
    df2 <- df2[1:idx2, 1:2];

    A <- unlist(lapply(1:length(df2$V2), FUN=function(k) nchar(paste(df2$V2[k]))));
    df1 <- df1[which(A > 0), ];
    df2 <- df2[which(A > 0), ];

    dates <- unlist(lapply(1:dim(df1)[1], function(j) {
        str <- df1[j, 1];
        return(paste(sep="-", substr(str, 1, 4), convert.date.str(substr(str, 6, 8)), "01"));
    }));
    last.mon <- tail(dates, 1);
    stmt <- paste((lapply(1:dim(df1)[1], FUN=function(k) {
        sprintf("('%s', '%s', '%s', '%s')", dates[k], last.mon, df2[k, 2], df1[k, 2]);
    })), collapse=", ");
    stmt <- paste("insert into rsi_history values", stmt, sep=" ");
    rs <- dbSendQuery(
        database,
        sprintf("delete from rsi_history where mon_pub = '%s';", last.mon)
    );
    dbClearResult(rs);
    rs <- dbSendQuery(database, stmt);
    dbClearResult(rs);
}
dbDisconnect(database);

