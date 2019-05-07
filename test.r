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

for (i in 35) {
    indices <- matrix(NA, 2, 2);
    ## df1 <- read.xlsx(
    ##     sprintf("data/rsi-v%d.xlsx", i),
    ##     sheet=15, cols=1:2,
    ##     colNames=FALSE, skipEmptyRows=FALSE
    ## );
    df1 <- read.xls(
        sprintf("data/rsi-v%d.xls", i),
        sheet=15, perl="C:/cygwin64/bin/perl",
        blank.lines.skip=FALSE, header=FALSE, skip=10
    );
    idx1 <- which(!grepl("[0-9]{4} [A-Z]{1}[a-z]{2}", df1$V1))[1] - 1;
    df1 <- df1[1:idx1, 1:2];

    ## indices[1, 1] <- which(df1$X1 == '1996 Jan')[1];
    ## A <- grep("[0-9]{4} [A-Z]{1}[a-z]{2}", tail(df1$X1, n=-indices[1, 1]+1));
    ## indices[1, 2] <- indices[1, 1] - 1 + which(diff(A) > 1);

    df2 <- read.xls(
        sprintf("data/rsi-v%d.xls", i),
        sheet=21, perl="C:/cygwin64/bin/perl",
        blank.lines.skip=FALSE, header=FALSE, skip=9
    );
    idx2 <- which(!grepl("[0-9]{4} [A-Z]{1}[a-z]{2}", df2$V1))[1] - 1;
    df2 <- df2[1:idx2, 1:2];


    ## indices[2, 1] <- which(df2$X1 == '1996 Jan')[1];
    ## A <- grep("[0-9]{4} [A-Z]{1}[a-z]{2}", tail(df2$X1, n=-indices[2, 1]+1));
    ## indices[2, 2] <- indices[2, 1] - 1 + which(diff(A) > 1);

    rs <- dbSendQuery(
        database,
        "select distinct mon_pub from rsi_history"
    );
    dates <- fetch(rs)[[1]];
    dbClearResult(rs);

    str <- paste(df1[idx1, 1]);
    str <- paste(sep="-", substr(str, 1, 4), convert.date.str(substr(str, 6, 8)), "01");
    if (str %in% dates) {
        warning(paste("Publication date", str, "already exists"));
        next;
    }

    str <- paste(df2[idx2, 1]);
    str <- paste(sep="-", substr(str, 1, 4), convert.date.str(substr(str, 6, 8)), "01");
    if (str %in% dates) {
        warning(paste("Publication date", str, "already exists"));
        next;
    }

    L <- unlist(lapply(1:idx1, FUN=function(j) nchar(paste(df1[j, 2]))));
    idx <- which(L > 0)[1];
    ## if (indices[1, 2] - indices[1, 1] !=  indices[2, 2] - indices[2, 1] ||
    ##     sum(df1$X1[indices[1, 1]:indices[1, 2]] != df2$X1[indices[2, 1]:indices[2, 2]]) > 0) {
    ##     stop(sprintf("errors in v%d.xlsx", i));
    ## }
    dates <- unlist(lapply(idx:idx1, function(j) {
        str <- df1[j, 1];
        return(paste(sep="-", substr(str, 1, 4), convert.date.str(substr(str, 6, 8)), "01"));
    }));
    last.mon <- tail(dates, 1);
    values = data.frame(
        "mon"=dates,
        "unadjusted"=df1[idx:idx1, 2],
        "adjusted"=df2[idx:idx2, 2]
    );
    stmt <- paste(apply(values, MARGIN=1, FUN=function(x) {
        sprintf("('%s', '%s', '%s', '%s')", x[1], last.mon, x[2], x[3]);
    }), collapse=", ");
    stmt <- paste("insert into rsi_history values", stmt, sep=" ");
    rs <- dbSendQuery(database, stmt);
    dbClearResult(rs);
}
dbDisconnect(database);

