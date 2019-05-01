rm(list=ls());
library(fBasics);
library(RMySQL);
library("nloptr");

predict.retails <- function(category, columns, which.mon)
{
    database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                         dbname='LBG', host="localhost");

    stmt <- paste(
        "select W.mon,",
        "W.Tmin + 273.15 as Tmin,",
        "W.Tmean + 273.15 as Tmean,",
        "W.Tmax + 273.15 as Tmax,",
        "W.sunshine, W.rainfall, W.raindays1mm,",
        paste(columns, collapse=", "),
        "from UK_weather as W join",
        sprintf("UK_RSI_%s as H on", category),
        "W.mon = H.mon",
        sprintf("where H.mon_pub = date_add('%s', interval -1 month)", which.mon),
        sprintf("and H.mon < '%s'", which.mon),
        "order by W.mon desc limit 120;"
        ## "order by W.mon desc limit 240;"
    );
    rs <- dbSendQuery(database, stmt);
    data <- fetch(rs);
    dbClearResult(rs);

    D <- apply(as.matrix(data[, -1]), MARGIN=2, FUN=rev);
    log.weather <- log(D[, 1:6]);
    E.weather <- eigen(cov(log.weather));
    weather.factor <- log.weather %*% E.weather$vectors[, 1:2];
    weather.factor.avg <- apply(weather.factor, MARGIN=2, FUN=mean);
    weather.factor <- sapply(1:2, FUN=function(i) weather.factor[, i] - weather.factor.avg[i]);

    if (length(columns) > 1) {
        log.sales <- as.matrix(log(D[, 7:dim(D)[2]]));
        E.sales <- eigen(cov(log.sales));
        ## K <- min(which(cumsum(E.sales$values)/sum(E.sales$values) > 0.98));
        K <- dim(log.sales)[2];
        sales.factor <- log.sales %*% E.sales$vectors[, 1:K];
    } else {
        K <- 1;
        log.sales <- matrix(log(D[, 7:dim(D)[2]]), nrow=dim(D)[1], ncol=1);
        sales.factor <- log.sales;
    }

    regrs.coef <- matrix(0, nrow=dim(weather.factor)[2], ncol=K);
    arma.coef <- matrix(NA, nrow=K, ncol=4);
    predictions <- matrix(NA, nrow=K, ncol=2);

    ## algorithms <- c(auglag, bobyqa, cobyla, lbfgs, mlsl, mma, sbplx);
    for (i in 1:K) {
        response <- sales.factor[, i];

        mdl1 <- lm(response~weather.factor[, 1] - 1);
        mdl2 <- lm(response~weather.factor - 1);
        selected <- if (AIC(mdl1) < AIC(mdl2)) 1 else 1:2;

        if (sum(selected) > 0) {
            explanatory <- weather.factor[, selected];
            mdl <- lm(response~explanatory - 1);
            R <- residuals(mdl);
            regrs.coef[selected, i] <- coef(mdl);
        } else {
            R <- response;
            regrs.coef[, i] <- 0;
        }
        curaic <- Inf;
        for (p in 0:1) {
            for (q in 0:1) {
                sp <- 0;
                sq <- 1;
                ## for (sp in 0:0) {
                ## for (sq in 0:1) {
                tryCatch({
                    mdl <- arima(
                        R, order=c(p, 1, q),
                        seasonal=list(period=12, order=c(sp, 1, sq))
                    );
                    if (AIC(mdl) < curaic) {
                        arma.coef[i, 1] <- {
                            if (p > 0) mdl$coef[p] else 0;
                        }
                        arma.coef[i, 2] <- {
                            if (q > 0) mdl$coef[p+q] else 0;
                        }
                        arma.coef[i, 3] <- {
                            if (sp > 0) mdl$coef[p+q+sp] else 0;
                        }
                        arma.coef[i, 4] <- {
                            if (sq > 0) mdl$coef[p+q+sp+sq] else 0;
                        }
                        curaic <- AIC(mdl);
                        pred <- predict(mdl, n.ahead=1);
                        predictions[i, 1] <- pred$pred;
                        predictions[i, 2] <- pred$se;
                    }
                }, error = function(cond) {
                    message(sprintf(
                        paste("Failed to fit model",
                              "{(%d, 1, %d), 12, (%d, 1, %d)}",
                              "for component %d of %s. %s."),
                        p, q, sp, sq, i, category, which.mon
                    ));
                }, warning=function(cond) {
                    message(sprintf(
                        paste("Warned while fitting model",
                              "{(%d, 1, %d), 12, (%d, 1, %d)}",
                              "for component %d of %s. %s."),
                        p, q, sp, sq, i, category, which.mon
                    ))
                });
                ## }
                ## }
            }
        }
        if (curaic == Inf) {
            stop(sprintf("Failed to fit a model to component %d of %s. %s",
                         i, category, which.mon));
        }
    }

    stmt <- paste(
        "select Tmin+273.15, Tmean+273.15, Tmax+273.15,",
        "sunshine, rainfall, raindays1mm",
        sprintf("from UK_weather where mon='%s';", which.mon)
    );
    rs <- dbSendQuery(database, stmt);
    X <- as.matrix(fetch(rs));
    dbClearResult(rs);

    Y <- log(X) %*% E.weather$vectors[, 1:2];
    Y <- sapply(1:2, FUN=function(i) Y[, i] - weather.factor.avg[i]);

    factor.pred <- Y %*% regrs.coef + predictions[, 1];

    if (dim(log.sales)[2] > K) {
        F <- log.sales %*% E.sales$vectors[, (K+1):dim(log.sales)[2]];
        sales.pred <- c(
            factor.pred,
            apply(F, MARGIN=2, FUN=mean)
        ) %*% t(E.sales$vectors);
    } else if (K > 1) { ## dim(log.sales)[2] == K
        sales.pred <- factor.pred %*% t(E.sales$vectors);
    } else { ## dim(log.sales)[2] == K == 1
        sales.pred <- factor.pred;
    }
    indices <- exp(sales.pred);

    stmt <- sprintf(
        paste(
            "select count(*) from uk_rsi_%s where mon = '%s'",
            "and mon_pub = date_add('%s', interval -1 month);"
            ),
        category, which.mon, which.mon
    );
    rs <- dbSendQuery(conn, stmt);
    date.exists <- (fetch(rs)[1] > 0);
    dbClearResult(rs);

    if (! date.exists) {
        stmt <- sprintf("insert into UK_RSI_%s (mon, mon_pub, ", category);
        stmt <- paste(
            stmt,
            paste(sprintf("%s_f", columns), collapse=", "),
            sprintf(") values ('%s', ", which.mon),
            sprintf("date_add('%s', interval -1 month), ", which.mon),
            paste(indices, collapse=","), ")"
        );
    } else {
        stmt <- sprintf("update UK_RSI_%s set", category);
        for (i in 1:length(columns)) {
            if (i < length(columns)) {
                temp <- sprintf(" %s_f = %f,", columns[i], indices[i]);
            } else {
                temp <- sprintf(" %s_f = %f", columns[i], indices[i]);
            }
            stmt <- paste(stmt, temp);
        }
        stmt <- paste(
            stmt,
            sprintf("where mon= '%s'", which.mon),
            sprintf("and mon_pub = date_add('%s', interval -1 month);", which.mon)
        );
    }
    rs <- dbSendQuery(database, stmt);
    dbClearResult(rs);
    dbDisconnect(database);
}

categories <- c(
    "food",
    "non_specialized_non_food",
    "textiles_etc",
    "household_goods",
    "pharmaceutical_etc",
    "books_etc",
    "floor_cover",
    "computer_telcomm",
    "other_specialized",
    "non_store",
    "fuel"
);

days <- c(
    ## "2018-03-01",
    ## "2018-04-01",
    ## "2018-05-01",
    ## "2018-06-01",
    ## "2018-07-01"
    "2019-04-01"
);

conn = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                 dbname='LBG', host="localhost");

## the.day <- "2018-08-01"
for (the.day in days) {
    for (j in 1:length(categories)) {
        col_names <- {};
        rs <- dbSendQuery(
            conn, sprintf("show columns in UK_RSI_%s;", categories[j])
        );
        fields <- fetch(rs);
        dbClearResult(rs);
        for (f in fields$Field) {
            if (f == "mon" || f == "mon_pub" || endsWith(f, "_f")) next;
            col_names <- append(col_names, f);
        }
        predict.retails(categories[j], col_names, the.day);
    }
}
dbDisconnect(conn);
