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
        ##    "H.furniture, H.electrical, H.hardware, H.music",
        paste(columns, collapse=", "),
        "from UK_weather as W join",
        sprintf("UK_RSI_%s as H on", category),
        "W.mon = H.mon",
        sprintf("where H.mon < '%s'", which.mon),
        "order by W.mon desc limit 240;"
    );
    rs <- dbSendQuery(database, stmt);
    data <- fetch(rs);
    dbClearResult(rs);

    M <- 6;
    D <- apply(as.matrix(data[, -1]), MARGIN=2, FUN=rev);
    log.weather <- log(D[, 1:M]);


    if (length(columns) > 1) {
        log.sales <- log(D[, 7:dim(D)[2]]);
        E.sales <- eigen(cov(log.sales));
        ## K <- min(which(cumsum(E.sales$values)/sum(E.sales$values) > 0.98));
        K <- dim(log.sales)[2];
        sales.factor <- log.sales %*% E.sales$vectors[, 1:K];
    } else {
        K <- 1;
        log.sales <- matrix(log(D[, 7:dim(D)[2]]), nrow=dim(D)[1], ncol=1);
        sales.factor <- log.sales;
    }

    regrs.coef <- matrix(0, nrow=M, ncol=K);
    arma.coef <- matrix(NA, nrow=K, ncol=4);
    predictions <- matrix(NA, nrow=K, ncol=2);

    ## A <- apply(abs(log.weather), MARGIN=2, FUN=mean);
    ## B <- apply(abs(sales.factor), MARGIN=2, FUN=mean);
    algorithms <- c(auglag, bobyqa, cobyla, lbfgs, mlsl, mma, sbplx);
    for (i in 1:K) {
        response <- sales.factor[, i];
        rho <- cor(x=log.weather, y=sales.factor[, i]);
        ## selected <- abs(rho) > 0.05;
        selected <- rep(FALSE, length(log.weather));
        if (sum(selected) > 0) {
            explanatory <- as.matrix(log.weather[, selected]);
            mdl <- lm(response~explanatory - 1);
            R <- residuals(mdl);
            regrs.coef[selected, i] <- coef(mdl);

            ## for (algo in algorithms) {
            ##     fit <- algo(
            ##     ## fit <- auglag(
            ##         x0=rep(1, dim(explanatory)[2]),
            ##         fn=function(arg) {
            ##             temp <- sum(
            ##             (sales.factor[, i] - explanatory %*% arg)^2
            ##             );
            ##             temp <- temp * (1 + sum(abs(arg)));
            ##             return(temp);
            ##         },
            ##         lower=-rep(5, dim(explanatory)[2]),
            ##         upper=rep(5, dim(explanatory)[2]),
            ##         control=list(xtol_rel=1.0e-4, maxeval=2000)
            ##         ## xtol_rel=1.0e-4,
            ##         ## maxeval=2000
            ##     );
            ##     if (fit$convergence > 0 && fit$convergence < 5) {
            ##         break;
            ##     }
            ## }
            ## if (fit$convergence < 0) {
            ##     stop(sprintf(
            ##         "Algorithms failed for factor %d: %s", i, fit$message));
            ## } else if (fit$convergence == 5) {
            ##     stop(sprintf(
            ##         "Algorithms failed to converge for factor %d: %s",
            ##         i, fit$message));
            ## }
            ## regrs.coef[selected, i] <- fit$par;
            ## R <- sales.factor[, i];
            ## R <- R - explanatory %*% regrs.coef[selected, i];
        } else {
            R <- response;
            regrs.coef[, i] <- 0;
        }
        ## R <- response;
        ## regrs.coef[, i] <- 0;


        ## Not using weather as explanatory variables
        ## regrs.coef[, i] <- rep(0, M);
        ## R <- sales.factor[, i];

        curaic <- Inf;
        for (p in 0:1) {
            for (q in 0:1) {
                for (sp in 0:0) {
                    for (sq in 0:1) {
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
                    }
                }
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
    factor.pred <- log(X) %*% regrs.coef + predictions[, 1];

    ## Not using weather as explanatory variables.
    ## factor.pred <- predictions[, 1];

    if (dim(log.sales)[2] > K) {
        F <- log.sales %*% E.sales$vectors[, (K+1):dim(log.sales)[2]];
        sales.pred <- c(
            factor.pred,
            apply(F, MARGIN=2, FUN=mean)
        ) %*% t(E.sales$vectors);
        ## sales.pred <- c(
        ##     predictions[, 1],
        ##     apply(F, MARGIN=2, FUN=mean)
        ## ) %*% t(E.sales$vectors);
    } else if (K > 1) { ## dim(log.sales)[2] == K
        sales.pred <- factor.pred %*% t(E.sales$vectors);
    } else { ## dim(log.sales)[2] == K == 1
        sales.pred <- factor.pred;
    }


    indices <- exp(sales.pred);

    stmt <- sprintf(
        "select count(*) from uk_rsi_%s where mon = '%s'",
        category, which.mon
    );
    rs <- dbSendQuery(conn, stmt);
    date.exists <- (fetch(rs)[1] > 0);
    dbClearResult(rs);

    if (! date.exists) {
        stmt <- sprintf("insert into UK_RSI_%s ( mon, ", category);
        stmt <- paste(
            stmt,
            paste(sprintf("%s_f", columns), collapse=", "),
            ") values ('", which.mon, "', ",
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
        stmt <- paste(stmt, sprintf("where mon= '%s';", which.mon));
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

conn = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                 dbname='LBG', host="localhost");

rs <- dbSendQuery(
    conn,
    paste(
        "select A.mon from UK_RSI_fuel as A join UK_weather as B",
        "on A.mon = b.mon",
        "where fuel_f is not NULL",
##        "and mon < '2018-02-01'",
        "order by mon desc"
    )
);
dates <- fetch(rs)$mon;
dbClearResult(rs);

## dates <- "2018-11-01";

for (i in 1:length(dates)) {
    for (j in 1:length(categories)) {
        col_names <- {};
        rs <- dbSendQuery(
            conn, sprintf("show columns in UK_RSI_%s;", categories[j])
        );
        fields <- fetch(rs);
        dbClearResult(rs);
        for (f in fields$Field) {
            if (f == "mon" || endsWith(f, "_f")) next;
            col_names <- append(col_names, f);
        }
        predict.retails(categories[j], col_names, dates[i]);
    }
}
dbDisconnect(conn);
