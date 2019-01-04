rm(list=ls());
library(fBasics);
library(RMySQL);
library("nloptr");

sigmoid <- function(x) 1/(1 + exp(-x));
database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");

## stmt <- paste(
##     "select",
##     "C.released_on, ",
##     "(B.close_price - A.close_price)/A.close_price as response,",
##     "C.incl_fl_mom_init/100 as reported, D.forecast",
##     "from V1 as A join V1 as B join UK_retail_sales as C",
##     "join UK_retail_sales_incl_fl_mom_forecast as D",
##     "on",
##     "date(A.t2) = C.released_on and",
##     "date(B.t2) = C.released_on and",
##     "D.released_on = C.released_on and",
##     "time(B.t2) = '10:00:00' and",
##     "time(A.t2) = '09:30:00'",
##     "order by C.released_on;"
## );

stmt <- paste(
    "select UK_retail_sales.released_on as D,",
    "(A.t1100_price - A.open_price)/A.open_price as response,",
    "UK_retail_sales.incl_fl_mom_init/100 as reported,",
    "UK_retail_sales_incl_fl_mom_forecast.forecast as forecast",
    "from UK_retail_sales join GBP_swap_5Y_daily_sampled as A",
    "join UK_retail_sales_incl_fl_mom_forecast",
    "on UK_retail_sales.released_on = UK_retail_sales_incl_fl_mom_forecast.released_on",
    "and UK_retail_sales.released_on = A.D"
);

## stmt <- paste(
##     "select UK_retail_sales.released_on as D,",
##     "(A.t1000_price - A.t0930_price)/A.t0930_price as response,",
##     "UK_retail_sales.incl_fl_mom_init/100 as reported,",
##     "UK_retail_sales_incl_fl_mom_forecast.forecast as forecast",
##     "from UK_retail_sales join GBP_USD_daily_sampled as A",
##     "join UK_retail_sales_incl_fl_mom_forecast",
##     "on UK_retail_sales.released_on = UK_retail_sales_incl_fl_mom_forecast.released_on",
##     "and UK_retail_sales.released_on = A.D"
## );

rs <- dbSendQuery(database, stmt);
data <- fetch(rs);
dbClearResult(rs);
dbDisconnect(database);

response <- data$response;
reported <- data$reported;
mean.est <- rep(NA, length(response));

est.dist <- vector("list", length=length(response));

for (i in 1:length(response)) {
    estimated <- as.numeric(unlist(strsplit(data$forecast[i], ",")))/100;
    mean.est[i] <- mean(estimated);
    if (shapiro.test(estimated)$p.value > 0.05) {
        est.dist[[i]] <- list(dist="normal", par=c(mean.est[i], sd(estimated)));
    } else {
        fit <- cobyla(c(1, 1.0e-3, 1, 0, -0.5), fn=function(arg) {
            ret <- sum(dgh(
                ## estimates[i, ],
                tail(estimated, n=-1),
                arg[1], arg[2], arg[3], arg[4], arg[5],
                log=TRUE));
            return(-ret);
        }, hin=function(arg) {
            return(arg[1] - arg[2] - 1.0e-3);
        }, lower=c(1.0e-3, 1.0e-3, 1.0e-3, -Inf, -Inf),
        upper=c(Inf, Inf, Inf, Inf, Inf));
        if (fit$convergence < 0 || fit$convergence == 5) {
            ## Fitting failed.
            est.dist[[i]] <- list(dist="normal", par=c(mean.est[i], sd(estimated)));
        } else {
            est.dist[[i]] <- list(dist="GH", par=fit$par);
        }
    }
}

discrepancy <- reported - mean.est;
r <- cor(response, discrepancy);
T <- r * sqrt(length(response)-2) / sqrt(1 - r^2);
pt(T, df=length(response)-2);

N <- length(response);
M <- N;
sqr.err <- function(params)
{
    errors <- rep(NA, M);
    for (i in 1:M) {
        avg <- mean.est[i];
        par <- est.dist[[i]]$par;
        if (est.dist[[i]]$dist == "normal") {
            Z <- dnorm(avg, par[1], par[2], log=TRUE);
            Z <- Z - dnorm(reported[i], par[1], par[2], log=TRUE);
        } else {
            Z <- dgh(avg, par[1], par[2], par[3], par[4], par[5], log=TRUE);
            Z <- Z - dgh(reported[i], par[1], par[2], par[3], par[4], par[5],
                     log=TRUE);
        }
        Z <- sigmoid(params[1] + params[2] * Z);
        ## errors[i] <- response[i] - params[3];
        ## errors[i] <- errors[i] - params[4] * (reported[i] - avg) * Z;
        errors[i] <- params[3] * (reported[i] - avg) * Z - response[i];
    }
    x <- errors^2 * (1 + sum(params^2));
    ## x <- errors^2;
    return(sum(x));
}

fit1 <- cobyla(c(0, 1, 1), fn=sqr.err,
              lower=c(-3, 0, 0),
              upper=c(3, 2, 2));
fit2 <- direct(fn=sqr.err,
              lower=c(-3, 0, 0),
              upper=c(3, 2, 2));
fit3 <- crs2lm(x0=c(0, 1, 1), fn=sqr.err,
              lower=c(-3, 0, 0),
              upper=c(3, 2, 2));
fit4 <- bobyqa(x0=c(0, 1, 1), fn=sqr.err,
              lower=c(-3, 0, 0),
              upper=c(3, 2, 2));
fit5 <- isres(x0=c(0, 1, 1), fn=sqr.err,
              lower=c(-3, 0, 0),
              upper=c(3, 2, 2));

c(fit1$convergence, fit2$convergence, fit3$convergence, fit4$convergence, fit5$convergence);
rbind(fit1$par, fit2$par, fit3$par, fit4$par, fit5$par);
format(
    rbind(fit1$par, fit2$par, fit3$par, fit4$par, fit5$par),
    digits=3, scientific=TRUE
);

mdl.par <- fit4$par;
## Now we compute the model residuals
res <- rep(NA, length(response));
for (i in 1:length(response)) {
    avg <- mean.est[i];
    par <- est.dist[[i]]$par;
    if (est.dist[[i]]$dist == "normal") {
        Z <- dnorm(avg, par[1], par[2], log=TRUE);
        Z <- Z - dnorm(reported[i], par[1], par[2], log=TRUE);
    } else {
        Z <- dgh(avg, par[1], par[2], par[3], par[4], par[5], log=TRUE);
        Z <- Z - dgh(reported[i], par[1], par[2], par[3], par[4], par[5],
                     log=TRUE);
    }
    Z <- sigmoid(mdl.par[1] + mdl.par[2] * Z);
    ## res[i] <- response[i] - mdl.par[3];
    ## res[i] <- res[i] - mdl.par[4] * (reported[i] - mean.est[i]) * Z;
    res[i] <- response[i] - mdl.par[3] * (reported[i] - mean.est[i]) * Z;
}

discrepancy <- reported - mean.est;
shock.model <- lm(response~discrepancy-1);
## shock.model <- optim(par=c(0, 1), fn=function(arg) {
##     y <- arg[1] + arg[2] * discrepancy - response;
##     return(sum(y^2));
## }, lower=c(-1.0e-3, 1.0e-3), upper=c(1.0e-3, 10), method="L-BFGS-B");
## mdl <- lm(response~discrepancy - 1);

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");

rs <- dbSendQuery(
    database,
    paste(
        "select forecast from UK_retail_sales_incl_fl_mom_forecast",
        "order by released_on desc limit 1"
    )
);
predictions <- fetch(rs);
predictions <- as.numeric(unlist(strsplit(predictions$forecast, split=",")))/100;
dbClearResult(rs);

rs <- dbSendQuery(
    database,
    paste(
        "select incl_fl_mom_rev/100 from UK_retail_sales",
        "order by released_on"
    )
);
actual <- fetch(rs);
dbClearResult(rs);
dbDisconnect(database);
retail.model <- arima(actual, order=c(0,0,1), include.mean=TRUE);
retail.model2 <- arima(actual, order=c(0,0,1), include.mean=FALSE);

my.prediction <- predict(retail.model, n.ahead=1);
R <- residuals(retail.model);

if (shapiro.test(predictions)$p.value > 0.05) {
    mu <- mean(predictions);
    sig <- sd(predictions);
    link.fun <- function(x) {
        Z <- mdl.par[1] + mdl.par[2] * (
            dnorm(mu, mu, sig, log=TRUE) - dnorm(x, mu, sig, log=TRUE)
        );
        Z <- sigmoid(Z);
        return(mdl.par[3] * Z * (x - mu));
    }
} else {
    ## Fit a GH distribution
    mu <- mean(predictions);
    fit <- cobyla(c(1, 1.0e-3, 1, 0, -0.5), fn=function(arg) {
        ret <- sum(dgh(
            predictions,
            arg[1], arg[2], arg[3], arg[4], arg[5],
            log=TRUE));
        return(-ret);
    }, hin=function(arg) {
        return(arg[1] - arg[2] - 1.0e-3);
    }, lower=c(1.0e-3, 1.0e-3, 1.0e-3, -Inf, -Inf),
    upper=c(Inf, Inf, Inf, Inf, Inf));
    if (fit$convergence < 0 || fit$convergence == 5) {
        sig <- sd(predictions);
        link.fun <- function(x) {
            Z <- mdl.par[1] + mdl.par[2] * (
                dnorm(mu, mu, sig, log=TRUE) - dnorm(x, mu, sig, log=TRUE)
            );
            Z <- sigmoid(Z);
            return(mdl.par[3] * Z * (x - mu));
        }
    } else {
        link.fun <- function(x) {
            Z <- mdl.par[1] + mdl.par[2] * (
                dgh(mu, fit$par[1], fit$par[2], fit$par[3], fit$par[4], fit$par[5],
                    log=TRUE) - dgh(x, fit$par[1], fit$par[2], fit$par[3],
                                    fit$par[4], fit$par[5], log=TRUE)
            );
            Z <- sigmoid(Z);
            return(mdl.par[3] * Z * (x - mu));
        }
    }
}

Y <- rnorm(n=2000, my.prediction$pred, my.prediction$se);
Y <- link.fun(Y);
c(mean(Y), sd(Y));

par <- coef(shock.model);
## par[1] + par[2] * (-0.5e-2  - mean(estimates))
c(par * (my.prediction$pred  - mean(predictions)), par * my.prediction$se);

pdf("f42gt5h.pdf");
plot(discrepancy, response, type="p",
     main="",
     xlab="retail sales shock", ylab="% change of GBP 2Y swap rate");
dev.off();
