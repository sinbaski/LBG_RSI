rm(list=ls());
library(fBasics);
library(RMySQL);
library("nloptr");

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");
stmt <- paste(
##  "select W.mon, W.Tmean, W.sunshine, W.rainfall, W.raindays1mm,",
    "select W.mon,",
    "W.Tmin + 273.15 as Tmin,",
    "W.Tmean + 273.15 as Tmean,",
    "W.Tmax + 273.15 as Tmax,",
    "W.sunshine, W.rainfall, W.raindays1mm,",
##    "select W.*,"
    "H.furniture, H.electrical, H.hardware, H.music from",
    "UK_weather as W join uk_rsi_household_goods as H on",
    "W.mon = H.mon",
    "order by W.mon desc limit 120;"
);
rs <- dbSendQuery(database, stmt);
data <- fetch(rs);
dbClearResult(rs);
## dbDisconnect(database);

D <- apply(as.matrix(data[, -1]), MARGIN=2, FUN=rev);
log.weather <- log(D[, 1:6]);
M <- 6;
## E.weather <- eigen(cov(log.weather));
## weather.factor <- log.weather %*% E.weather$vectors;

## acf(weather.factors[, 6], lag.max=60);
## grid();


## Keep the first 3 factors;
## weather.factors <- [, 1:3];
log.sales <- log(D[, 7:10]);
## mdl <- lm(log.sales~log.weather);

## acf((D[, 7]), lag.max=72);
E.sales <- eigen(cov(log.sales));
K <- 3;
sales.factor <- log.sales %*% E.sales$vectors[, 1:K];
acf(sales.factor[, 1], lag.max=72);

## Y <- sales.factor;
regrs.coef <- matrix(NA, nrow=M, ncol=K);
arma.coef <- matrix(NA, nrow=K, ncol=4);
A <- apply(abs(log.weather), MARGIN=2, FUN=mean);
B <- apply(abs(sales.factor), MARGIN=2, FUN=mean);
for (i in 1:K) {
    fit <- bobyqa(
        x0=rep(1, M),
        fn=function(arg) {
            temp <- sum((sales.factor[, i] - log.weather[, 1:M] %*% arg)^2);
            temp <- temp * (1 + sum(abs(arg)));
            return(temp);
        },
        lower=-10*rep(1, M),
        upper=10*rep(1, M)
    );
    if (fit$convergence < 0) {
        stop(sprintf("bobyqa failed for the %d-th factor: %s", fit$message));
    } else if (fit$convergence == 5) {
        warning(sprintf("bobyqa failed for the %d-th factor: %s", fit$message));
    } else {
        P <- fit$par;
        regrs.coef[, i] <- P * (abs(P) * A[i] / B[i] > 0.05);
    }
}

R <- sales.factor - log.weather %*% regrs.coef;
par(mfrow=c(1, 2));
acf(diff(diff(R[, 2]), lag=12), lag.max=72);
grid();
pacf(diff(diff(R[, 2]), lag=12), lag.max=72);
grid();
## which(abs(C) > 0.05, arr.ind=TRUE);
## matrix(as.vector(C[abs(C) > 0.05]), nrow=M, ncol=K);

## mdl <- lm(Y~log.weather);
## C <- coef(mdl);

## W <- log.weather[, 1:3];
## mdl2 <- lm(Y~W);
## C2 <- coef(mdl2);

## R <- residuals(mdl2);

predictions <- matrix(NA, nrow=dim(sales.factor)[2], ncol=2);

## Model of the first factor
R.model1 <- arima(
    R[, 1], order=c(1,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
R.model2 <- arima(
    R[, 1], order=c(0,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
R.model3 <- arima(
    R[, 1], order=c(0,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
R.model4 <- arima(
    R[, 1], order=c(1,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
c(AIC(R.model1), AIC(R.model2), AIC(R.model3), AIC(R.model4))
c(BIC(R.model1), BIC(R.model2), BIC(R.model3), BIC(R.model4))

acf(residuals(R.model3), lag.max=60);

R.model <- R.model3;
pred <- predict(R.model, n.ahead=1);
predictions[1, ] <- c(pred$pred, pred$se);

## Model of the second factor
R.model1 <- arima(
    R[, 2], order=c(1,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
R.model2 <- arima(
    R[, 2], order=c(0,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
R.model3 <- arima(
    R[, 2], order=c(0,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
R.model4 <- arima(
    R[, 2], order=c(1,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
c(AIC(R.model1), AIC(R.model2), AIC(R.model3), AIC(R.model4))
c(BIC(R.model1), BIC(R.model2), BIC(R.model3), BIC(R.model4))

acf(residuals(R.model3), lag.max=60);

R.model <- R.model3;
pred <- predict(R.model, n.ahead=1);
predictions[2, ] <- c(pred$pred, pred$se);

## Model of the third factor
R.model1 <- arima(
    R[, 3], order=c(1,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
R.model2 <- arima(
    R[, 3], order=c(0,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
R.model3 <- arima(
    R[, 3], order=c(0,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
R.model4 <- arima(
    R[, 3], order=c(1,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
c(AIC(R.model1), AIC(R.model2), AIC(R.model3), AIC(R.model4))
c(BIC(R.model1), BIC(R.model2), BIC(R.model3), BIC(R.model4))

acf(residuals(R.model4), lag.max=60);


R.model <- R.model4;
pred <- predict(R.model, n.ahead=1);
predictions[3, ] <- c(pred$pred, pred$se);

## Model of the third factor
## R.model1 <- arima(
##     R[, 3], order=c(1,1,1),
##     seasonal=list(order=c(1,1,1), period=12)
## );
## R.model2 <- arima(
##     R[, 3], order=c(0,1,1),
##     seasonal=list(order=c(1,1,1), period=12)
## );
## R.model3 <- arima(
##     R[, 3], order=c(0,1,1),
##     seasonal=list(order=c(0,1,1), period=12)
## );
## R.model4 <- arima(
##     R[, 3], order=c(1,1,1),
##     seasonal=list(order=c(0,1,1), period=12)
## );
## c(AIC(R.model1), AIC(R.model2), AIC(R.model3), AIC(R.model4))
## c(BIC(R.model1), BIC(R.model2), BIC(R.model3), BIC(R.model4))

## acf(residuals(R.model3), lag.max=60);

## R.model <- R.model4;
## pred <- predict(R.model, n.ahead=1);
## predictions[3, ] <- c(pred$pred, pred$se);

F <- log.sales %*% E.sales$vectors[, (K+1):dim(log.sales)[2]];

stmt <- paste(
    "select Tmin+273.15, Tmean+273.15, Tmax+273.15,",
    "sunshine, rainfall, raindays1mm",
    "from UK_weather where mon='2018-11-01';"
);
rs <- dbSendQuery(database, stmt);
X <- as.matrix(fetch(rs));
dbClearResult(rs);

factor.pred <- log(X) %*% regrs.coef + predictions[, 1];

## sales.pred <- factor.pred %*% t(E.sales$vectors[, 1:K]);
sales.pred <- c(factor.pred, mean(F)) %*% t(E.sales$vectors);
indices <- exp(sales.pred);
worth <- c(13671, 6287, 11713, 1002);

stmt <- paste(
    "update UK_RSI_household_goods",
    "set furniture_f = %f, electrical_f = %f, hardware_f = %f, music_f = %f",
    "where mon= '2018-11-01';"
);
stmt <- sprintf(stmt, indices[1], indices[2], indices[3], indices[4]);
rs <- dbSendQuery(database, stmt);
dbClearResult(rs);

dbDisconnect(database);


## mdl2 <- lm(Y~weather.factor);
## C2 <- coef(mdl2);


