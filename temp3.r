rm(list=ls());
library(fBasics);
library(RMySQL);
library("nloptr");

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");

data.table <- "UK_RSI_automotive_fuel";
## columns <- c("H.mail_order, H.other");
columns <- "H.val";
names <- unlist(strsplit(columns, split=", "));

stmt <- paste(
    "select W.mon,",
    "W.Tmin + 273.15 as Tmin,",
    "W.Tmean + 273.15 as Tmean,",
    "W.Tmax + 273.15 as Tmax,",
    "W.sunshine, W.rainfall, W.raindays1mm,",
    ##    "H.furniture, H.electrical, H.hardware, H.music",
    columns,
    "from UK_weather as W join", data.table,
    "as H on",
    "W.mon = H.mon",
    "where H.mon <= '2018-10-01'",
    "order by W.mon desc limit 120;"
);
rs <- dbSendQuery(database, stmt);
data <- fetch(rs);
dbClearResult(rs);
## dbDisconnect(database);

M <- 6;
D <- apply(as.matrix(data[, -1]), MARGIN=2, FUN=rev);
log.weather <- log(D[, 1:M]);


if (length(names) > 1) {
    log.sales <- log(D[, 7:dim(D)[2]]);
    E.sales <- eigen(cov(log.sales));
    K <- min(which(cumsum(E.sales$values)/sum(E.sales$values) > 0.95));
    sales.factor <- log.sales %*% E.sales$vectors[, 1:K];
} else {
    K <- 1;
    log.sales <- matrix(log(D[, 7:dim(D)[2]]), nrow=dim(D)[1], ncol=1);
    sales.factor <- log.sales;
}


## Y <- sales.factor;
regrs.coef <- matrix(NA, nrow=M, ncol=K);
arma.coef <- matrix(NA, nrow=K, ncol=4);
predictions <- matrix(NA, nrow=K, ncol=2);

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
        stop(sprintf("bobyqa failed for the %d-th factor: %s", fit$message));
    }
    P <- fit$par;
    regrs.coef[, i] <- P * (abs(P) * A[i] / B[i] > 0.05);
    R <- sales.factor[, i] - log.weather %*% regrs.coef[, i];
    curaic <- Inf;
    for (p in 0:1) {
        for (q in 0:1) {
            for (sp in 0:1) {
                for (sq in 0:1) {
                    mdl <- arima(R, order=c(p, 1, q),
                                 seasonal=list(period=12, order=c(sp, 1, sq)));
                    if (AIC(mdl) < curaic) {
                        arma.coef[i, 1] <- if (p > 0) mdl$coef[p] else 0;
                        arma.coef[i, 2] <- if (q > 0) mdl$coef[p+q] else 0;
                        arma.coef[i, 3] <- if (sp > 0) mdl$coef[p+q+sp] else 0;
                        arma.coef[i, 4] <- if (sq > 0) mdl$coef[p+q+sp+sq] else 0;
                        curaic <- AIC(mdl);
                        pred <- predict(mdl, n.ahead=1);
                        predictions[i, 1] <- pred$pred;
                        predictions[i, 2] <- pred$se;
                    }
                }
            }
        }
    }
}

stmt <- paste(
    "select Tmin+273.15, Tmean+273.15, Tmax+273.15,",
    "sunshine, rainfall, raindays1mm",
    "from UK_weather where mon='2018-11-01';"
);
rs <- dbSendQuery(database, stmt);
X <- as.matrix(fetch(rs));
dbClearResult(rs);
factor.pred <- log(X) %*% regrs.coef + predictions[, 1];

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

stmt <- sprintf("update %s set", data.table);

for (i in 1:length(names)) {
    name <- substr(names[i], start=3, stop=nchar(names[i]));
    if (i < length(names)) {
        temp <- sprintf(" %s_f = %f,", name, indices[i]);
    } else {
        temp <- sprintf(" %s_f = %f", name, indices[i]);
    }
    stmt <- paste(stmt, temp);
}
stmt <- paste(stmt, "where mon= '2018-11-01';");
rs <- dbSendQuery(database, stmt);
dbClearResult(rs);

dbDisconnect(database);
