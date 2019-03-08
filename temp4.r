rm(list=ls());
library(RMySQL);
library("nloptr");

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");
stmt <- paste(
    "select mon from uk_rsi_food",
    "where non_specialized_food_f is not null",
    "and mon <= '2018-11-01'",
    "order by mon desc"
);
rs <- dbSendQuery(database, stmt);
dates <- fetch(rs)$mon;
dbClearResult(rs);

overall <- rep(NA, length(dates));

weights <- {};
for (i in 1:length(dates)) {
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
        sprintf("where UK_RSI_food.mon = '%s';", dates[i])
    );
    rs <- dbSendQuery(database, stmt);
    forecast <- as.vector(fetch(rs));
    dbClearResult(rs);

    if (length(weights) == 0) {
        weights <- rep(NA, length(forecast$names));
        columns <- attributes(forecast)$names;
        for (j in 1:length(columns)) {
            columns[j] <- substr(columns[j], start=1, stop=nchar(columns[j])-2);
            stmt <- paste(
                "select weight",
                "from uk_rsi_sector_weights",
                sprintf("where category='%s'", columns[j])
            );
            rs <- dbSendQuery(database, stmt);
            weights[j] <- fetch(rs)$weight;
        }
    }
    overall[i] <- sum(forecast * weights) / sum(weights);
}

rs <- dbSendQuery(
    database,
    paste(
        "select * from uk_rsi_overall_unadjusted",
        "where mon <= '2018-11-01'",
        sprintf("order by mon desc limit %d", length(overall))
    )
);
data <- fetch(rs);
dbClearResult(rs);
dbDisconnect(database);

series <- cbind(data$overall, overall);
series <- apply(series, MARGIN=2, FUN=rev);

pdf("RetailSalesIndex.pdf", width=12, height=6);
plot(1:dim(series)[1], series[, 1],
     ylim=c(min(series), max(series)),
     main="UK retail sales index - non-seasonally adjusted: forecast accuracy",
     type="b", col="#FF0000", xaxt='n', pch=16,
     xlab="", ylab="");
lines(1:dim(series)[1], series[, 2], type="b", col="#0000FF", pch=16);
labels <- seq(from=1, length(overall), by=3);
axis(side=1, at=labels, labels=substr(rev(dates)[labels], start=1, stop=7),
     las=2);
abline(v=labels, h=seq(80, 130, 5), lty="dotted", col="#aaaaaa");
legend("topleft", legend=c("reported", "forecast"),
       col=c("#FF0000", "#0000FF"), lwd=c(1,1));
dev.off();


## X <- c(head(series, n=-1)[, 1], tail(series, n=1)[2]);
## X <- rev(data$overall);
X <- rev(c(overall[1], data$overall));
## X <- rev(log(data$overall));
## model1 <- arima(X, order=c(0,1,1), seasonal=list(order=c(0,1,1), period=12));
## model2 <- arima(X, order=c(1,1,0), seasonal=list(order=c(0,1,1), period=12));
## model3 <- arima(X, order=c(1,1,1), seasonal=list(order=c(0,1,1), period=12));
## c(AIC(model1), AIC(model2), AIC(model3));
## R <- residuals(model1);
## model <- model1;
## model$coef <- model1$coef[1];
## model
## pred <- predict(model, n.ahead=1);
## exp(pred$pred);


X.ts <- ts(tail(X, 5*12), frequency=12, end=c(2018, 12));
Y <- decompose(X.ts, type="multiplicative");
Y.adjusted <- Y$x/Y$seasonal;
## Y <- decompose(X.ts, type="additive");
## Y.adjusted <- Y$x - Y$seasonal;
## tail(Y.adjusted, n=6);
diff(log(tail(Y.adjusted, n=7)))

rs <- dbSendQuery(
    database,
    sprintf(
        "select * from uk_rsi_adjusted_overall order by mon desc limit %d",
        length(X.ts) - 1
    )
);
adjusted <- fetch(rs);
dbClearResult(rs);

errors <- head(Y.adjusted, n=-1) - rev(adjusted$adjusted);
## errors <- Y.adjusted - rev(adjusted$adjusted);
N <- length(adjusted$adjusted);
plot(1:N, rev(adjusted$adjusted), type="b", col="#FF0000", pch=16);
lines(1:N, head(Y.adjusted, n=-1), type="b", col="#0000FF", pch=16);
## lines(1:N, Y.adjusted, type="b", col="#0000FF", pch=16);
c(mean(errors), sd(errors))

## Z <- decompose(
##     ts(tail(series[, 1], 36), end=c(2018, 11), frequency=12),
##     type="multiplicative"
## );
## Z.adjusted <- Z$x / Z$seasonal;

## U <- cbind(Y.adjusted, Z.adjusted)

## series <- data$overall;
## series[length(series)] <- overall[1, 1];
## X <- ts(
##     series,
##     ## tail(data$overall, n=72),
##     ## tail(data$overall, n=-11),
##     ## start=c(1998, 11), end=c(2018, 11),
##     frequency=12
## );
## Y <- decompose(X, type="multiplicative");
## adjusted <- Y$x/Y$seasonal;
## adjusted;
dbDisconnect(database);

A <- 2:10;
G <- rep(NA, length(A));
H <- rep(NA, length(A));
for (i in A) {
    X <- ts(
        ## tail(series, n=i * 12),
        tail(rev(data$overall), n=i*12),
        ## tail(data$overall, n=-11),
        end=c(2018, 11),
        frequency=12
    );
    Y <- decompose(X, type="multiplicative");
    H[i-1] <- tail(Y$x/Y$seasonal, n=1);

    Z <- decompose(log(X), type="additive");
    G[i-1] <- exp(tail(Z$x - Z$seasonal, n=1));
}
cbind(G, H)
par(mfrow=c(1,2))
acf(data$overall, lag.max=72)
pacf(data$overall, lag.max=72)

model1 <- arima(
    rev(data$overall),
    order=c(0,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
model2 <- arima(
    rev(data$overall),
    order=c(1,1,1),
    seasonal=list(order=c(0,1,1), period=12)
);
model3 <- arima(
    rev(data$overall),
    order=c(0,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
model4 <- arima(
    rev(data$overall),
    order=c(1,1,1),
    seasonal=list(order=c(1,1,1), period=12)
);
c(AIC(model1), AIC(model2), AIC(model3), AIC(model4))
## c(AIC(model1), AIC(model2), AIC(model3))

R <- residuals(model1);

bet <- coef(model1);
Y <- rep(NA, length(data$overall));
for (i in 13:length(Y)) {
    Y[i] <- bet[1] * R
}
