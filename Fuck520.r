rm(list=ls());
library(RMySQL);

sigmoid <- function(x, par)
{
    return(1/(par[1] + par[2] * exp(-x)) + par[3]);
}

estimates <- matrix(c(-0.3, 0.45, -0.5, 0.42), byrow=TRUE, ncol=2);
forecast <- c(-0.62, -0.83);
## columns <- c("sprs_inc", "sprs_exc");
columns <- c("sprs_inc");
database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");
impact <- matrix(NA, nrow=length(columns), ncol=2);
for (i in 1:length(columns)) {
    rs <- dbSendQuery(
        database,
        sprintf("select %s as X, resp from uk_rsi_bpsw2_resp where resp is not NULL;", columns[i])
    );
    data <- fetch(rs);
    dbClearResult(rs);
    model <- optim(
        par=c(1,1,1),
        fn=function(P) {
            dev <- data$resp - sigmoid(data$X, P);
            sum(dev^2)
        }
    );
    impact[i, 1] <- sigmoid((forecast[i] - estimates[i, 1])/estimates[i, 2], model$par);
    R <- data$resp - sigmoid(data$X, model$par);
    impact[i, 2] <- sd(R);
}
dbDisconnect(database);


idx <- order(data$X);
Y <- sigmoid(data$X, model$par);

plot(data$X[idx], data$resp[idx],type="b");
points(data$X[idx], Y[idx], col="#0000FF", lwd=2);
curve(
    sigmoid(x, model$par),
    from=min(data$X), to=max(data$X), col="#0000FF",
    add=TRUE
);
abline(h=0, v=0, lty=5, col="#AAAAAA");
