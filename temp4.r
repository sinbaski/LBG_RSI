rm(list=ls());
library(RMySQL);
library("nloptr");

database = dbConnect(MySQL(), user='sinbaski', password='q1w2e3r4',
                     dbname='LBG', host="localhost");
stmt <- paste(
    "select",
    "non_specialized_f,",
    "specialist_f,",
    "drinks_tobacco_f,",
    "textiles_f,",
    "clothing_f,",
    "footwear_f,",
    "furniture_f,",
    "electrical_f,",
    "hardware_f,",
    "music_f,",
    "UK_RSI_pharmaceutical_etc.val_f,",
    "UK_RSI_books_etc.val_f,",
    "UK_RSI_floor_cover.val_f,",
    "UK_RSI_computer_telcomm.val_f,",
    "UK_RSI_other_specialized.val_f,",
    "mail_order_f,",
    "other_f,",
    "UK_RSI_automotive_fuel.val_f",
    "from UK_RSI_food",
    "join UK_RSI_textiles_etc",
    "join UK_RSI_household_goods",
    "join UK_RSI_pharmaceutical_etc",
    "join UK_RSI_books_etc",
    "join UK_RSI_floor_cover",
    "join UK_RSI_computer_telcomm",
    "join UK_RSI_other_specialized",
    "join UK_RSI_non_store",
    "join UK_RSI_automotive_fuel",
    "on",
    "UK_RSI_food.mon = UK_RSI_textiles_etc.mon",
    "and UK_RSI_food.mon = UK_RSI_household_goods.mon",
    "and UK_RSI_food.mon = UK_RSI_pharmaceutical_etc.mon",
    "and UK_RSI_food.mon = UK_RSI_books_etc.mon",
    "and UK_RSI_food.mon = UK_RSI_floor_cover.mon",
    "and UK_RSI_food.mon = UK_RSI_computer_telcomm.mon",
    "and UK_RSI_food.mon = UK_RSI_other_specialized.mon",
    "and UK_RSI_food.mon = UK_RSI_non_store.mon",
    "and UK_RSI_food.mon = UK_RSI_automotive_fuel.mon",
    "where UK_RSI_food.mon = '2018-11-01';"
);
rs <- dbSendQuery(database, stmt);
forecast <- as.vector(fetch(rs));
dbClearResult(rs);

stmt <- "select * from UK_RSI_sector_weights;";
rs <- dbSendQuery(database, stmt);
weights <- as.vector(fetch(rs));
dbClearResult(rs);

dbDisconnect(database);

overall <- sum(forecast * weights) / sum(weights);
