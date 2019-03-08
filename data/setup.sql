drop table UK_retail_sales;

create table UK_retail_sales (
       released_on date primary key,
       incl_fl_mom_init float,
       incl_fl_mom_rev float,
       incl_fl_yoy_init float,
       incl_fl_yoy_rev float,
       excl_fl_mom_init float,
       excl_fl_mom_rev float,
       excl_fl_yoy_init float,
       excl_fl_yoy_rev float
);

create table GBP_USD_daily_sampled (
       D date not null primary key,
       open_price float,
       t0900_price float,
       t0930_price float,
       t1000_price float,
       close_price float
);

create table GBP_swap_2Y_daily_sampled (
       D date not null primary key,
       open_price float,
       t1100_price float,
       close_price float
);

create table GBP_swap_5Y_daily_sampled (
       D date not null primary key,
       open_price float,
       t1100_price float,
       close_price float
);

create table GBP_swap_sa_vs_6M_2Y_30min (
       t2 datetime not null primary key,
       open_price float,
       high_price float,
       low_price float,
       close_price float
);

create table GBP_swap_sa_vs_6M_2Y_30min (
       t2 datetime not null primary key,
       open_price float,
       high_price float,
       low_price float,
       close_price float
);

create table GBP_swap_sa_vs_6M_5Y_30min (
       t2 datetime not null primary key,
       open_price float,
       high_price float,
       low_price float,
       close_price float
);

create table FTSE100_30min (
       t2 datetime not null primary key,
       open_price float,
       high_price float,
       low_price float,
       close_price float
);

create table UK_retail_sales_incl_fl_mom_forecast (
       released_on date primary key,
       forecast varchar(512)
);

delete from Uk_Retail_Sales_Incl_Fl_Mom_Forecast;

load data infile "../Uploads/retail_sales.txt" into table UK_retail_sales columns terminated by ',';

load data local infile "FTSE100_30min.txt" into table FTSE100_30min columns terminated by '\t';

load data local infile "A.txt" into table GBP_swap_5Y_daily_sampled columns terminated by '\t';

load data local infile 'GBPUSD_daily_sampled.txt' into table GBP_USD_daily_sampled columns terminated by '\t';

load data infile "../Uploads/GBP_swap_SA_vs_6M_2Y.txt" into table GBP_swap_sa_vs_6M_2Y_30min columns terminated by ',';

load data local infile "GBP_swap_SA_vs_6M_5Y.txt" into table GBP_swap_sa_vs_6M_5Y_30min columns terminated by '\t';

delete from UK_retail_sales_incl_fl_mom_forecast;
load data local infile 'UK_Retail_Sales.csv' into table UK_retail_sales_incl_fl_mom_forecast columns terminated by '\t';

drop view V1;

-- create view V1 as
-- select * from FTSE100_30min where time(t2) = '09:30:00'
-- or time(t2) = '10:00:00' order by t2;

create view V1 as
select * from GBP_swap_sa_vs_6M_5Y_30min where time(t2) = '09:30:00'
or time(t2) = '10:00:00' order by t2;

select
C.released_on, 
(B.close_price - A.close_price)/A.close_price as ret,
C.incl_fl_mom_init, D.forecast
from V1 as A join V1 as B join UK_retail_sales as C
join UK_retail_sales_incl_fl_mom_forecast as D
on
date(A.t2) = C.released_on and
date(B.t2) = C.released_on and
D.released_on = C.released_on and
time(B.t2) = '10:00:00' and
time(A.t2) = '09:30:00';


select A.released_on, (C.close_price - B.close_price)/B.close_price as ret
from
UK_retail_sales as A join FTSE100_30min as B join FTSE100_30min as C
on
A.released_on = date(B.t2)

select
A.released_on as D,
A.incl_fl_mom_init as init,
A.incl_fl_mom_rev as rev,
B.forecast as forecast
from UK_retail_sales as A
join UK_retail_sales_incl_fl_mom_forecast as B
on A.released_on = B.released_on
order by A.released_on;


desc UK_Tmin;

select A.V, B.V from UK_Tmax as A join UK_Tmin as B
on
A.Y = B.Y
and
A.M = B.M;



alter view UK_weather as
select
A.D as mon, A.val as Tmax, B.val as Tmin, C.val as Tmean,
D.val as sunshine, E.val as Rainfall,
F.val as raindays1mm
from UK_Tmax as A
join UK_Tmin as B
join UK_Tmean as C
join UK_Sunshine as D
join UK_Rainfall as E
join UK_Raindays1mm as F
on A.D = B.D
and A.D = C.D
and A.D = D.D
and A.D = E.D
and A.D = F.D

-- insert into GBP_swap_sa_vs_6M_2Y_30min values
-- ('2017-12-20 07:30:00', .78675, .78675, .78675, .78675);

-- alter table UK_Tmax change val V float;

-- create table UK_RSI_sector_weights (
--        non_specialized float,
--        specialist float,
--        drinks_tobacco float,
--        textiles float,
--        clothing float,
--        footwear float,
--        furniture float,
--        electrical float,
--        hardware float,
--        music float,
--        pharmaceutical_etc float,
--        books_etc float,
--        floor_cover float,
--        computer_telcomm	float,
--        other_specialized float,
--        mail_order float,
--        other_non_store float,
--        automotive_fuel float
-- );

-- insert into uk_rsi_sector_weights values (
--        142507, 8346, 3593, 800, 40106, 4823, 13671, 6287, 11713,
--        1002, 5603, 3723, 1520, 5675, 34098, 30738, 2464, 36849
-- );
drop table uk_rsi_sector_weights;

create table uk_rsi_sector_weights (
       category varchar(32) primary key,
       grp varchar(32),
       weight float
);

insert into uk_rsi_sector_weights values
('non_specialized_food', 'food',142507),
('specialist_food', 'food',8346),
('drinks_tobacco', 'food',3593),
('non_specialized_non_food', 'non_specialized_non_food',34180),
('textiles', 'textiles_etc', 800),
('clothing', 'textiles_etc', 40106),
('footwear', 'textiles_etc', 4823),
('furniture', 'household_goods', 13671),
('electrical', 'household_goods', 6287),
('hardware', 'household_goods', 11713),
('music', 'household_goods', 1002),
('pharmaceutical_etc', 'pharmaceutical_etc', 5603),
('books_etc', 'books_etc', 3723),
('floor_cover', 'floor_cover', 1520),
('computer_telcomm', 'computer_telcomm', 5675),
('other_specialized', 'other_specialized', 34098),
('mail_order', 'non_store', 30738),
('other_non_store', 'non_store', 2464),
('fuel', 'fuel', 36849);

create table UK_RSI_food (
       mon date primary key,
       non_specialized_food float,
       specialist float,
       drinks_tobacco float,
       non_specialized_f float,
       specialist_f float,
       drinks_tobacco_f float
);

create table UK_RSI_non_specialized_non_food (
       mon date primary key,
       val float,
       val_f float
);

create table UK_RSI_textiles_etc (
       mon date primary key,
       textiles float,
       clothing float,
       footwear float,
       textiles_f float,
       clothing_f float,
       footwear_f float
);

create table UK_RSI_household_goods (
       mon date primary key,
       furniture float,
       electrical float,
       hardware float,
       music float,
       furniture_f float,
       electrical_f float,
       hardware_f float,
       music_f float
);

create table UK_RSI_pharmaceutical_etc (
       mon date primary key,
       val float,
       val_f float
);

create table UK_RSI_books_etc (
       mon date primary key,
       val float,
       val_f float
);

create table UK_RSI_floor_cover (
       mon date primary key,
       val float,
       val_f float
);

create table UK_RSI_computer_telcomm (
       mon date primary key,
       val float,
       val_f float
);

create table UK_RSI_other_specialized (
       mon date primary key,
       val float,
       val_f float
);

create table UK_RSI_non_store (
       mon date primary key,
       mail_order float,
       other float,
       mail_order_f float,
       other_f float       
);

create table UK_RSI_automotive_fuel (
       mon date primary key,
       val float,
       val_f float
);

load data local infile 'B.txt' into table UK_RSI_food columns terminated by '\t' (non_specialized_s, specialist_s, drinks_tobacco_s);

select
non_specialized_f,
specialist_f,
drinks_tobacco_f,
textiles_f,
clothing_f,
footwear_f,
furniture_f,
electrical_f,
hardware_f,
music_f,
UK_RSI_pharmaceutical_etc.val_f,
UK_RSI_books_etc.val_f,
UK_RSI_floor_cover.val_f,
UK_RSI_computer_telcomm.val_f,
UK_RSI_other_specialized.val_f,
mail_order_f,
other_f,
UK_RSI_automotive_fuel.val_f
from UK_RSI_food
join UK_RSI_textiles_etc
join UK_RSI_household_goods
join UK_RSI_pharmaceutical_etc
join UK_RSI_books_etc
join UK_RSI_floor_cover
join UK_RSI_computer_telcomm
join UK_RSI_other_specialized
join UK_RSI_non_store
join UK_RSI_automotive_fuel
on
UK_RSI_food.mon = UK_RSI_textiles_etc.mon
and UK_RSI_food.mon = UK_RSI_household_goods.mon
and UK_RSI_food.mon = UK_RSI_pharmaceutical_etc.mon
and UK_RSI_food.mon = UK_RSI_books_etc.mon
and UK_RSI_food.mon = UK_RSI_floor_cover.mon
and UK_RSI_food.mon = UK_RSI_computer_telcomm.mon
and UK_RSI_food.mon = UK_RSI_other_specialized.mon
and UK_RSI_food.mon = UK_RSI_non_store.mon
and UK_RSI_food.mon = UK_RSI_automotive_fuel.mon
where UK_RSI_food.mon = '2018-11-01';

create table my_lm_tm_models (
       series varchar(64) primary key,
       UK_Tmin float,
       UK_Tmean float,
       UK_Tmax float,
       UK_sunshine float,
       UK_Rainfall float,
       UK_Raindays1mm float,
       UK_Airfrost float,
       d tinyint,
       ar1 float,
       ma1 float,
       sd tinyint,
       sar1 float,
       sma1 float
);

drop table uk_rsi_adjusted;
create table uk_rsi_adjusted (
       mon date primary key,
       non_specialized_food float,
       specialist_food float,
       drink_tobacco float,
       non_specialized_non_food	float,
       textiles float,
       clothing float,
       footwear_leather float,
       furnature_lighting float,
       electrical float,
       hardware float,
       music float,
       chemists float,
       medical float,
       cosmetic_toilet float,
       computer_telcomm float,
       floor_cover float,
       books_periodicals float,
       sports_games float,
       plants_pet_food float,
       watches_jewellery float,
       other_specialized float,
       second_hand float,
       mail_order float,
       other_non_store float,
       fuel float
);

alter table uk_rsi_adjusted
add non_specialized_food_f float,
add specialist_food_f float,
add drink_tobacco_f float,
add non_specialized_non_food_f float,
add textiles_f float,
add clothing_f float,
add footwear_leather_f float,
add furnature_lighting_f float,
add electrical_f float,
add hardware_f float,
add music_f float,
add chemists_f float,
add medical_f float,
add cosmetic_toilet_f float,
add computer_telcomm_f float,
add floor_cover_f float,
add books_periodicals_f float,
add sports_games_f float,
add plants_pet_food_f float,
add watches_jewellery_f float,
add other_specialized_f float,
add second_hand_f float,
add mail_order_f float,
add other_non_store_f float,
add fuel_f float
;

create table uk_rsi_adjusted_weights (
       category varchar(64) primary key,
       subclass varchar(64),
       weight float
);

insert into uk_rsi_adjusted_weights values
('non_specialized_food', 'food', 38.5),
('specialist_food', 'food', 2.2),
('drink_tobacco', 'food', 0.8),
('non_specialized_non_food', 'non-specialized non-food', 8.1),
('textiles', 'textiles etc', 0.19),
('clothing', 'textiles etc', 10.69),
('footwear_leather', 'textiles etc', 1.29),
('furnature_lighting', 'household', 3.11),
('electrical', 'household', 2.08),
('hardware', 'household', 2.92),
('music', 'household', 0.43),
('chemists', 'other specialized non-food', 0.31),
('medical', 'other specialized non-food', 0.14),
('cosmetic_toilet', 'other specialized non-food', 0.84),
('computer_telcomm', 'other specialized non-food', 1.23),
('floor_cover', 'other specialized non-food', 0.59),
('books_periodicals', 'other specialized non-food', 1.22),
('sports_games', 'other specialized non-food', 2.18),
('plants_pet_food', 'other specialized non-food', 1.15),
('watches_jewellery', 'other specialized non-food', 1.42),
('other_specialized', 'other specialized non-food', 2.81),
('second_hand', 'other specialized non-food', 0.64),
('mail_order', 'non-store', 4.9),
('other_non_store', 'non-store', 0.8),
('fuel', 'fuel', 11.5);

load data local infile 'B.txt' into table uk_rsi_adjusted
columns terminated by '\t';

load data local infile 'C.txt' into table UK_RSI_non_specialized_non_food
columns terminated by '\t' (mon, val);

select * from UK_RSI_overall_unadjusted order by mon desc limit 20;

create table uk_rsi_adjusted_overall (
       mon char(10) primary key,
       adjusted float
);

load data local infile 'C.txt' into table uk_rsi_adjusted_overall
columns terminated by '\t';

delimiter #
create function may_bank_hld (y char(4))
returns date
begin
  set @x = concat(y, '-05-31');
  return date_add(@x, interval -weekday(@x) day);
end#
delimiter ;

delimiter #
create function august_bank_hld (y char(4))
returns date
begin
  set @x = concat(y, '-08-31');
  return date_add(@x, interval -weekday(@x) day);
end#
delimiter ;

delimiter #
create function easter_sunday(y char(4))
returns date
begin
set @a = y mod 19;
set @b = y div 100;
set @c = y mod 100;
set @d = @b div 4;
set @e = @b mod 4;
set @f = (@b + 8) div 25;
set @g = (@b - @f + 1) div 3;
set @h = (19*@a + @b - @d - @g + 15) mod 30;
set @i = @c div 4;
set @k = @c mod 4;
set @l = (32 + 2*@e + 2*@i - @h - @k) mod 7;
set @m = (@a + 11*@h + 22*@l) div 451;
set @mth = (@h + @l - 7*@m + 114) div 31;
set @dag = ((@h + @l - 7*@m + 114) mod 31) + 1;
return concat(y, '-', @mth, '-', @dag);
end#
delimiter ;



create view bank_holiday_effects as
select T1.mon, T1.d1, date_add(T2.d1, interval -1 day) as d2,
may_bank_hld(year(T1.mon)) as d3,
august_bank_hld(year(T1.mon)) as d4,
case
when month(T1.mon) = 5 && may_bank_hld(year(T1.mon)) >= T1.d1 && may_bank_hld(year(T1.mon)) < T2.d1
then 1
when month(T1.mon) = 6 && may_bank_hld(year(T1.mon)) < T1.d1
then -0.8
when month(T1.mon) = 8 && august_bank_hld(year(T1.mon)) >= T1.d1 && august_bank_hld(year(T1.mon)) < T2.d1
then 1
when month(T1.mon) = 9 && august_bank_hld(year(T1.mon)) < T1.d1
then -0.8
else 0
end as bh
from uk_rsi_standard_reporting_period as T1
join uk_rsi_standard_reporting_period as T2
on T2.mon = date_add(T1.mon, interval 1 month);
