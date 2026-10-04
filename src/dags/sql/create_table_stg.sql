drop table if exists VT260525088095__STAGING.transactions;


create table IF NOT EXISTS VT260525088095__STAGING.transactions
(
    operation_id VARCHAR(60) NOT NULL ,
    account_number_from int NOT NULL ,
    account_number_to int NOT NULL ,
    currency_code int NOT NULL ,
    country        varchar(7) NOT NULL,
    status varchar(11) NOT NULL ,
    transaction_type  varchar(60) NOT NULL ,
    amount int NOT NULL ,
    transaction_dt     TIMESTAMP(3)    NOT NULL 
)
    order by  operation_id, transaction_dt
    segmented by hash(operation_id, transaction_dt) all nodes ksafe 1
    partition by transaction_dt::date
    group by calendar_hierarchy_day(transaction_dt::date, 3, 2);

drop table if exists VT260525088095__STAGING.currencies;

drop table if exists VT260525088095__STAGING.currencies;

create table IF NOT EXISTS VT260525088095__STAGING.currencies
(
    date_update     timestamp NOT NULL ,
    currency_code int NOT NULL ,
    currency_code_with int NOT NULL ,
    currency_with_div NUMERIC(5, 3) NOT NULL 
)
    order by  date_update
    segmented by hash(date_update) all nodes
    partition by date_update::date
    group by calendar_hierarchy_day(date_update::date, 3, 2);
