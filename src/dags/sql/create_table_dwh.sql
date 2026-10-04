    

drop table if exists VT260525088095__DWH.global_metrics;


create table IF NOT EXISTS VT260525088095__DWH.global_metrics
(
	date_update DATE NOT NULL ,
	currency_from int NOT NULL ,
	amount_total numeric(18,2) NOT NULL ,
	cnt_transactions int NOT NULL ,
	avg_transactions_per_account numeric(18,2) NOT NULL ,
	cnt_accounts_make_transactions int NOT NULL 
)
    order by date_update
    segmented by hash(date_update, currency_from) all nodes ksafe 1
    partition by date_update
    group by calendar_hierarchy_day(date_update::date, 3, 2);

