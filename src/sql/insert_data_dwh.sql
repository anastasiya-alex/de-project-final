 with prep_c as 
            (
            select 
                * 
            from VT260525088095__STAGING.currencies c  
            where 
                c.currency_code_with = 420
            ),
            prep as
            (
            select 
                t.transaction_dt::date as transaction_d,
                t.currency_code as currency_from,
                case when currency_with_div is null then 1 else currency_with_div end as   currency_with_div,
                sum(amount)/100::float as amount,
                count(distinct operation_id) as operations,
                count(distinct account_number_from) as accounts
            from VT260525088095__STAGING.transactions as t
                left join prep_c 
                as c on 
                    c.currency_code = t.currency_code 
                    and t.transaction_dt::date=c.date_update 
            where 1=1
                --and t.operation_id  in ('000701b0-5fcf-4373-afb6-b43ee4acb56d', '0000d925-b0c4-4007-aac6-cc81ef1900c7','00087ece-ecc3-41a6-9120-5d06bfa730f0','9ccf7b7b-7a0d-4321-bd41-3001fce98d55','8ef6e321-6768-4832-903a-b4cb6759514c', '7f9922e8-48b8-4097-9ff6-3eab131e80d9')
                and status = 'done'
                --and transaction_dt::date = '2022-10-01'
                --and t.transaction_dt::date = '{load_date}'
                and t.account_number_from > 0
                and t.account_number_to > 0
                and t.operation_id not in (select operation_id from VT260525088095__STAGING.transactions where status = 'chargeback') --убираю возвраты
            group by 1,2,3
            )

            select 
                transaction_d,
                currency_from,
                round(amount*currency_with_div,2) as amount_total,
                operations as cnt_transactions,
                round(operations/accounts::float,2) as avg_transactions_per_account,
                accounts as cnt_accounts_make_transactions
            from prep
            order by transaction_d, currency_from;