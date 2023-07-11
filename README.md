# ashtonrob1Таня, [28.03.2023 21:50]
WITH 

accounts as (
        SELECT 
            DISTINCT("to") as accts
        FROM arbitrum.traces
        WHERE success = true 
),

transactions_count as (
        SELECT
            ac.accts, 
            COUNT(at.hash) as num_transactions
        FROM 
        accounts ac 
        INNER JOIN 
        arbitrum.transactions at 
            ON ac.accts = at."from"
        GROUP BY 1 
        ORDER BY 2 DESC 
), 

count_profile as (
        SELECT
            CASE
                WHEN num_transactions > 100000 THEN '> 100k'
                WHEN num_transactions >= 10000 AND num_transactions < 100000 THEN '> 10k'
                WHEN num_transactions >= 1000 AND num_transactions < 10000 THEN '> 1k'
                WHEN num_transactions >= 100 AND num_transactions < 1000 THEN '> 100'
                WHEN num_transactions >= 50 AND num_transactions < 100 THEN '50-100'
                WHEN num_transactions >= 20 AND num_transactions < 50 THEN '20-50'
                WHEN num_transactions >= 5 AND num_transactions < 20 THEN '5-20'
                WHEN num_transactions > 1 AND num_transactions < 5 THEN '2-5'
                WHEN num_transactions = 1 THEN 'One Time'
            END as "Transactions Profile", 
            COUNT(accts) as "No of Users"
        FROM 
        transactions_count 
        GROUP BY 1 
        ORDER BY 2 DESC 
)

SELECT * FROM count_profile

Таня, [28.03.2023 22:24]
with 
address(address) as (
    values
    (0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2), 
    (0xc098b2a3aa256d2140208c3de6543aaef5cd3a94)
), token_price_dec as (
    select date_trunc('day', minute) as time, symbol, contract_address, decimals, avg(price) as avg_price
    from prices.usd
    where minute >= cast('2022-11-06' as TIMESTAMP) and minute < cast('2022-11-07' as TIMESTAMP)
        and blockchain = 'ethereum'
        and contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
        and symbol = 'WETH'
    group by 1, 2, 3, 4
), eth_in as (
    select date_trunc('day', block_time) as time, 'in' as category, sum( cast(value as double) / pow(10, p.decimals)) as raw_amt, sum( cast(value as double) / pow(10, p.decimals) * p.avg_price) as usd_amt
    from ethereum.traces t
    left join token_price_dec p on p.time = date_trunc('day', block_time)
    where t.block_time >= cast('2022-11-06' as TIMESTAMP) and t.block_time < cast('2022-11-07' as TIMESTAMP)
        and t."to" in (select address from address) 
    group by 1, 2
), eth_out as (
    select date_trunc('day', block_time) as time, 'out' as category, -1 * sum( cast(value as double) / pow(10, p.decimals)) as raw_amt, -1 * sum( cast(value as double) / pow(10, p.decimals) * p.avg_price) as usd_amt
    from ethereum.traces t
    left join token_price_dec p on date_trunc('day', block_time) = p.time
    where t.block_time >= cast('2022-11-06' as TIMESTAMP) and t.block_time < cast('2022-11-07' as TIMESTAMP)
        and t."from" in (select address from address) 
    group by 1, 2
)
select sum(usd_amt) as usd_netflow
from (
    select time, category, raw_amt, usd_amt
    from eth_in
    union all 
    select time, category, raw_amt, usd_amt
    from eth_out
)
