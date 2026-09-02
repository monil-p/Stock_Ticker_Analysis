with daily_prices as (

    select * from {{ ref('stg_alpha_vantage__daily_prices') }}

),

with_prior_session as (

    select
        *,

        -- lag() reaches back one ROW within each ticker, ordered by date.
        -- partition by ticker is not optional: without it the window runs
        -- across company boundaries and AAPL's first row silently borrows
        -- AMZN's last close. The numbers stay plausible, which is worse
        -- than an obvious error.
        lag(close_price) over (
            partition by ticker
            order by trade_date
        ) as prev_close_price,

        lag(trade_date) over (
            partition by ticker
            order by trade_date
        ) as prev_trade_date

    from daily_prices

),

with_returns as (

    select
        *,

        -- Prices are NUMERIC because money must be exact. A return is a ratio,
        -- not money, so it becomes FLOAT64: NUMERIC would round it to nine
        -- decimal places and the precision matters more than the exactness.
        safe_divide(
            cast(close_price - prev_close_price as float64),
            cast(prev_close_price as float64)
        ) as daily_return,

        -- Calendar days since the prior session. 1 over a normal week, 3 across
        -- a weekend, more across holidays. Downstream this is how you tell a
        -- market closure from genuinely missing data.
        date_diff(trade_date, prev_trade_date, day) as days_since_prev_session

    from with_prior_session

)

select
    ticker,
    trade_date,
    prev_trade_date,
    days_since_prev_session,

    open_price,
    high_price,
    low_price,
    close_price,
    prev_close_price,
    daily_return,

    volume,
    _extracted_at

from with_returns
