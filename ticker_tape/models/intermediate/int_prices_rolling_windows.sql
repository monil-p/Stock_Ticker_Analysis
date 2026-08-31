with daily_returns as (

    select * from {{ ref('int_prices_daily_returns') }}

),

sequenced as (

    select
        *,

        -- How many sessions of history this ticker has up to and including
        -- this row. Used below to suppress windows that are not yet full.
        row_number() over (
            partition by ticker
            order by trade_date
        ) as session_index

    from daily_returns

),

rolling as (

    select
        *,

        -- Each frame counts ROWS, not calendar days, so "30 session" spans
        -- roughly six calendar weeks. The case guard suppresses the window
        -- until it is actually full: without it, row 5 would average five
        -- prices and present the result as a 30-session average.
        case when session_index >= 7 then
            avg(close_price) over (
                partition by ticker
                order by trade_date
                rows between 6 preceding and current row
            )
        end as close_ma_7_session,

        case when session_index >= 30 then
            avg(close_price) over (
                partition by ticker
                order by trade_date
                rows between 29 preceding and current row
            )
        end as close_ma_30_session,

        case when session_index >= 50 then
            avg(close_price) over (
                partition by ticker
                order by trade_date
                rows between 49 preceding and current row
            )
        end as close_ma_50_session,

        -- Threshold is 31, not 30: the earliest session of each ticker has a
        -- NULL daily_return (no prior close), and stddev_samp skips NULLs. A
        -- 30-row frame starting at session 30 therefore holds only 29 returns.
        --
        -- stddev_samp rather than stddev_pop because these 30 sessions are a
        -- sample of the ticker's return distribution, not the whole of it.
        -- sqrt(252) annualises: volatility scales with the square root of
        -- time, and a year holds roughly 252 trading sessions.
        case when session_index >= 31 then
            stddev_samp(daily_return) over (
                partition by ticker
                order by trade_date
                rows between 29 preceding and current row
            ) * sqrt(252)
        end as volatility_30_session_annualized

    from sequenced

),

signals as (

    select
        *,

        -- Computed in its own CTE because SQL cannot reference a select-list
        -- alias from within the same select list. NULL until both averages
        -- exist, which is the honest answer before session 50.
        close_ma_30_session > close_ma_50_session as golden_cross_flag

    from rolling

)

select
    ticker,
    trade_date,
    session_index,

    open_price,
    high_price,
    low_price,
    close_price,
    prev_close_price,
    daily_return,
    days_since_prev_session,

    close_ma_7_session,
    close_ma_30_session,
    close_ma_50_session,
    volatility_30_session_annualized,
    golden_cross_flag,

    volume,
    _extracted_at

from signals
