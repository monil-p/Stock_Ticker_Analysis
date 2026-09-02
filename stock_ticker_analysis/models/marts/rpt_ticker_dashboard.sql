with performance as (

    select * from {{ ref('fct_ticker_performance_daily') }}

),

companies as (

    select * from {{ ref('dim_companies') }}

),

latest_session as (

    select max(trade_date) as max_trade_date from performance

),

joined as (

    select
        -- Company attributes, denormalised onto every row. Looker Studio can
        -- blend data sources but the join is fiddly and re-runs on every chart
        -- interaction; one flat view is faster to query and far simpler to
        -- build against.
        p.ticker,
        c.company_name,
        c.sector,
        c.industry,
        c.exchange,
        c.currency,
        c.market_cap,

        p.trade_date,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.volume,

        p.daily_return,
        p.close_ma_7_session,
        p.close_ma_30_session,
        p.close_ma_50_session,
        p.volatility_30_session_annualized,
        p.golden_cross_flag,

        -- Percentages are deliberately NOT pre-multiplied here. Looker Studio's
        -- Percent format multiplies by 100 itself, so shipping 29.8 and then
        -- formatting it as a percent renders 2,980%. Carrying both a fraction
        -- and a percentage of the same measure guarantees someone eventually
        -- picks the wrong one. One representation, formatted at the edge.

        -- A three-valued string rather than a nullable boolean. Looker Studio
        -- renders NULL booleans as blanks in legends and filter controls, which
        -- reads as a rendering bug rather than "not enough history yet".
        case
            when p.golden_cross_flag is true  then 'Uptrend'
            when p.golden_cross_flag is false then 'Downtrend'
            else 'Insufficient history'
        end as trend_label,

        -- Lets scorecards show "latest close" without a date filter, and lets
        -- charts use a relative window without date arithmetic in the UI.
        p.trade_date = l.max_trade_date as is_latest_session,
        date_diff(l.max_trade_date, p.trade_date, day) as days_from_latest

    from performance as p

    inner join companies as c
        on p.ticker = c.ticker

    cross join latest_session as l

)

select * from joined
