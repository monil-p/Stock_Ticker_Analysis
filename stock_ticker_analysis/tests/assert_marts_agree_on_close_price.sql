-- The two fact marts reach close_price by different routes: fct_daily_prices
-- comes straight from staging, fct_ticker_performance_daily arrives via two
-- intermediate models. They must still agree on every session they share.
--
-- This is the test most likely to catch a real regression. A change to the
-- intermediate layer that quietly alters a price would leave every
-- single-model test passing, because each table is internally consistent --
-- they just disagree with each other.

select
    p.ticker,
    p.trade_date,
    d.close_price as close_from_daily_prices,
    p.close_price as close_from_performance

from {{ ref('fct_ticker_performance_daily') }} as p

inner join {{ ref('fct_daily_prices') }} as d
    on  p.ticker = d.ticker
    and p.trade_date = d.trade_date

where p.close_price != d.close_price
