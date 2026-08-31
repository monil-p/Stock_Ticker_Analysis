-- Volatility is deliberately NULL until a ticker has 31 sessions of history,
-- because the earliest session has no return and stddev_samp skips NULLs.
--
-- The models yml already asserts the NULLs appear where they should. This
-- asserts the converse: once the warm-up is over, a value must actually be
-- there. Without it, a broken window frame that returned NULL everywhere
-- would pass every other test in the project.

select
    ticker,
    trade_date,
    session_index

from {{ ref('int_prices_rolling_windows') }}

where session_index >= 31
  and volatility_30_session_annualized is null
