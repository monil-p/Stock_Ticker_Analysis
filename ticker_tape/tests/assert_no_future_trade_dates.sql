-- No session may be dated in the future.
--
-- Alpha Vantage reports US market dates while the loader stamps _extracted_at
-- in UTC, so a timezone mistake in the extract surfaces here as a session
-- dated tomorrow. Those rows look entirely normal in isolation, which is
-- exactly why they need an explicit assertion.

select ticker, trade_date, 'fct_daily_prices' as source_model
from {{ ref('fct_daily_prices') }}
where trade_date > current_date()

union all

select ticker, trade_date, 'fct_ticker_performance_daily' as source_model
from {{ ref('fct_ticker_performance_daily') }}
where trade_date > current_date()
