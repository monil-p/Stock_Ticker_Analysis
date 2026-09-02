{{ config(severity='warn') }}

-- Trading sessions should be close together: 1 calendar day mid-week, 3 across
-- a weekend, 4 when a Monday or Friday is a holiday. A larger gap means either
-- a genuine market closure or -- more likely -- a hole in the extract.
--
-- severity warn, not error. This cannot distinguish a real closure from a
-- missing extract, so it is a prompt to look rather than a reason to stop the
-- build. A test that cries wolf at 3am gets ignored, and then so do the others.

select
    ticker,
    trade_date,
    days_since_prev_session

from {{ ref('int_prices_rolling_windows') }}

where days_since_prev_session > 5
