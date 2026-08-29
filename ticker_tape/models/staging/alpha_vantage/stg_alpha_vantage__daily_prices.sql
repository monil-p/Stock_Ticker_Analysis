with source as (

    select * from {{ source('alpha_vantage', 'raw_daily_prices') }}

),

renamed as (

    select
        symbol                      as ticker,
        cast(trade_date as date)    as trade_date,

        cast(open as numeric)       as open_price,
        cast(high as numeric)       as high_price,
        cast(low as numeric)        as low_price,
        cast(close as numeric)      as close_price,
        cast(volume as int64)       as volume,

        _extracted_at

    from source

),

deduplicated as (

    -- Raw is append-only: every extract appends its whole window, so the same
    -- (ticker, trade_date) arrives once per run. Keep the newest observation.
    -- qualify filters on a window function without a self-join or subquery.
    select * from renamed
    qualify row_number() over (
        partition by ticker, trade_date
        order by _extracted_at desc
    ) = 1

)

select * from deduplicated
