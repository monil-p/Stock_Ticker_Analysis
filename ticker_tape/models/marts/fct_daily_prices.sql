{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='daily_price_key',
        partition_by={
            'field': 'trade_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['ticker'],
        on_schema_change='append_new_columns'
    )
}}

with daily_prices as (

    select * from {{ ref('stg_alpha_vantage__daily_prices') }}

    {% if is_incremental() %}

    -- Three days of overlap rather than a strict "> max(trade_date)".
    --
    -- A strict cutoff never revisits a session it has already loaded, so an
    -- upstream restatement of yesterday's close would be invisible forever.
    -- Re-reading a short tail and letting the merge overwrite on
    -- daily_price_key is what makes this model self-correcting.
    --
    -- The subquery reads a single value from an existing partition, so it is
    -- cheap; the filter itself prunes partitions on the source scan.
    where trade_date >= (
        select date_sub(max(trade_date), interval 3 day) from {{ this }}
    )

    {% endif %}

),

final as (

    select
        -- Deterministic hash of the grain. Two rows with the same ticker and
        -- date always produce the same key, which is what lets the merge
        -- recognise a restated session as an update rather than an insert.
        {{ dbt_utils.generate_surrogate_key(['ticker', 'trade_date']) }}
            as daily_price_key,

        ticker,
        trade_date,

        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        _extracted_at,
        current_timestamp() as _loaded_at

    from daily_prices

)

select * from final
