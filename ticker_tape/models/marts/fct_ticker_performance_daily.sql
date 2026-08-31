with rolling_windows as (

    select * from {{ ref('int_prices_rolling_windows') }}

),

final as (

    select
        ticker,
        trade_date,

        open_price,
        high_price,
        low_price,
        close_price,
        daily_return,

        close_ma_7_session,
        close_ma_30_session,
        close_ma_50_session,
        volatility_30_session_annualized,
        golden_cross_flag,

        volume,
        _extracted_at,
        current_timestamp() as _loaded_at

    from rolling_windows

)

select * from final