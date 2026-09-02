with companies as (

    select * from {{ ref('stg_alpha_vantage__companies') }}

),

final as (

    select
        ticker,
        company_name,

        sector,
        industry,
        exchange,
        currency,
        market_cap,

        _extracted_at,
        current_timestamp() as _loaded_at

    from companies

)

select * from final
