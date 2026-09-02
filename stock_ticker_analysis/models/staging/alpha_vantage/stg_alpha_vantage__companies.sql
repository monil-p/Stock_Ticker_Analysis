with source as (

    select * from {{ source('alpha_vantage', 'raw_company_overview') }}

),

renamed as (

    select
        symbol                                as ticker,

        Name                                  as company_name,
        Sector                                as sector,
        Industry                              as industry,
        Exchange                              as exchange,
        Currency                              as currency,
        cast(MarketCapitalization as numeric) as market_cap,

        _extracted_at

    from source

),

deduplicated as (

    -- Grain is one row per ticker. Raw is append-only, so each extract adds a
    -- fresh profile row per ticker; keep the most recently extracted one.
    select * from renamed
    qualify row_number() over (
        partition by ticker
        order by _extracted_at desc
    ) = 1

)

select * from deduplicated
