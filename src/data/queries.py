
#VIEW CREATION FOR OIL
create_oil_clean_view = """ 
    DROP MATERIALIZED VIEW IF EXISTS oil_clean CASCADE;

    CREATE MATERIALIZED VIEW oil_clean AS
    -- Create a date series covering weeekens and holidays
    WITH date_series AS (
        SELECT generate_series(
            (SELECT MIN(date) FROM oil),
            (SELECT MAX(date) FROM oil),
            '1 day'::interval
        )::DATE AS date
    ),

    -- Left join with oil prices
    oil_with_gaps AS (
        SELECT
            date_series.date,
            oil.dcoilwtico AS oil_price_raw
        FROM date_series
        LEFT JOIN oil
        ON date_series.date = oil.date
    ),

    -- Forward fill, creating groups where each group start with a non-null value
    oil_forward_filled AS (
        SELECT 
            date,
            oil_price_raw,
            SUM(CASE WHEN oil_price_raw IS NOT NULL THEN 1 ELSE 0 END)
                OVER(ORDER BY date) AS fill_group
        FROM oil_with_gaps
    )

    SELECT
        date,
        -- Apply first value to fill gaps
        COALESCE(
            oil_price_raw, FIRST_VALUE(oil_price_raw)OVER(
                PARTITION BY fill_group
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                --Jan 1 case
                (SELECT dcoilwtico FROM oil WHERE dcoilwtico IS NOT NULL ORDER BY date ASC LIMIT 1)
            ) AS oil_price,
        CASE
            WHEN oil_price_raw IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_imputed
    FROM oil_forward_filled
    ORDER BY DATE;

    --INDEX
    CREATE INDEX idx_oil_clean_date ON oil_clean(date);

"""


# Train Sales Cleaning

create_train_clean_view = """
DROP MATERIALIZED VIEW IF EXISTS train_clean CASCADE;

CREATE MATERIALIZED VIEW train_clean AS
-- Identity sales with non-existent stores or items 
WITH
    combined_data AS (
    SELECT 
        id, 
        date, 
        store_nbr, 
        item_nbr, 
        unit_sales, 
        onpromotion
    FROM train
    WHERE unit_sales > 0
    
    UNION ALL
    
    SELECT 
        id, 
        date, 
        store_nbr, 
        item_nbr, 
        NULL AS unit_sales,
        onpromotion
    FROM test
), 
    
    valid_sales AS (
    SELECT
        combined_data.id,
        combined_data.date,
        combined_data.store_nbr,
        combined_data.item_nbr,
        combined_data.unit_sales,
        combined_data.onpromotion,
        CASE WHEN stores.store_nbr IS NULL THEN TRUE ELSE FALSE END AS is_orphan_store,
        CASE WHEN items.item_nbr IS NULL THEN TRUE ELSE FALSE END AS is_orphan_item
    FROM combined_data
    LEFT JOIN stores
    ON stores.store_nbr = combined_data.store_nbr
    LEFT JOIN items
    ON items.item_nbr = combined_data.item_nbr
)


SELECT
    id,
    date,
    store_nbr,
    item_nbr,
    unit_sales,
    onpromotion,
    is_orphan_store,
    is_orphan_item,
    CASE
        WHEN is_orphan_store OR is_orphan_item THEN TRUE
        ELSE FALSE
    END AS has_integrity_issues
FROM valid_sales
ORDER BY date, store_nbr, item_nbr;


--INDEX
CREATE INDEX idx_train_clean_composite ON train_clean(date, store_nbr, item_nbr);
CREATE INDEX idx_train_clean_item ON train_clean(item_nbr);
CREATE INDEX idx_train_clean_promo ON train_clean(onpromotion) WHERE onpromotion = TRUE;
"""


#View creation Holidays Clean

create_holidays_clean_view = """
    DROP MATERIALIZED VIEW IF EXISTS holidays_clean CASCADE;

    CREATE MATERIALIZED VIEW holidays_clean AS
    -- Exclude transferred holidays
    WITH filtered_holidays AS (
    SELECT
        date,
        type,
        locale,
        locale_name,
        description
    FROM holidays_events
    WHERE transferred = FALSE
        ),

    -- Aggregate multiple holidays on same date/location
    aggregated_holidays AS (
    SELECT
        date,
        locale,
        locale_name,
        STRING_AGG(DISTINCT type, ',' ORDER BY type) AS type,
        STRING_AGG(DISTINCT description, ',' ORDER BY description) AS description,
        COUNT(*) AS holiday_count
    FROM filtered_holidays
    GROUP BY date,locale, locale_name
    )

    SELECT
        date,
        type,
        locale,
        locale_name,
        description,
        holiday_count,
        CASE WHEN holiday_count> 1 THEN TRUE ELSE FALSE END AS is_multiple_holidays
    FROM aggregated_holidays
    ORDER BY date,locale,locale_name;

    --INDEX
    CREATE INDEX idx_holidays_clean_composite ON holidays_clean(date, locale, locale_name);

 """


#Master training table

create_master_training_data_view = """ 
    DROP MATERIALIZED VIEW IF EXISTS master_training_data CASCADE;

    CREATE MATERIALIZED VIEW master_training_data AS
    WITH base_data AS (
        --Unifying data from base tables
        SELECT
            train_clean.id,
            train_clean.date,
            train_clean.store_nbr,
            train_clean.item_nbr,
            train_clean.unit_sales,
            train_clean.onpromotion,
            stores.city,
            stores.state,
            stores.type AS store_type,
            stores.cluster AS store_cluster,
            items.family AS item_family,
            items.class AS item_class,
            items.perishable
        FROM train_clean
        LEFT JOIN stores
        ON train_clean.store_nbr = stores.store_nbr
        LEFT JOIN items
        ON train_clean.item_nbr = items.item_nbr
    ),


    data_with_oil AS (
        --Add oil prices
        SELECT
            base_data.*,
            oil_clean.oil_price,
            oil_clean.is_imputed AS oil_is_imputed
        FROM base_data
        LEFT JOIN oil_clean
        ON base_data.date = oil_clean.date
        ),


    data_with_holidays AS (
        -- Add holidays by locale type
        SELECT
            data_with_oil.*,
            holidays_clean.type AS holiday_type,
            holidays_clean.description AS holiday_description,
            CASE WHEN holidays_clean.date IS NOT NULL THEN TRUE ELSE FALSE END AS is_holiday
        FROM data_with_oil
        LEFT JOIN holidays_clean
        ON data_with_oil.date = holidays_clean.date
        AND (
            holidays_clean.locale = 'National'
            OR (holidays_clean.locale = 'Regional' AND holidays_clean.locale_name = data_with_oil.state)
            OR (holidays_clean.locale = 'Local' AND holidays_clean.locale_name = data_with_oil.city)
            )
    
    ),

    final_features AS (
        --Add time based features
        SELECT
            *,
            EXTRACT(DOW FROM date) AS day_of_week,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(DAY FROM date) AS day_of_month,
            EXTRACT(YEAR FROM date) AS year,
            EXTRACT(WEEK FROM date) AS week_of_year,

            CASE
                WHEN EXTRACT(DOW FROM date) IN (0,6) THEN TRUE 
                ELSE FALSE
            END AS is_weekend,

            
            CASE
                WHEN EXTRACT(DAY FROM date) = 15 
                    OR date = (DATE_TRUNC('month', date) + INTERVAL '1 month - 1 day')::DATE
                THEN TRUE 
                ELSE FALSE
            END AS is_payday,

            CASE
                WHEN EXTRACT(DAY FROM date)<=7
                THEN TRUE
                ELSE FALSE
            END AS is_month_start,

            CASE
                WHEN EXTRACT(DAY FROM date)>=24
                THEN TRUE
                ELSE FALSE
            END AS is_month_end

        FROM data_with_holidays
    )

    SELECT 
        id,
        date,
        store_nbr,
        item_nbr,

        unit_sales,

        onpromotion,
        city,
        state,
        store_type,
        store_cluster,

        item_family,
        item_class,
        perishable,

        oil_price,
        oil_is_imputed,

        is_holiday,
        holiday_type,
        holiday_description,

        year,
        month,
        week_of_year,
        day_of_month,
        day_of_week,

        is_weekend,
        is_payday,
        is_month_start,
        is_month_end
    
    FROM final_features
    ORDER BY date,store_nbr,item_nbr;

    --Index
    CREATE INDEX idx_master_composite ON master_training_data(date, store_nbr, item_nbr);

"""







# Validation queries

validate_oil_clean = """ 
    SELECT
        COUNT (*) AS total_rows,
        SUM(CASE WHEN is_imputed THEN 1 ELSE 0 END) as imputed_rows,
        MIN(date) as min_date,
        MAX(date) as max_date,
        MIN(oil_price) as min_price,
        MAX(oil_price) as max_price
    FROM oil_clean;    
"""

validate_train_clean = """ 
    SELECT
        COUNT (*) AS total_rows,
        COUNT(DISTINCT date) as unique_dates,
        COUNT(DISTINCT store_nbr) as unique_stores,
        COUNT(DISTINCT item_nbr) as unique_items,
        MIN(date) as min_date,
        MAX(date) as max_date,
        SUM(CASE WHEN has_integrity_issues THEN 1 ELSE 0 END) as integrity_issues,
        SUM(CASE WHEN is_orphan_store THEN 1 ELSE 0 END) as orphan_stores,
        SUM(CASE WHEN is_orphan_item THEN 1 ELSE 0 END) as orphan_items
    FROM train_clean
    WHERE date<'2017-08-16';
"""

validate_holidays_clean = """ 
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT date) AS unique_dates,
        COUNT(DISTINCT locale) AS unique_locales,
        COUNT(DISTINCT locale_name) AS unique_locale_names,
        MIN(date) AS min_date,
        MAX(date) AS max_date,
        SUM(CASE WHEN is_multiple_holidays THEN 1 ELSE 0 END) as multi_holidays_dates
    FROM holidays_clean;

"""

#Master training validations


validate_master_data = """ 
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT date) as unique_dates,
        COUNT(DISTINCT store_nbr) as unique_stores,
        COUNT(DISTINCT item_nbr) as unique_items,
        MIN(date) as min_date,
        MAX(date) as max_date,
        ROUND(AVG(unit_sales), 2) as avg_sales,
        ROUND(AVG(oil_price), 2) as avg_oil_price,
        SUM(CASE WHEN onpromotion THEN 1 ELSE 0 END) as promotion_count,
        SUM(CASE WHEN is_holiday THEN 1 ELSE 0 END) as holiday_count,
        SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) as weekend_count,
        SUM(CASE WHEN is_payday THEN 1 ELSE 0 END) as payday_count,
        COUNT(DISTINCT item_family) as unique_families,
        COUNT(DISTINCT store_type) as unique_store_types
        FROM master_training_data
        WHERE date<'2017-08-16';

"""

holiday_distribution = """
    SELECT 
        holiday_type,
        COUNT(*) as occurrences,
        COUNT(DISTINCT date) as unique_dates,
        ROUND(AVG(unit_sales), 2) as avg_sales_on_holiday
    FROM master_training_data
    WHERE is_holiday = TRUE
    AND date<'2017-08-16'
    GROUP BY holiday_type
    ORDER BY occurrences DESC;

    """

payday_impact_preview = """
    SELECT 
        is_payday,
        COUNT(*) as transactions,
        ROUND(AVG(unit_sales), 2) as avg_sales,
        ROUND(STDDEV(unit_sales), 2) as std_sales
    FROM master_training_data
    WHERE date<'2017-08-16'
    GROUP BY is_payday
    ORDER BY is_payday DESC;

    """

top_selling_families = """
    SELECT 
        item_family,
        COUNT(*) as transactions,
        ROUND(SUM(unit_sales), 2) as total_sales
    FROM master_training_data
    WHERE date<'2017-08-16'
    GROUP BY item_family
    ORDER BY total_sales DESC
    LIMIT 5;

    """
