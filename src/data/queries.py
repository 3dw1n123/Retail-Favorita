
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
WITH valid_sales AS (
    SELECT
        train.id,
        train.date,
        train.store_nbr,
        train.item_nbr,
        train.unit_sales,
        train.onpromotion,
        CASE WHEN stores.store_nbr IS NULL THEN TRUE ELSE FALSE END AS is_orphan_store,
        CASE WHEN items.item_nbr IS NULL THEN TRUE ELSE FALSE END AS is_orphan_item
    FROM train
    LEFT JOIN stores
    ON stores.store_nbr = train.store_nbr
    LEFT JOIN items
    ON items.item_nbr = train.item_nbr

    -- Discard refunds
    WHERE
        train.unit_sales>0

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
    FROM train_clean;
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