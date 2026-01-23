CREATE TABLE IF NOT EXISTS stores (
    store_nbr INTEGER PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    type CHAR(1) NOT NULL,
    cluster INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS items (
    item_nbr INTEGER PRIMARY KEY,
    family VARCHAR(100) NOT NULL,
    class INTEGER NOT NULL,
    perishable SMALLINT NOT NULL CHECK (perishable IN (0, 1))
);

CREATE TABLE IF NOT EXISTS transactions (
    date DATE NOT NULL,
    store_nbr INTEGER NOT NULL REFERENCES stores(store_nbr),
    transactions INTEGER NOT NULL,
    PRIMARY KEY (date, store_nbr)
);

CREATE TABLE IF NOT EXISTS oil (
    date DATE PRIMARY KEY,
    dcoilwtico NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS holidays_events (
    date DATE NOT NULL,
    type VARCHAR(50) NOT NULL,
    locale VARCHAR(50) NOT NULL,
    locale_name VARCHAR(100),
    description TEXT,
    transferred BOOLEAN DEFAULT FALSE
);


CREATE TABLE IF NOT EXISTS train (
    id SERIAL,
    date DATE NOT NULL,
    store_nbr INTEGER NOT NULL REFERENCES stores(store_nbr),
    item_nbr INTEGER NOT NULL REFERENCES items(item_nbr),
    unit_sales NUMERIC(12, 5) NOT NULL,
    onpromotion BOOLEAN,
    PRIMARY KEY (id, date)
) PARTITION BY RANGE (date);


--Partitions tables by year

CREATE TABLE IF NOT EXISTS train_2013 PARTITION OF train
    FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');

CREATE TABLE IF NOT EXISTS train_2014 PARTITION OF train
    FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');

CREATE TABLE IF NOT EXISTS train_2015 PARTITION OF train
    FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');

CREATE TABLE IF NOT EXISTS train_2016 PARTITION OF train
    FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');

CREATE TABLE IF NOT EXISTS train_2017 PARTITION OF train
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');


--Helpfull indexes
CREATE INDEX IF NOT EXISTS idx_train_date ON train(date);
CREATE INDEX IF NOT EXISTS idx_train_store ON train(store_nbr);
CREATE INDEX IF NOT EXISTS idx_train_item ON train(item_nbr);
CREATE INDEX IF NOT EXISTS idx_train_date_store ON train(date, store_nbr);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_oil_date ON oil(date);
