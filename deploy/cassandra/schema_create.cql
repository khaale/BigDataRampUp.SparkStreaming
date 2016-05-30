CREATE TYPE ad_exchange (
    id smallint,
    name text,
    description text
);

CREATE TYPE city (
    id int,
    name text,
    state_id int,
    population int,
    area float,
    density float,
    latitude float,
    longitude float
);

CREATE TYPE log_type (
    id int,
    name text
);

CREATE TYPE state (
    id int,
    name text,
    population int,
    gsp int
);

CREATE TYPE user_agent (
    ua_type text,
    ua_family text,
    os_name text,
    device text
);

CREATE TYPE user_tags (
    id bigint,
    keywords frozen<set<text>>,
    status text,
    pricing_type text,
    match_type text,
    destination_url text
);

CREATE TABLE bidding_bundle (
    ipinyou_id text,
    ts timestamp,
    bid_id text,
    ad_exchange smallint,
    ad_slot_format text,
    ad_slot_height int,
    ad_slot_id text,
    ad_slot_visibility int,
    ad_slot_width int,
    advertiser_id int,
    anonymous_url text,
    bidding_price decimal,
    city int,
    creative_id text,
    domain text,
    ext_ad_exchange frozen<ad_exchange>,
    ext_city frozen<city>,
    ext_log_type frozen<log_type>,
    ext_state frozen<state>,
    ext_user_agent frozen<user_agent>,
    ext_user_tags frozen<user_tags>,
    ip text,
    paying_price decimal,
    region int,
    stream_id int,
    url text,
    user_agent text,
    user_tags bigint,
    PRIMARY KEY (ipinyou_id, ts, bid_id)
) WITH CLUSTERING ORDER BY (ts ASC, bid_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE bidding_session (
    ipinyou_id text,
    ts_start int,
    ad_exchange frozen<ad_exchange>,
    bid_counters map<frozen<log_type>, bigint>,
    city frozen<city>,
    keyword_counters map<text, bigint>,
    log_type frozen<log_type>,
    state frozen<state>,
    ts_end int,
    user_agent frozen<user_agent>,
    PRIMARY KEY (ipinyou_id, ts_start)
) WITH CLUSTERING ORDER BY (ts_start ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
