
#ifndef LUNDGREN_CONSTANTS
#define LUNDGREN_CONSTANTS

#define INTERIM_TABLE_ENGINE "ENGINE = MEMORY"

#define PLUGIN_FLAG "distributed"

#define BATCH_SIZE 100000
#define BLOOM_SLAVE_BATCH_SIZE 100000

// SEMI (& bloom)
#define IGNORE_TABLE_PARTITIONS_FLAG "ignore_table_partitions"

// BLOOM JOIN

#define BLOOM_SLAVE_FLAG "bloom_slave"
#define BLOOM_FILTER_FLAG "bloom_filter"
#define BLOOM_FILTER_PARAMETER_COUNT_FLAG "filter_parameter_count"
#define BLOOM_FILTERED_INTERIM_NAME_FLAG "filtered_interim_name"
#define BLOOM_FILTER_REMOTE_TABLE_FLAG "remote_table_name"
#define BLOOM_FILTER_REMOTE_JOIN_COLUMN_FLAG "remote_join_column"

// HASH REDISTRIBUTION
#define HASH_REDIST_SLAVE_FLAG "hash_redist_slave"

// SORT MERGE
#define SORT_MERGE_BATCH_SIZE 100000


#endif  // LUNDGREN_CONSTANTS