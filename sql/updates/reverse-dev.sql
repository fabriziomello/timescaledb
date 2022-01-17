DROP VIEW _timescaledb_internal.hypertable_chunk_local_size;
DROP FUNCTION _timescaledb_internal.relation_size(relation REGCLASS);
DROP INDEX _timescaledb_catalog.chunk_constraint_dimension_slice_id_idx;
CREATE INDEX chunk_constraint_chunk_id_dimension_slice_id_idx ON _timescaledb_catalog.chunk_constraint (chunk_id, dimension_slice_id);

CREATE OR REPLACE FUNCTION timescaledb_experimental.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    hypertable_chunk         REGCLASS
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_continuous_agg_refresh_chunk' LANGUAGE C VOLATILE;
