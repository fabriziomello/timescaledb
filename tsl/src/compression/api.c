/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/* This file contains the implementation for SQL utility functions that
 *  compress and decompress chunks
 */
#include <postgres.h>
#include <access/tableam.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/pg_am.h>
#include <commands/event_trigger.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>
#include <libpq-fe.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>
#include <parser/parse_func.h>
#include <postgres_ext.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <tcop/utility.h>
#include <trigger.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/fmgrprotos.h>
#include <utils/inval.h>

#include "compat/compat.h"
#include "annotations.h"
#include "api.h"
#include "cache.h"
#include "chunk.h"
#include "compression.h"
#include "compression_storage.h"
#include "create.h"
#include "debug_point.h"
#include "error_utils.h"
#include "errors.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "recompress.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/chunk_column_stats.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "utils.h"
#include "wal_utils.h"

typedef struct CompressChunkCxt
{
	Hypertable *srcht;
	Chunk *srcht_chunk;		 /* chunk from srcht */
	Hypertable *compress_ht; /*compressed table for srcht */
} CompressChunkCxt;

static Oid get_compressed_chunk_index_for_recompression(Chunk *uncompressed_chunk);

static Node *
create_dummy_query()
{
	RawStmt *query = NULL;
	query = makeNode(RawStmt);
	query->stmt = (Node *) makeNode(SelectStmt);
	return (Node *) query;
}

void
compression_chunk_size_catalog_insert(int32 src_chunk_id, const RelationSize *src_size,
									  int32 compress_chunk_id, const RelationSize *compress_size,
									  int64 rowcnt_pre_compression, int64 rowcnt_post_compression,
									  int64 rowcnt_frozen)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	CatalogSecurityContext sec_ctx;

	Datum values[Natts_compression_chunk_size];
	bool nulls[Natts_compression_chunk_size] = { false };

	rel = table_open(catalog_get_table_id(catalog, COMPRESSION_CHUNK_SIZE), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));

	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_chunk_id)] =
		Int32GetDatum(src_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_chunk_id)] =
		Int32GetDatum(compress_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] =
		Int64GetDatum(src_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] =
		Int64GetDatum(src_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] =
		Int64GetDatum(src_size->index_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
		Int64GetDatum(compress_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
		Int64GetDatum(compress_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
		Int64GetDatum(compress_size->index_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
		Int64GetDatum(rowcnt_pre_compression);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
		Int64GetDatum(rowcnt_post_compression);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_frozen_immediately)] =
		Int64GetDatum(rowcnt_frozen);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

static int
compression_chunk_size_catalog_update_merged(int32 chunk_id, const RelationSize *size,
											 int32 merge_chunk_id, const RelationSize *merge_size,
											 int64 merge_rowcnt_pre_compression,
											 int64 merge_rowcnt_post_compression)
{
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, RowExclusiveLock, CurrentMemoryContext);
	bool updated = false;

	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), COMPRESSION_CHUNK_SIZE, COMPRESSION_CHUNK_SIZE_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_compression_chunk_size_pkey_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
	ts_scanner_foreach(&iterator)
	{
		Datum values[Natts_compression_chunk_size];
		bool replIsnull[Natts_compression_chunk_size] = { false };
		bool repl[Natts_compression_chunk_size] = { false };
		bool should_free;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple new_tuple;
		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull);

		/* Increment existing sizes with sizes from uncompressed chunk. */
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] =
			Int64GetDatum(size->heap_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_heap_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] =
			Int64GetDatum(size->toast_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_toast_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] =
			Int64GetDatum(size->index_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_index_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
			Int64GetDatum(merge_size->heap_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
			Int64GetDatum(merge_size->toast_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
			Int64GetDatum(merge_size->index_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
			Int64GetDatum(merge_rowcnt_pre_compression +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_numrows_pre_compression)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
			Int64GetDatum(merge_rowcnt_post_compression +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_numrows_post_compression)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] = true;

		new_tuple =
			heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull, repl);
		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

		updated = true;
		break;
	}

	ts_scan_iterator_end(&iterator);
	ts_scan_iterator_close(&iterator);
	return updated;
}

static void
get_hypertable_or_cagg_name(Hypertable *ht, Name objname)
{
	ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
	if (status == HypertableIsNotContinuousAgg || status == HypertableIsRawTable)
		namestrcpy(objname, NameStr(ht->fd.table_name));
	else if (status == HypertableIsMaterialization)
	{
		ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id, false);
		namestrcpy(objname, NameStr(cagg->data.user_view_name));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unexpected hypertable status for %s %d",
						NameStr(ht->fd.table_name),
						status)));
	}
}

static void
compresschunkcxt_init(CompressChunkCxt *cxt, Cache *hcache, Oid hypertable_relid, Oid chunk_relid)
{
	Hypertable *srcht = ts_hypertable_cache_get_entry(hcache, hypertable_relid, CACHE_FLAG_NONE);
	Hypertable *compress_ht;
	Chunk *srcchunk;

	ts_hypertable_permissions_check(srcht->main_table_relid, GetUserId());

	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(srcht))
	{
		NameData cagg_ht_name;
		get_hypertable_or_cagg_name(srcht, &cagg_ht_name);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnstore not enabled on \"%s\"", NameStr(cagg_ht_name)),
				 errdetail("It is not possible to convert chunks to columnstore on a hypertable or"
						   " continuous aggregate that does not have columnstore enabled."),
				 errhint("Enable columnstore using ALTER TABLE/MATERIALIZED VIEW with"
						 " the timescaledb.enable_columnstore option.")));
	}
	compress_ht = ts_hypertable_get_by_id(srcht->fd.compressed_hypertable_id);
	if (compress_ht == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("missing columnstore-enabled hypertable")));
	/* user has to be the owner of the compression table too */
	ts_hypertable_permissions_check(compress_ht->main_table_relid, GetUserId());

	if (!srcht->space) /* something is wrong */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing hyperspace for hypertable")));
	/* refetch the srcchunk with all attributes filled in */
	srcchunk = ts_chunk_get_by_relid(chunk_relid, true);
	ts_chunk_validate_chunk_status_for_operation(srcchunk, CHUNK_COMPRESS, true);
	cxt->srcht = srcht;
	cxt->compress_ht = compress_ht;
	cxt->srcht_chunk = srcchunk;
}

static Chunk *
find_chunk_to_merge_into(Hypertable *ht, Chunk *current_chunk)
{
	int64 max_chunk_interval, current_chunk_interval = 0, compressed_chunk_interval = 0;
	Chunk *previous_chunk;
	Point *p;

	const Dimension *time_dim = hyperspace_get_open_dimension(ht->space, 0);

	if (!time_dim || time_dim->fd.compress_interval_length == 0)
		return NULL;

	Assert(current_chunk->cube->num_slices > 0);
	Assert(current_chunk->cube->slices[0]->fd.dimension_id == time_dim->fd.id);

	max_chunk_interval = time_dim->fd.compress_interval_length;

	p = ts_point_create(current_chunk->cube->num_slices);

	/* First coordinate is the time coordinates and we want it to fall into previous chunk
	 * hence we reduce it by 1
	 */
	p->coordinates[p->num_coords++] = current_chunk->cube->slices[0]->fd.range_start - 1;
	current_chunk_interval = current_chunk->cube->slices[0]->fd.range_end -
							 current_chunk->cube->slices[0]->fd.range_start;

	for (int i = p->num_coords; i < current_chunk->cube->num_slices; i++)
	{
		p->coordinates[p->num_coords++] = current_chunk->cube->slices[i]->fd.range_start;
	}

	previous_chunk = ts_hypertable_find_chunk_for_point(ht, p);

	/* If there is no previous adjacent chunk along the time dimension or
	 * if it hasn't been compressed yet, we can't merge.
	 */
	if (!previous_chunk || !OidIsValid(previous_chunk->fd.compressed_chunk_id))
		return NULL;

	Assert(previous_chunk->cube->num_slices > 0);
	Assert(previous_chunk->cube->slices[0]->fd.dimension_id == time_dim->fd.id);

	compressed_chunk_interval = previous_chunk->cube->slices[0]->fd.range_end -
								previous_chunk->cube->slices[0]->fd.range_start;

	/* If the slices do not match (except on time dimension), we cannot merge the chunks. */
	if (previous_chunk->cube->num_slices != current_chunk->cube->num_slices)
		return NULL;

	for (int i = 1; i < previous_chunk->cube->num_slices; i++)
	{
		if (previous_chunk->cube->slices[i]->fd.id != current_chunk->cube->slices[i]->fd.id)
		{
			return NULL;
		}
	}

	/* If the compressed chunk is full, we can't merge any more. */
	if (compressed_chunk_interval == 0 ||
		compressed_chunk_interval + current_chunk_interval > max_chunk_interval)
		return NULL;

	/* Get reloid of the previous compressed chunk via settings */
	CompressionSettings *prev_comp_settings = ts_compression_settings_get(previous_chunk->table_id);
	CompressionSettings *ht_comp_settings = ts_compression_settings_get(ht->main_table_relid);
	if (!ts_compression_settings_equal(ht_comp_settings, prev_comp_settings))
		return NULL;

	/* We don't support merging chunks with sequence numbers */
	if (get_attnum(prev_comp_settings->fd.compress_relid,
				   COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) != InvalidAttrNumber)
		return NULL;

	return previous_chunk;
}

/* Check if compression order is violated by merging in a new chunk
 * Because data merged in uses higher sequence numbers than any data already in the chunk,
 * the only way the order is guaranteed can be if we know the data we are merging in would come
 * after the existing data according to the compression order. This is true if the data being merged
 * in has timestamps greater than the existing data and the first column in the order by is time
 * ASC.
 *
 * The CompressChunkCxt references the chunk we are merging and mergable_chunk is the chunk we
 * are merging into.
 */
static bool
check_is_chunk_order_violated_by_merge(CompressChunkCxt *cxt, const Dimension *time_dim,
									   Chunk *mergable_chunk)
{
	const DimensionSlice *mergable_slice =
		ts_hypercube_get_slice_by_dimension_id(mergable_chunk->cube, time_dim->fd.id);
	if (!mergable_slice)
		elog(ERROR, "mergeable chunk has no time dimension slice");
	const DimensionSlice *compressed_slice =
		ts_hypercube_get_slice_by_dimension_id(cxt->srcht_chunk->cube, time_dim->fd.id);
	if (!compressed_slice)
		elog(ERROR, "columnstore chunk has no time dimension slice");
	/*
	 * Ensure the compressed chunk is AFTER the chunk that
	 * it is being merged into. This is already guaranteed by previous checks.
	 */
	Ensure(mergable_slice->fd.range_end == compressed_slice->fd.range_start,
		   "chunk being merged is not after the chunk that is being merged into");
	CompressionSettings *ht_settings =
		ts_compression_settings_get(mergable_chunk->hypertable_relid);

	char *attname = get_attname(cxt->srcht->main_table_relid, time_dim->column_attno, false);
	int index = ts_array_position(ht_settings->fd.orderby, attname);

	/* Primary dimension column should be first compress_orderby column. */
	if (index != 1)
		return true;

	return false;
}

static Oid
compress_chunk_impl(Oid hypertable_relid, Oid chunk_relid)
{
	Oid result_chunk_id = chunk_relid;
	CompressChunkCxt cxt = { 0 };
	Chunk *compress_ht_chunk, *mergable_chunk;
	Cache *hcache;
	RelationSize before_size, after_size;
	CompressionStats cstat;
	bool new_compressed_chunk = false;

	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, hypertable_relid, chunk_relid);

	/* acquire locks on src and compress hypertable and src chunk */
	ereport(DEBUG1,
			(errmsg("acquiring locks for converting to columnstore \"%s.%s\"",
					get_namespace_name(get_rel_namespace(chunk_relid)),
					get_rel_name(chunk_relid))));
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, ExclusiveLock);

	/* acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);
	ereport(DEBUG1,
			(errmsg("locks acquired for converting to columnstore \"%s.%s\"",
					get_namespace_name(get_rel_namespace(chunk_relid)),
					get_rel_name(chunk_relid))));

	DEBUG_WAITPOINT("compress_chunk_impl_start");

	/*
	 * Re-read the state of the chunk after all locks have been acquired and ensure
	 * it is still uncompressed. Another process running in parallel might have
	 * already performed the compression while we were waiting for the locks to be
	 * acquired.
	 */
	Chunk *chunk_state_after_lock = ts_chunk_get_by_relid(chunk_relid, true);

	/* Throw error if chunk has invalid status for operation */
	ts_chunk_validate_chunk_status_for_operation(chunk_state_after_lock, CHUNK_COMPRESS, true);

	/* get compression properties for hypertable */
	mergable_chunk = find_chunk_to_merge_into(cxt.srcht, cxt.srcht_chunk);
	if (!mergable_chunk)
	{
		/*
		 * Set up a dummy parsetree since we're calling AlterTableInternal
		 * inside create_compress_chunk(). We can use anything here because we
		 * are not calling EventTriggerDDLCommandEnd but we use a parse tree
		 * type that CreateCommandTag can handle to avoid spurious printouts
		 * in the event that EventTriggerDDLCommandEnd is called.
		 */
		EventTriggerAlterTableStart(create_dummy_query());
		/* create compressed chunk and a new table */
		compress_ht_chunk = create_compress_chunk(cxt.compress_ht, cxt.srcht_chunk, InvalidOid);
		/* Associate compressed chunk with main chunk. Needed for Hypercore
		 * TAM to not recreate the compressed chunk again when the main chunk
		 * rel is opened. */
		ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id);
		new_compressed_chunk = true;
		ereport(DEBUG1,
				(errmsg("new columnstore chunk \"%s.%s\" created",
						NameStr(compress_ht_chunk->fd.schema_name),
						NameStr(compress_ht_chunk->fd.table_name))));

		EventTriggerAlterTableEnd();
	}
	else
	{
		/* use an existing compressed chunk to compress into */
		compress_ht_chunk = ts_chunk_get_by_id(mergable_chunk->fd.compressed_chunk_id, true);
		result_chunk_id = mergable_chunk->table_id;
		ereport(DEBUG1,
				(errmsg("merge into existing columnstore chunk \"%s.%s\"",
						NameStr(compress_ht_chunk->fd.schema_name),
						NameStr(compress_ht_chunk->fd.table_name))));
	}

	/* Since the compressed relation is created in the same transaction as the tuples that will be
	 * written by the compressor, we can insert the tuple directly in frozen state. This is the same
	 * logic as performed in COPY INSERT FROZEN.
	 *
	 * Note: Tuples inserted with HEAP_INSERT_FROZEN become immediately visible to all transactions
	 * (they violate the MVCC pattern). So, this flag can only be used when creating the compressed
	 * chunk in the same transaction as the compressed tuples are inserted.
	 *
	 * If this isn't the case, then tuples can be seen multiple times by parallel readers - once in
	 * the uncompressed part of the hypertable (since they are not deleted in the transaction) and
	 * once in the compressed part of the hypertable since the MVCC semantic is violated due to the
	 * flag.
	 *
	 * In contrast, when the compressed chunk part is created in the same transaction as the tuples
	 * are written, the compressed chunk (i.e., the catalog entry) becomes visible to other
	 * transactions only after the transaction that performs the compression is committed and
	 * the uncompressed chunk is truncated.
	 */
	int insert_options = new_compressed_chunk ? HEAP_INSERT_FROZEN : 0;

	before_size = ts_relation_size_impl(cxt.srcht_chunk->table_id);

	cstat = compress_chunk(cxt.srcht_chunk->table_id, compress_ht_chunk->table_id, insert_options);
	after_size = ts_relation_size_impl(compress_ht_chunk->table_id);

	if (cxt.srcht->range_space)
		ts_chunk_column_stats_calculate(cxt.srcht, cxt.srcht_chunk);

	if (new_compressed_chunk)
	{
		compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
											  &before_size,
											  compress_ht_chunk->fd.id,
											  &after_size,
											  cstat.rowcnt_pre_compression,
											  cstat.rowcnt_post_compression,
											  cstat.rowcnt_frozen);

		/* Detect and emit warning if poor compression ratio is found */
		float compression_ratio = ((float) before_size.total_size / after_size.total_size);
		float POOR_COMPRESSION_THRESHOLD = 1.0;
		if (ts_guc_enable_compression_ratio_warnings &&
			compression_ratio < POOR_COMPRESSION_THRESHOLD)
			ereport(WARNING,
					errcode(ERRCODE_WARNING),
					errmsg("poor compression rate detected for chunk \"%s\"'",
						   get_rel_name(chunk_relid)),
					errdetail("Chunk \"%s\" has a poor compression ratio: %.2f. Size before "
							  "compression: " INT64_FORMAT
							  " bytes. Size after compression: " INT64_FORMAT " bytes",
							  get_rel_name(chunk_relid),
							  compression_ratio,
							  before_size.total_size,
							  after_size.total_size),
					errhint("Changing compression settings for \"%s\" can improve compression rate",
							get_rel_name(hypertable_relid)));
	}
	else
	{
		compression_chunk_size_catalog_update_merged(mergable_chunk->fd.id,
													 &before_size,
													 compress_ht_chunk->fd.id,
													 &after_size,
													 cstat.rowcnt_pre_compression,
													 cstat.rowcnt_post_compression);

		const Dimension *time_dim = hyperspace_get_open_dimension(cxt.srcht->space, 0);
		Assert(time_dim != NULL);

		bool chunk_unordered =
			check_is_chunk_order_violated_by_merge(&cxt, time_dim, mergable_chunk);

		ts_chunk_merge_on_dimension(cxt.srcht, mergable_chunk, cxt.srcht_chunk, time_dim->fd.id);

		if (chunk_unordered)
		{
			ts_chunk_set_unordered(mergable_chunk);
			tsl_compress_chunk_wrapper(mergable_chunk, true, false);
		}
	}

	ts_cache_release(&hcache);
	return result_chunk_id;
}

static void
decompress_chunk_impl(Chunk *uncompressed_chunk, bool if_compressed)
{
	Cache *hcache;
	Hypertable *uncompressed_hypertable =
		ts_hypertable_cache_get_cache_and_entry(uncompressed_chunk->hypertable_relid,
												CACHE_FLAG_NONE,
												&hcache);
	Hypertable *compressed_hypertable;
	Chunk *compressed_chunk;

	ts_hypertable_permissions_check(uncompressed_hypertable->main_table_relid, GetUserId());

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(uncompressed_hypertable))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg(
					 "convert_to_rowstore must not be called on the internal columnstore chunk")));

	compressed_hypertable =
		ts_hypertable_get_by_id(uncompressed_hypertable->fd.compressed_hypertable_id);
	if (compressed_hypertable == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("missing columnstore-enabled hypertable")));

	if (uncompressed_chunk->fd.hypertable_id != uncompressed_hypertable->fd.id)
		elog(ERROR, "hypertable and chunk do not match");

	if (uncompressed_chunk->fd.compressed_chunk_id == INVALID_CHUNK_ID)
	{
		ts_cache_release(&hcache);
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk \"%s\" is not converted to columnstore",
						get_rel_name(uncompressed_chunk->table_id))));
		return;
	}

	write_logical_replication_msg_decompression_start();

	ts_chunk_validate_chunk_status_for_operation(uncompressed_chunk, CHUNK_DECOMPRESS, true);
	compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);

	ereport(DEBUG1,
			(errmsg("acquiring locks for converting to rowstore \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));
	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(uncompressed_hypertable->main_table_relid, AccessShareLock);
	LockRelationOid(compressed_hypertable->main_table_relid, AccessShareLock);

	/*
	 * Acquire an ExclusiveLock on the uncompressed and the compressed
	 * chunk (the chunks can still be accessed by reads).
	 *
	 * The lock on the compressed chunk is needed because it gets deleted
	 * after decompression. The lock on the uncompressed chunk is needed
	 * to avoid deadlocks (e.g., caused by later lock upgrades or parallel
	 * started chunk compressions).
	 *
	 * Note: Also the function decompress_chunk() will request an
	 *       ExclusiveLock on the compressed and on the uncompressed
	 *       chunk. See the comments in function about the concurrency of
	 *       operations.
	 */
	LockRelationOid(uncompressed_chunk->table_id, ExclusiveLock);
	LockRelationOid(compressed_chunk->table_id, ExclusiveLock);

	/* acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);
	ereport(DEBUG1,
			(errmsg("locks acquired for converting to rowstore \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	DEBUG_WAITPOINT("decompress_chunk_impl_start");

	/*
	 * Re-read the state of the chunk after all locks have been acquired and ensure
	 * it is still compressed. Another process running in parallel might have
	 * already performed the decompression while we were waiting for the locks to be
	 * acquired.
	 */
	Chunk *chunk_state_after_lock = ts_chunk_get_by_id(uncompressed_chunk->fd.id, true);

	/* Throw error if chunk has invalid status for operation */
	ts_chunk_validate_chunk_status_for_operation(chunk_state_after_lock, CHUNK_DECOMPRESS, true);

	decompress_chunk(compressed_chunk->table_id, uncompressed_chunk->table_id);

	/* Delete the compressed chunk */
	ts_compression_chunk_size_delete(uncompressed_chunk->fd.id);
	ts_chunk_clear_compressed_chunk(uncompressed_chunk);
	ts_compression_settings_delete(uncompressed_chunk->table_id);

	/*
	 * Lock the compressed chunk that is going to be deleted. At this point,
	 * the reference to the compressed chunk is already removed from the
	 * catalog but we need to block readers from accessing this chunk
	 * until the catalog changes are visible to them.
	 *
	 * Note: Calling performMultipleDeletions in chunk_index_tuple_delete
	 * also requests an AccessExclusiveLock on the compressed_chunk. However,
	 * this call makes the lock on the chunk explicit.
	 */
	LockRelationOid(uncompressed_chunk->table_id, AccessExclusiveLock);
	LockRelationOid(compressed_chunk->table_id, AccessExclusiveLock);
	ts_chunk_drop(compressed_chunk, DROP_RESTRICT, -1);
	ts_cache_release(&hcache);
	write_logical_replication_msg_decompression_end();
}

/*
 * Create a new compressed chunk using existing table with compressed data.
 *
 * chunk_relid - non-compressed chunk relid
 * chunk_table - table containing compressed data
 */
Datum
tsl_create_compressed_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_GETARG_OID(0);
	Oid chunk_table = PG_GETARG_OID(1);
	RelationSize uncompressed_size = { .heap_size = PG_GETARG_INT64(2),
									   .toast_size = PG_GETARG_INT64(3),
									   .index_size = PG_GETARG_INT64(4) };
	RelationSize compressed_size = { .heap_size = PG_GETARG_INT64(5),
									 .toast_size = PG_GETARG_INT64(6),
									 .index_size = PG_GETARG_INT64(7) };
	int64 numrows_pre_compression = PG_GETARG_INT64(8);
	int64 numrows_post_compression = PG_GETARG_INT64(9);
	Chunk *chunk;
	Chunk *compress_ht_chunk;
	Cache *hcache;
	CompressChunkCxt cxt;
	bool chunk_was_compressed;

	Assert(!PG_ARGISNULL(0));
	Assert(!PG_ARGISNULL(1));

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, chunk->hypertable_relid, chunk_relid);

	/* Acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, ShareLock);

	/* Acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	/*
	 * Set up a dummy parsetree since we're calling AlterTableInternal inside
	 * create_compress_chunk(). We can use anything here because we are not
	 * calling EventTriggerDDLCommandEnd but we use a parse tree type that
	 * CreateCommandTag can handle to avoid spurious printouts.
	 */
	EventTriggerAlterTableStart(create_dummy_query());
	/* Create compressed chunk using existing table */
	compress_ht_chunk = create_compress_chunk(cxt.compress_ht, cxt.srcht_chunk, chunk_table);
	EventTriggerAlterTableEnd();

	/* Insert empty stats to compression_chunk_size */
	compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
										  &uncompressed_size,
										  compress_ht_chunk->fd.id,
										  &compressed_size,
										  numrows_pre_compression,
										  numrows_post_compression,
										  0);

	chunk_was_compressed = ts_chunk_is_compressed(cxt.srcht_chunk);
	ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id);
	if (!chunk_was_compressed && ts_table_has_tuples(cxt.srcht_chunk->table_id, AccessShareLock))
	{
		/* The chunk was not compressed before it had the compressed chunk
		 * attached to it, and it contains rows, so we set it to be partial.
		 */
		ts_chunk_set_partial(cxt.srcht_chunk);
	}
	ts_cache_release(&hcache);

	PG_RETURN_OID(chunk_relid);
}

Datum
tsl_compress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? true : PG_GETARG_BOOL(1);
	bool recompress = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	uncompressed_chunk_id = tsl_compress_chunk_wrapper(chunk, if_not_compressed, recompress);

	PG_RETURN_OID(uncompressed_chunk_id);
}

Oid
tsl_compress_chunk_wrapper(Chunk *chunk, bool if_not_compressed, bool recompress)
{
	Oid uncompressed_chunk_id = chunk->table_id;

	write_logical_replication_msg_compression_start();

	if (ts_chunk_is_compressed(chunk))
	{
		CompressionSettings *chunk_settings = ts_compression_settings_get(chunk->table_id);
		bool valid_orderby_settings = chunk_settings && chunk_settings->fd.orderby;
		if (recompress)
		{
			CompressionSettings *ht_settings = ts_compression_settings_get(chunk->hypertable_relid);

			if (!valid_orderby_settings ||
				!ts_compression_settings_equal(ht_settings, chunk_settings))
			{
				decompress_chunk_impl(chunk, false);
				compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
				write_logical_replication_msg_compression_end();
				return uncompressed_chunk_id;
			}
		}
		if (!ts_chunk_needs_recompression(chunk))
		{
			write_logical_replication_msg_compression_end();
			ereport((if_not_compressed ? NOTICE : ERROR),
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("chunk \"%s\" is already converted to columnstore",
							get_rel_name(chunk->table_id))));
			return uncompressed_chunk_id;
		}

		if (ts_guc_enable_segmentwise_recompression && valid_orderby_settings &&
			ts_chunk_is_partial(chunk) && get_compressed_chunk_index_for_recompression(chunk))
		{
			uncompressed_chunk_id = recompress_chunk_segmentwise_impl(chunk);
		}
		else
		{
			if (!ts_guc_enable_segmentwise_recompression || !valid_orderby_settings)
				elog(NOTICE,
					 "segmentwise recompression is disabled%s, performing full "
					 "recompression on "
					 "chunk \"%s.%s\"",
					 (ts_guc_enable_segmentwise_recompression && !valid_orderby_settings ?
						  " due to no order by" :
						  ""),
					 NameStr(chunk->fd.schema_name),
					 NameStr(chunk->fd.table_name));
			decompress_chunk_impl(chunk, false);
			compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
		}
	}
	else
	{
		uncompressed_chunk_id = compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
	}

	write_logical_replication_msg_compression_end();

	return uncompressed_chunk_id;
}

Datum
tsl_decompress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_compressed = PG_ARGISNULL(1) ? true : PG_GETARG_BOOL(1);
	int32 chunk_id;

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	TS_PREVENT_FUNC_IF_READ_ONLY();

	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);
	chunk_id = uncompressed_chunk->fd.id;

	Hypertable *ht = ts_hypertable_get_by_id(uncompressed_chunk->fd.hypertable_id);
	ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());

	if (!ht->fd.compressed_hypertable_id)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("missing columnstore-enabled hypertable")));

	if (!ts_chunk_is_compressed(uncompressed_chunk))
	{
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk \"%s\" is not converted to columnstore",
						get_rel_name(uncompressed_chunk_id))));

		PG_RETURN_NULL();
	}
	else
		decompress_chunk_impl(uncompressed_chunk, if_compressed);

	/*
	 * Post decompression regular DML can happen into this chunk. So, we update
	 * chunk_column_stats entries for this chunk to min/max entries now.
	 */
	ts_chunk_column_stats_reset_by_chunk_id(chunk_id);

	PG_RETURN_OID(uncompressed_chunk_id);
}

/*
 * This is hacky but it doesn't matter. We just want to check for the existence of such an index
 * on the compressed chunk.
 */
extern Datum
tsl_get_compressed_chunk_index_for_recompression(PG_FUNCTION_ARGS)
{
	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);

	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	Oid index_oid = get_compressed_chunk_index_for_recompression(uncompressed_chunk);

	if (OidIsValid(index_oid))
	{
		PG_RETURN_OID(index_oid);
	}
	else
		PG_RETURN_NULL();
}

static Oid
get_compressed_chunk_index_for_recompression(Chunk *uncompressed_chunk)
{
	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);
	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, AccessShareLock);
	Relation compressed_chunk_rel = table_open(compressed_chunk->table_id, AccessShareLock);

	CompressionSettings *settings = ts_compression_settings_get(uncompressed_chunk->table_id);

	CatalogIndexState indstate = CatalogOpenIndexes(compressed_chunk_rel);
	Oid index_oid = get_compressed_chunk_index(indstate, settings);
	CatalogCloseIndexes(indstate);
	table_close(compressed_chunk_rel, NoLock);
	table_close(uncompressed_chunk_rel, NoLock);

	return index_oid;
}

Chunk *
tsl_compression_chunk_create(Hypertable *compressed_ht, Chunk *src_chunk)
{
	/* Create a new compressed chunk */
	return create_compress_chunk(compressed_ht, src_chunk, InvalidOid);
}
