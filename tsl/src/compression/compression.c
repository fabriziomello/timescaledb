/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attmap.h>
#include <access/attnum.h>
#include <access/skey.h>
#include <access/tupdesc.h>
#include <catalog/heap.h>
#include <catalog/indexing.h>
#include <catalog/pg_am.h>
#include <common/base64.h>
#include <funcapi.h>
#include <libpq/pqformat.h>
#include <storage/predicate.h>
#include <utils/datum.h>
#include <utils/elog.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "compat/compat.h"

#include "algorithms/array.h"
#include "algorithms/bool_compress.h"
#include "algorithms/deltadelta.h"
#include "algorithms/dictionary.h"
#include "algorithms/gorilla.h"
#include "algorithms/null.h"
#include "algorithms/uuid_compress.h"
#include "batch_metadata_builder.h"
#include "compression.h"
#include "create.h"
#include "custom_type_cache.h"
#include "debug_assert.h"
#include "debug_point.h"
#include "guc.h"
#include "nodes/chunk_dispatch/chunk_insert_state.h"
#include "nodes/modify_hypertable.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include <nodes/decompress_chunk/vector_quals.h>

/*
 * Timing parameters for truncate locking heuristics.
 * These are the same as used by Postgres for truncate locking during lazy vacuum.
 * https://github.com/postgres/postgres/blob/4a0650d359c5981270039eeb634c3b7427aa0af5/src/backend/access/heap/vacuumlazy.c#L82
 */
#define COMPRESS_TRUNCATE_LOCK_WAIT_INTERVAL 50 /* ms */
#define COMPRESS_TRUNCATE_LOCK_TIMEOUT 5000		/* ms */

StaticAssertDecl(GLOBAL_MAX_ROWS_PER_COMPRESSION >= TARGET_COMPRESSED_BATCH_SIZE,
				 "max row numbers must be harmonized");
StaticAssertDecl(GLOBAL_MAX_ROWS_PER_COMPRESSION <= INT16_MAX,
				 "dictionary compression uses signed int16 indexes");

static const CompressionAlgorithmDefinition definitions[_END_COMPRESSION_ALGORITHMS] = {
	[COMPRESSION_ALGORITHM_ARRAY] = ARRAY_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_DICTIONARY] = DICTIONARY_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_GORILLA] = GORILLA_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_DELTADELTA] = DELTA_DELTA_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_BOOL] = BOOL_COMPRESS_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_NULL] = NULL_COMPRESS_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_UUID] = UUID_COMPRESS_ALGORITHM_DEFINITION,
};

static NameData compression_algorithm_name[] = {
	[_INVALID_COMPRESSION_ALGORITHM] = { "INVALID" },
	[COMPRESSION_ALGORITHM_ARRAY] = { "ARRAY" },
	[COMPRESSION_ALGORITHM_DICTIONARY] = { "DICTIONARY" },
	[COMPRESSION_ALGORITHM_GORILLA] = { "GORILLA" },
	[COMPRESSION_ALGORITHM_DELTADELTA] = { "DELTADELTA" },
	[COMPRESSION_ALGORITHM_BOOL] = { "BOOL" },
	[COMPRESSION_ALGORITHM_NULL] = { "NULL" },
	[COMPRESSION_ALGORITHM_UUID] = { "UUID" },
};

Name
compression_get_algorithm_name(CompressionAlgorithm alg)
{
	return &compression_algorithm_name[alg];
}

static Compressor *
compressor_for_type(Oid type)
{
	CompressionAlgorithm algorithm = compression_get_default_algorithm(type);
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	return definitions[algorithm].compressor_for_type(type);
}

DecompressionInitializer
tsl_get_decompression_iterator_init(CompressionAlgorithm algorithm, bool reverse)
{
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	if (reverse)
		return definitions[algorithm].iterator_init_reverse;
	else
		return definitions[algorithm].iterator_init_forward;
}

DecompressAllFunction
tsl_get_decompress_all_function(CompressionAlgorithm algorithm, Oid type)
{
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	if (type != TEXTOID && type != BOOLOID && type != UUIDOID &&
		(algorithm == COMPRESSION_ALGORITHM_DICTIONARY || algorithm == COMPRESSION_ALGORITHM_ARRAY))
	{
		/* Bulk decompression of array and dictionary is only supported for
		 * text, bool and uuid */
		return NULL;
	}

	return definitions[algorithm].decompress_all;
}

static Tuplesortstate *compress_chunk_sort_relation(CompressionSettings *settings, Relation in_rel);
static void row_compressor_process_ordered_slot(RowCompressor *row_compressor, TupleTableSlot *slot,
												BulkWriter *writer);
static void row_compressor_update_group(RowCompressor *row_compressor, TupleTableSlot *row);
static bool row_compressor_new_row_is_in_new_group(RowCompressor *row_compressor,
												   TupleTableSlot *row);
static void create_per_compressed_column(RowDecompressor *decompressor);
static void row_compressor_append_row(RowCompressor *row_compressor, TupleTableSlot *row);
static void row_compressor_flush(RowCompressor *row_compressor, BulkWriter *writer,
								 bool changed_groups);

/********************
 ** compress_chunk **
 ********************/

static CompressedDataHeader *
get_compressed_data_header(Datum data)
{
	CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(data);

	if (header->compression_algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", header->compression_algorithm);

	return header;
}

/* Truncate the relation WITHOUT applying triggers. This is the
 * main difference with ExecuteTruncate. Triggers aren't applied
 * because the data remains, just in compressed form. Also don't
 * restart sequences. Use the transactional branch through ExecuteTruncate.
 */
static void
truncate_relation(Oid table_oid)
{
	List *fks = heap_truncate_find_FKs(list_make1_oid(table_oid));
	/* Take an access exclusive lock now. Note that this may very well
	 *  be a lock upgrade. */
	Relation rel = table_open(table_oid, AccessExclusiveLock);
	Oid toast_relid;

	/* Chunks should never have fks into them, but double check */
	if (fks != NIL)
		elog(ERROR, "found a FK into a chunk while truncating");

	CheckTableForSerializableConflictIn(rel);

#if PG16_LT
	RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
#else
	RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);
#endif

	toast_relid = rel->rd_rel->reltoastrelid;

	table_close(rel, NoLock);

	if (OidIsValid(toast_relid))
	{
		rel = table_open(toast_relid, AccessExclusiveLock);
#if PG16_LT
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
#else
		RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);
#endif
		Assert(rel->rd_rel->relpersistence != RELPERSISTENCE_UNLOGGED);
		table_close(rel, NoLock);
	}

	ReindexParams params = { 0 };
	ReindexParams *options = &params;
	reindex_relation_compat(NULL, table_oid, REINDEX_REL_PROCESS_TOAST, options);
	rel = table_open(table_oid, AccessExclusiveLock);
	CommandCounterIncrement();
	table_close(rel, NoLock);
}

/* Handle the all rows deletion of a given relation */
static void
RelationDeleteAllRows(Relation rel, Snapshot snap)
{
	TupleTableSlot *slot = table_slot_create(rel, NULL);
	TableScanDesc scan = table_beginscan(rel, snap, 0, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		simple_table_tuple_delete(rel, &(slot->tts_tid), snap);
	}
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
}

/*
 * Delete the relation WITHOUT applying triggers. This will be used when
 * `enable_delete_after_compression = true` instead of truncating the relation.
 * Also don't restart sequences.
 */
static void
delete_relation_rows(Oid table_oid)
{
	Relation rel = table_open(table_oid, RowExclusiveLock);
	Snapshot snap = GetLatestSnapshot();

	/* Delete the rows in the table */
	RelationDeleteAllRows(rel, snap);

	/* Delete the rows in the toast table */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
	{
		Relation toast_rel = table_open(rel->rd_rel->reltoastrelid, RowExclusiveLock);
		RelationDeleteAllRows(toast_rel, snap);
		table_close(toast_rel, NoLock);
	}

	table_close(rel, NoLock);
}

/*
 * Use reltuples as an estimate for the number of rows that will get compressed. This value
 * might be way off the mark in case analyze hasn't happened in quite a while on this input
 * chunk. But that's the best guesstimate to start off with.
 *
 * We will report progress for every 10% of reltuples compressed. If rel or reltuples is not valid
 * or it's just too low then we just assume reporting every 100K tuples for now.
 */
#define RELTUPLES_REPORT_DEFAULT 100000
static int64
calculate_reltuples_to_report(float4 reltuples)
{
	int64 report_reltuples = RELTUPLES_REPORT_DEFAULT;

	if (reltuples > 0)
	{
		report_reltuples = (int64) (0.1 * reltuples);
		/* either analyze has not been done or table doesn't have a lot of rows */
		if (report_reltuples < RELTUPLES_REPORT_DEFAULT)
			report_reltuples = RELTUPLES_REPORT_DEFAULT;
	}

	return report_reltuples;
}

CompressionStats
compress_chunk(Oid in_table, Oid out_table, int insert_options)
{
	int n_keys;
	ListCell *lc;
	ScanDirection indexscan_direction = NoMovementScanDirection;
	Relation matched_index_rel = NULL;
	TupleTableSlot *slot;
	IndexScanDesc index_scan;
	HeapTuple in_table_tp = NULL, index_tp = NULL;
	Form_pg_attribute in_table_attr_tp, index_attr_tp;
	CompressionStats cstat;
	CompressionSettings *settings = ts_compression_settings_get_by_compress_relid(out_table);

	int64 report_reltuples;

	/* We want to prevent other compressors from compressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our compression.
	 * We may as well allow readers to keep reading the uncompressed data while
	 * we are compressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation in_rel = table_open(in_table, ExclusiveLock);
	/* We are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 *
	 * Putting RowExclusiveMode behind a GUC so we can try this out with
	 * rollups during compression.
	 */
	int out_rel_lockmode = ExclusiveLock;
	if (ts_guc_enable_rowlevel_compression_locking)
	{
		out_rel_lockmode = RowExclusiveLock;
	}
	Relation out_rel = relation_open(out_table, out_rel_lockmode);
	BulkWriter writer = bulk_writer_build(out_rel, insert_options);

	/* Sanity check we are dealing with relations */
	Ensure(in_rel->rd_rel->relkind == RELKIND_RELATION, "compress_chunk called on non-relation");
	Ensure(out_rel->rd_rel->relkind == RELKIND_RELATION, "compress_chunk called on non-relation");

	/* Before calling row compressor relation should be segmented and sorted as configured
	 * by compress_segmentby and compress_orderby.
	 * Cost of sorting can be mitigated if we find an existing BTREE index defined for
	 * uncompressed chunk otherwise expensive tuplesort will come into play.
	 *
	 * The following code is trying to find an existing index that
	 * matches the configuration so that we can skip sequential scan and
	 * tuplesort.
	 *
	 */
	if (ts_guc_enable_compression_indexscan)
	{
		List *in_rel_index_oids = RelationGetIndexList(in_rel);
		foreach (lc, in_rel_index_oids)
		{
			Oid index_oid = lfirst_oid(lc);
			Relation index_rel = index_open(index_oid, AccessShareLock);
			IndexInfo *index_info = BuildIndexInfo(index_rel);

			if (index_info->ii_Predicate != 0)
			{
				/*
				 * Can't use partial indexes for compression because they refer
				 * only to a subset of all rows.
				 */
				index_close(index_rel, AccessShareLock);
				continue;
			}

			int previous_direction = NoMovementScanDirection;
			int current_direction = NoMovementScanDirection;

			n_keys =
				ts_array_length(settings->fd.segmentby) + ts_array_length(settings->fd.orderby);

			if (n_keys <= index_info->ii_NumIndexKeyAttrs && index_info->ii_Am == BTREE_AM_OID)
			{
				int i;
				for (i = 0; i < n_keys; i++)
				{
					const char *attname;
					int16 position;
					bool is_orderby_asc = true;
					bool is_null_first = false;

					if (i < ts_array_length(settings->fd.segmentby))
					{
						position = i + 1;
						attname = ts_array_get_element_text(settings->fd.segmentby, position);
					}
					else
					{
						position = i - ts_array_length(settings->fd.segmentby) + 1;
						attname = ts_array_get_element_text(settings->fd.orderby, position);
						is_orderby_asc =
							!ts_array_get_element_bool(settings->fd.orderby_desc, position);
						is_null_first =
							ts_array_get_element_bool(settings->fd.orderby_nullsfirst, position);
					}
					int16 att_num = get_attnum(in_table, attname);

					int16 option = index_rel->rd_indoption[i];
					bool index_orderby_asc = ((option & INDOPTION_DESC) == 0);
					bool index_null_first = ((option & INDOPTION_NULLS_FIRST) != 0);

					if (att_num == 0 || index_info->ii_IndexAttrNumbers[i] != att_num)
					{
						break;
					}

					in_table_tp = SearchSysCacheAttNum(in_table, att_num);
					if (!HeapTupleIsValid(in_table_tp))
						elog(ERROR,
							 "table \"%s\" does not have column \"%s\"",
							 get_rel_name(in_table),
							 attname);

					index_tp = SearchSysCacheAttNum(index_oid, i + 1);
					if (!HeapTupleIsValid(index_tp))
						elog(ERROR,
							 "index \"%s\" does not have column \"%s\"",
							 get_rel_name(index_oid),
							 attname);

					in_table_attr_tp = (Form_pg_attribute) GETSTRUCT(in_table_tp);
					index_attr_tp = (Form_pg_attribute) GETSTRUCT(index_tp);

					if (index_orderby_asc == is_orderby_asc && index_null_first == is_null_first &&
						in_table_attr_tp->attcollation == index_attr_tp->attcollation)
					{
						current_direction = ForwardScanDirection;
					}
					else if (index_orderby_asc != is_orderby_asc &&
							 index_null_first != is_null_first &&
							 in_table_attr_tp->attcollation == index_attr_tp->attcollation)
					{
						current_direction = BackwardScanDirection;
					}
					else
					{
						current_direction = NoMovementScanDirection;
						break;
					}

					ReleaseSysCache(in_table_tp);
					in_table_tp = NULL;
					ReleaseSysCache(index_tp);
					index_tp = NULL;
					if (previous_direction == NoMovementScanDirection)
					{
						previous_direction = current_direction;
					}
					else if (previous_direction != current_direction)
					{
						break;
					}
				}

				if (n_keys == i && (previous_direction == current_direction &&
									current_direction != NoMovementScanDirection))
				{
					matched_index_rel = index_rel;
					indexscan_direction = current_direction;
					break;
				}
				else
				{
					if (HeapTupleIsValid(in_table_tp))
					{
						ReleaseSysCache(in_table_tp);
						in_table_tp = NULL;
					}
					if (HeapTupleIsValid(index_tp))
					{
						ReleaseSysCache(index_tp);
						index_tp = NULL;
					}
					index_close(index_rel, AccessShareLock);
				}
			}
			else
			{
				index_close(index_rel, AccessShareLock);
			}
		}
	}

	RowCompressor row_compressor;

	Assert(settings->fd.compress_relid == RelationGetRelid(out_rel));
	row_compressor_init(&row_compressor,
						settings,
						RelationGetDescr(in_rel),
						RelationGetDescr(out_rel));

	if (matched_index_rel != NULL)
	{
		int64 nrows_processed = 0;

		elog(ts_guc_debug_compression_path_info ? INFO : DEBUG1,
			 "using index \"%s\" to scan rows for converting to columnstore",
			 get_rel_name(matched_index_rel->rd_id));

		index_scan =
			index_beginscan_compat(in_rel, matched_index_rel, GetTransactionSnapshot(), NULL, 0, 0);
		slot = table_slot_create(in_rel, NULL);
		index_rescan(index_scan, NULL, 0, NULL, 0);
		report_reltuples = calculate_reltuples_to_report(in_rel->rd_rel->reltuples);
		while (index_getnext_slot(index_scan, indexscan_direction, slot))
		{
			row_compressor_process_ordered_slot(&row_compressor, slot, &writer);
			if ((++nrows_processed % report_reltuples) == 0)
				elog(DEBUG2,
					 "converted " INT64_FORMAT " rows to columnstore from \"%s\"",
					 nrows_processed,
					 RelationGetRelationName(in_rel));
		}

		if (row_compressor.rows_compressed_into_current_value > 0)
			row_compressor_flush(&row_compressor, &writer, true);

		elog(DEBUG1,
			 "finished converting " INT64_FORMAT " rows to columnstore from \"%s\"",
			 nrows_processed,
			 RelationGetRelationName(in_rel));

		ExecDropSingleTupleTableSlot(slot);
		index_endscan(index_scan);
		index_close(matched_index_rel, AccessShareLock);
	}
	else
	{
		elog(ts_guc_debug_compression_path_info ? INFO : DEBUG1,
			 "using tuplesort to scan rows from \"%s\" for converting to columnstore",
			 RelationGetRelationName(in_rel));

		Tuplesortstate *sorted_rel = compress_chunk_sort_relation(settings, in_rel);
		row_compressor_append_sorted_rows(&row_compressor, sorted_rel, in_rel, &writer);
		tuplesort_end(sorted_rel);
	}

	row_compressor_close(&row_compressor);
	bulk_writer_close(&writer);

	if (ts_guc_enable_delete_after_compression)
	{
		ereport(NOTICE,
				(errcode(ERRCODE_WARNING_DEPRECATED_FEATURE),
				 errmsg("timescaledb.enable_delete_after_compression is deprecated and will be "
						"removed in a future version. Please use "
						"timescaledb.compress_truncate_behaviour instead.")));
		delete_relation_rows(in_table);
		DEBUG_WAITPOINT("compression_done_after_delete_uncompressed");
	}
	else
	{
		int lock_retry = 0;
		switch (ts_guc_compress_truncate_behaviour)
		{
			case COMPRESS_TRUNCATE_ONLY:
				DEBUG_WAITPOINT("compression_done_before_truncate_uncompressed");
				truncate_relation(in_table);
				DEBUG_WAITPOINT("compression_done_after_truncate_uncompressed");
				break;
			case COMPRESS_TRUNCATE_OR_DELETE:
				DEBUG_WAITPOINT("compression_done_before_truncate_or_delete_uncompressed");
				while (true)
				{
					if (ConditionalLockRelation(in_rel, AccessExclusiveLock))
					{
						truncate_relation(in_table);
						break;
					}

					/*
					 * Check for interrupts while trying to (re-)acquire the exclusive
					 * lock.
					 */
					CHECK_FOR_INTERRUPTS();

					if (++lock_retry >
						(COMPRESS_TRUNCATE_LOCK_TIMEOUT / COMPRESS_TRUNCATE_LOCK_WAIT_INTERVAL))
					{
						/*
						 * We failed to establish the lock in the specified number of
						 * retries. This means we give up truncating and fallback to delete
						 */
						delete_relation_rows(in_table);
						break;
					}

					(void) WaitLatch(MyLatch,
									 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
									 COMPRESS_TRUNCATE_LOCK_WAIT_INTERVAL,
									 WAIT_EVENT_VACUUM_TRUNCATE);
					ResetLatch(MyLatch);
				}
				DEBUG_WAITPOINT("compression_done_after_truncate_or_delete_uncompressed");
				break;
			case COMPRESS_TRUNCATE_DISABLED:
				delete_relation_rows(in_table);
				DEBUG_WAITPOINT("compression_done_after_delete_uncompressed");
				break;
		}
	}

	table_close(out_rel, NoLock);
	table_close(in_rel, NoLock);
	cstat.rowcnt_pre_compression = row_compressor.rowcnt_pre_compression;
	cstat.rowcnt_post_compression = row_compressor.num_compressed_rows;

	if ((insert_options & HEAP_INSERT_FROZEN) == HEAP_INSERT_FROZEN)
		cstat.rowcnt_frozen = row_compressor.num_compressed_rows;
	else
		cstat.rowcnt_frozen = 0;

	return cstat;
}

Tuplesortstate *
compression_create_tuplesort_state(CompressionSettings *settings, Relation rel)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	int num_segmentby = ts_array_length(settings->fd.segmentby);
	int num_orderby = ts_array_length(settings->fd.orderby);
	int n_keys = num_segmentby + num_orderby;
	AttrNumber *sort_keys = palloc(sizeof(*sort_keys) * n_keys);
	Oid *sort_operators = palloc(sizeof(*sort_operators) * n_keys);
	Oid *sort_collations = palloc(sizeof(*sort_collations) * n_keys);
	bool *nulls_first = palloc(sizeof(*nulls_first) * n_keys);
	int n;

	for (n = 0; n < n_keys; n++)
	{
		const char *attname;
		int position;
		if (n < num_segmentby)
		{
			position = n + 1;
			attname = ts_array_get_element_text(settings->fd.segmentby, position);
		}
		else
		{
			position = n - num_segmentby + 1;
			attname = ts_array_get_element_text(settings->fd.orderby, position);
		}
		compress_chunk_populate_sort_info_for_column(settings,
													 RelationGetRelid(rel),
													 attname,
													 &sort_keys[n],
													 &sort_operators[n],
													 &sort_collations[n],
													 &nulls_first[n]);
	}

	/* Make a copy of the tuple descriptor so that it is allocated on the same
	 * memory context as the tuple sort instead of pointing into the relcache
	 * entry that could be blown away. */
	return tuplesort_begin_heap(CreateTupleDescCopy(tupdesc),
								n_keys,
								sort_keys,
								sort_operators,
								sort_collations,
								nulls_first,
								maintenance_work_mem,
								NULL,
								false /*=randomAccess*/);
}

static Tuplesortstate *
compress_chunk_sort_relation(CompressionSettings *settings, Relation in_rel)
{
	Tuplesortstate *tuplesortstate;
	TableScanDesc scan;
	TupleTableSlot *slot;
	tuplesortstate = compression_create_tuplesort_state(settings, in_rel);
	scan = table_beginscan(in_rel, GetLatestSnapshot(), 0, NULL);
	slot = table_slot_create(in_rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		if (!TTS_EMPTY(slot))
		{
			/*    This may not be the most efficient way to do things.
			 *     Since we use begin_heap() the tuplestore expects tupleslots,
			 *      so ISTM that the options are this or maybe putdatum().
			 */
			tuplesort_puttupleslot(tuplesortstate, slot);
		}
	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	tuplesort_performsort(tuplesortstate);

	return tuplesortstate;
}

void
compress_chunk_populate_sort_info_for_column(const CompressionSettings *settings, Oid table,
											 const char *attname, AttrNumber *att_nums,
											 Oid *sort_operator, Oid *collation, bool *nulls_first)
{
	HeapTuple tp;
	Form_pg_attribute att_tup;
	TypeCacheEntry *tentry;

	tp = SearchSysCacheAttName(table, attname);
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "table \"%s\" does not have column \"%s\"", get_rel_name(table), attname);

	att_tup = (Form_pg_attribute) GETSTRUCT(tp);
	/* Other validation checks beyond just existence of a valid comparison operator could be useful
	 */

	*att_nums = att_tup->attnum;
	*collation = att_tup->attcollation;

	tentry = lookup_type_cache(att_tup->atttypid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	if (ts_array_is_member(settings->fd.segmentby, attname))
	{
		*nulls_first = false;
		*sort_operator = tentry->lt_opr;
	}
	else
	{
		Assert(ts_array_is_member(settings->fd.orderby, attname));
		int position = ts_array_position(settings->fd.orderby, attname);
		*nulls_first = ts_array_get_element_bool(settings->fd.orderby_nullsfirst, position);

		if (ts_array_get_element_bool(settings->fd.orderby_desc, position))
			*sort_operator = tentry->gt_opr;
		else
			*sort_operator = tentry->lt_opr;
	}

	if (!OidIsValid(*sort_operator))
		elog(ERROR,
			 "no valid sort operator for column \"%s\" of type \"%s\"",
			 attname,
			 format_type_be(att_tup->atttypid));

	ReleaseSysCache(tp);
}

/*
 * Find segment by index on compressed chunk needed when doing index scans
 * over compressed data
 */
Oid
get_compressed_chunk_index(ResultRelInfo *resultRelInfo, const CompressionSettings *settings)
{
	int num_segmentby_columns = ts_array_length(settings->fd.segmentby);
	int num_orderby_columns = ts_array_length(settings->fd.orderby);

	for (int i = 0; i < resultRelInfo->ri_NumIndices; i++)
	{
		bool matches = true;
		Relation index_relation = resultRelInfo->ri_IndexRelationDescs[i];
		IndexInfo *index_info = resultRelInfo->ri_IndexRelationInfo[i];

		/* The index must include all segment by columns and at least two metadata columns.
		 * Default index we build includes all segmentby columns and metadata columns (min and max,
		 * in that order) for all orderby columns.*/
		if (index_info->ii_NumIndexKeyAttrs != num_segmentby_columns + (num_orderby_columns * 2))
			continue;

		for (int j = 0; j < num_segmentby_columns - 1; j++)
		{
			AttrNumber attno = index_relation->rd_index->indkey.values[j];
			const char *attname = get_attname(index_relation->rd_index->indrelid, attno, false);

			if (!ts_array_is_member(settings->fd.segmentby, attname))
			{
				matches = false;
				break;
			}
		}

		if (!matches)
			continue;

		return RelationGetRelid(index_relation);
	}

	return InvalidOid;
}

static void
build_column_map(const CompressionSettings *settings, const TupleDesc in_desc,
				 const TupleDesc out_desc, PerColumn **pcolumns, int16 **pmap)
{
	Oid compressed_data_type_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	PerColumn *columns = palloc0(sizeof(PerColumn) * in_desc->natts);
	int16 *map = palloc0(sizeof(int16) * in_desc->natts);

	for (int i = 0; i < in_desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(in_desc, i);

		if (attr->attisdropped)
			continue;

		PerColumn *column = &columns[AttrNumberGetAttrOffset(attr->attnum)];
		AttrNumber compressed_colnum =
			get_attnum(settings->fd.compress_relid, NameStr(attr->attname));
		Form_pg_attribute compressed_column_attr =
			TupleDescAttr(out_desc, AttrNumberGetAttrOffset(compressed_colnum));
		map[AttrNumberGetAttrOffset(attr->attnum)] = AttrNumberGetAttrOffset(compressed_colnum);

		bool is_segmentby = ts_array_is_member(settings->fd.segmentby, NameStr(attr->attname));
		bool is_orderby = ts_array_is_member(settings->fd.orderby, NameStr(attr->attname));

		if (!is_segmentby)
		{
			if (compressed_column_attr->atttypid != compressed_data_type_oid)
				elog(ERROR,
					 "expected column '%s' to be a compressed data type",
					 NameStr(attr->attname));

			AttrNumber segment_min_attr_number =
				compressed_column_metadata_attno(settings,
												 settings->fd.relid,
												 attr->attnum,
												 settings->fd.compress_relid,
												 "min");
			AttrNumber segment_max_attr_number =
				compressed_column_metadata_attno(settings,
												 settings->fd.relid,
												 attr->attnum,
												 settings->fd.compress_relid,
												 "max");
			int16 segment_min_attr_offset = segment_min_attr_number - 1;
			int16 segment_max_attr_offset = segment_max_attr_number - 1;

			BatchMetadataBuilder *batch_minmax_builder = NULL;
			if (segment_min_attr_number != InvalidAttrNumber ||
				segment_max_attr_number != InvalidAttrNumber)
			{
				Ensure(segment_min_attr_number != InvalidAttrNumber,
					   "could not find the min metadata column");
				Ensure(segment_max_attr_number != InvalidAttrNumber,
					   "could not find the min metadata column");
				batch_minmax_builder =
					batch_metadata_builder_minmax_create(attr->atttypid,
														 attr->attcollation,
														 segment_min_attr_offset,
														 segment_max_attr_offset);
			}

			Ensure(!is_orderby || batch_minmax_builder != NULL,
				   "orderby columns must have minmax metadata");

			const AttrNumber bloom_attr_number =
				compressed_column_metadata_attno(settings,
												 settings->fd.relid,
												 attr->attnum,
												 settings->fd.compress_relid,
												 "bloom1");
			if (AttributeNumberIsValid(bloom_attr_number))
			{
				const int bloom_attr_offset = AttrNumberGetAttrOffset(bloom_attr_number);
				batch_minmax_builder =
					batch_metadata_builder_bloom1_create(attr->atttypid, bloom_attr_offset);
			}

			*column = (PerColumn){
				.compressor = compressor_for_type(attr->atttypid),
				.metadata_builder = batch_minmax_builder,
				.segmentby_column_index = -1,
			};
		}
		else
		{
			if (attr->atttypid != compressed_column_attr->atttypid)
				elog(ERROR,
					 "expected segment by column \"%s\" to be same type as uncompressed column",
					 NameStr(attr->attname));
			int16 index = ts_array_position(settings->fd.segmentby, NameStr(attr->attname));
			*column = (PerColumn){
				.segment_info = segment_info_new(attr),
				.segmentby_column_index = index,
			};
		}
	}
	*pcolumns = columns;
	*pmap = map;
}

/* Check if we contain any compressors which need allocation limit checking */
static bool
check_for_limited_size_compressors(PerColumn *pcolumns, int16 natts)
{
	for (int i = 0; i < natts; i++)
	{
		if (pcolumns[i].compressor && pcolumns[i].compressor->is_full)
			return true;
	}

	return false;
}

void
tsl_compressor_add_slot(RowCompressor *compressor, BulkWriter *bulk_writer, TupleTableSlot *slot)
{
	if (compressor->sort_state)
	{
		tuplesort_puttupleslot(compressor->sort_state, slot);
		compressor->tuples_to_sort++;
	}
	else
	{
		row_compressor_process_ordered_slot(compressor, slot, bulk_writer);
	}
}

void
tsl_compressor_flush(RowCompressor *compressor, BulkWriter *bulk_writer)
{
	if (compressor->sort_state)
	{
		if (compressor->tuples_to_sort)
		{
			tuplesort_performsort(compressor->sort_state);

			TupleTableSlot *slot = MakeTupleTableSlot(compressor->in_desc, &TTSOpsMinimalTuple);

			while (tuplesort_gettupleslot(compressor->sort_state,
										  true /*=forward*/,
										  false /*=copy*/,
										  slot,
										  NULL /*=abbrev*/))
				row_compressor_process_ordered_slot(compressor, slot, bulk_writer);

			if (compressor->rows_compressed_into_current_value > 0)
				row_compressor_flush(compressor, bulk_writer, true);

			ExecDropSingleTupleTableSlot(slot);
			tuplesort_reset(compressor->sort_state);
			compressor->tuples_to_sort = 0;
		}
	}
	else
	{
		if (compressor->rows_compressed_into_current_value > 0)
			row_compressor_flush(compressor, bulk_writer, false);
	}
}

void
tsl_compressor_free(RowCompressor *compressor, BulkWriter *bulk_writer)
{
	if (compressor->sort_state)
		tuplesort_end(compressor->sort_state);
	tsl_compressor_flush(compressor, bulk_writer);
	row_compressor_close(compressor);
	bulk_writer_close(bulk_writer);
	table_close(bulk_writer->out_rel, NoLock);
}

/*
 * Initialize a RowCompressor for compressing tuples
 *
 * When `sort` is true, the compressor will buffer all the tuples in a
 * Tuplesortstate and sort them before flushing to the output relation.
 */
RowCompressor *
tsl_compressor_init(Relation in_rel, BulkWriter **bulk_writer, bool sort)
{
	RowCompressor *compressor = palloc0(sizeof(RowCompressor));
	CompressionSettings *settings = ts_compression_settings_get(in_rel->rd_id);
	Relation out_rel = table_open(settings->fd.compress_relid, RowExclusiveLock);
	*bulk_writer = bulk_writer_alloc(out_rel, 0);
	row_compressor_init(compressor, settings, RelationGetDescr(in_rel), RelationGetDescr(out_rel));

	if (sort)
		compressor->sort_state = compression_create_tuplesort_state(settings, in_rel);

	return compressor;
}

/********************
 ** row_compressor **
 ********************/
void
row_compressor_init(RowCompressor *row_compressor, const CompressionSettings *settings,
					const TupleDesc noncompressed_tupdesc, const TupleDesc compressed_tupdesc)
{
	Name count_metadata_name = DatumGetName(
		DirectFunctionCall1(namein, CStringGetDatum(COMPRESSION_COLUMN_METADATA_COUNT_NAME)));
	AttrNumber count_metadata_column_num =
		get_attnum(settings->fd.compress_relid, NameStr(*count_metadata_name));

	if (count_metadata_column_num == InvalidAttrNumber)
		elog(ERROR,
			 "missing metadata column '%s' in columnstore table",
			 COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	*row_compressor = (RowCompressor){
		.per_row_ctx = AllocSetContextCreate(CurrentMemoryContext,
											 "compress chunk per-row",
											 ALLOCSET_DEFAULT_SIZES),
		.in_desc = CreateTupleDescCopyConstr(noncompressed_tupdesc),
		.out_desc = CreateTupleDescCopyConstr(compressed_tupdesc),
		.n_input_columns = noncompressed_tupdesc->natts,
		.count_metadata_column_offset = AttrNumberGetAttrOffset(count_metadata_column_num),
		.compressed_values = palloc(sizeof(Datum) * compressed_tupdesc->natts),
		.compressed_is_null = palloc(sizeof(bool) * compressed_tupdesc->natts),
		.rows_compressed_into_current_value = 0,
		.rowcnt_pre_compression = 0,
		.num_compressed_rows = 0,
		.first_iteration = true,
		.sort_state = NULL,
	};

	memset(row_compressor->compressed_is_null, 1, sizeof(bool) * compressed_tupdesc->natts);

	build_column_map(settings,
					 noncompressed_tupdesc,
					 compressed_tupdesc,
					 &row_compressor->per_column,
					 &row_compressor->uncompressed_col_to_compressed_col);

	/* If we have dictionary or array compressors, we have to check compressor size so we don't end
	 * up going over allocation limit */
	row_compressor->needs_fullness_check =
		check_for_limited_size_compressors(row_compressor->per_column,
										   row_compressor->n_input_columns);
}

void
row_compressor_append_sorted_rows(RowCompressor *row_compressor, Tuplesortstate *sorted_rel,
								  Relation in_rel, BulkWriter *writer)
{
	TupleTableSlot *slot = MakeTupleTableSlot(row_compressor->in_desc, &TTSOpsMinimalTuple);
	int64 nrows_processed = 0;
	int64 report_reltuples;

	report_reltuples = calculate_reltuples_to_report(in_rel->rd_rel->reltuples);

	while (tuplesort_gettupleslot(sorted_rel,
								  true /*=forward*/,
								  false /*=copy*/,
								  slot,
								  NULL /*=abbrev*/))
	{
		row_compressor_process_ordered_slot(row_compressor, slot, writer);
		if ((++nrows_processed % report_reltuples) == 0)
			elog(DEBUG2,
				 "compressed " INT64_FORMAT " rows from \"%s\"",
				 nrows_processed,
				 RelationGetRelationName(in_rel));
	}

	if (row_compressor->rows_compressed_into_current_value > 0)
		row_compressor_flush(row_compressor, writer, true);
	elog(DEBUG1,
		 "finished compressing " INT64_FORMAT " rows from \"%s\"",
		 nrows_processed,
		 RelationGetRelationName(in_rel));

	ExecDropSingleTupleTableSlot(slot);
}

static bool
row_compressor_is_full(RowCompressor *row_compressor, TupleTableSlot *row)
{
	if (row_compressor->rows_compressed_into_current_value >=
		(uint32) ts_guc_compression_batch_size_limit)
		return true;

	if (!ts_guc_compression_enable_compressor_batch_limit)
		return false;

	if (!row_compressor->needs_fullness_check)
		return false;

	/* Check with every column compressor if they can add the next value to current batch */
	int col;
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		Compressor *compressor = row_compressor->per_column[col].compressor;
		bool is_null;
		Datum val;

		/* No compressor or the compressor has no check, just skip */
		if (compressor == NULL || compressor->is_full == NULL)
			continue;

		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		if (!is_null)
		{
			if (compressor->is_full(compressor, val))
				return true;
		}
	}

	return false;
}

void
row_compressor_append_ordered_slot(RowCompressor *row_compressor, TupleTableSlot *slot)
{
	MemoryContext old_ctx;
	slot_getallattrs(slot);
	old_ctx = MemoryContextSwitchTo(row_compressor->per_row_ctx);
	if (row_compressor->first_iteration)
	{
		row_compressor_update_group(row_compressor, slot);
		row_compressor->first_iteration = false;
	}
	bool changed_groups = row_compressor_new_row_is_in_new_group(row_compressor, slot);
	bool compressed_row_is_full = row_compressor_is_full(row_compressor, slot);

	Ensure(!changed_groups, "row is in different group");
	Ensure(!compressed_row_is_full, "batch is full");
	row_compressor_append_row(row_compressor, slot);
	MemoryContextSwitchTo(old_ctx);
	ExecClearTuple(slot);
}

static void
row_compressor_process_ordered_slot(RowCompressor *row_compressor, TupleTableSlot *slot,
									BulkWriter *writer)
{
	MemoryContext old_ctx;
	slot_getallattrs(slot);
	old_ctx = MemoryContextSwitchTo(row_compressor->per_row_ctx);
	if (row_compressor->first_iteration)
	{
		row_compressor_update_group(row_compressor, slot);
		row_compressor->first_iteration = false;
	}
	bool changed_groups = row_compressor_new_row_is_in_new_group(row_compressor, slot);
	bool compressed_row_is_full = row_compressor_is_full(row_compressor, slot);
	if (compressed_row_is_full || changed_groups)
	{
		if (row_compressor->rows_compressed_into_current_value > 0)
			row_compressor_flush(row_compressor, writer, changed_groups);
		if (changed_groups)
			row_compressor_update_group(row_compressor, slot);
	}

	row_compressor_append_row(row_compressor, slot);
	MemoryContextSwitchTo(old_ctx);
	ExecClearTuple(slot);
}

static void
row_compressor_update_group(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	/* save original memory context */
	const MemoryContext oldcontext = CurrentMemoryContext;

	Assert(row_compressor->rows_compressed_into_current_value == 0);
	Assert(row_compressor->n_input_columns <= row->tts_nvalid);

	MemoryContextSwitchTo(row_compressor->per_row_ctx->parent);
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Datum val;
		bool is_null;

		if (column->segment_info == NULL)
			continue;

		Assert(column->compressor == NULL);

		/* Performance Improvement: We should just use array access here; everything is guaranteed
		   to be fetched */
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		segment_info_update(column->segment_info, val, is_null);
	}
	/* switch to original memory context */
	MemoryContextSwitchTo(oldcontext);
}

static bool
row_compressor_new_row_is_in_new_group(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Datum datum = CharGetDatum(0);
		bool is_null;

		if (column->segment_info == NULL)
			continue;

		Assert(column->compressor == NULL);

		datum = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);

		if (!segment_info_datum_is_in_group(column->segment_info, datum, is_null))
			return true;
	}

	return false;
}

void
row_compressor_append_row(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		Compressor *compressor = row_compressor->per_column[col].compressor;
		bool is_null;
		Datum val;

		/* if there is no compressor, this must be a segmenter, so just skip */
		if (compressor == NULL)
			continue;

		/* Performance Improvement: Since we call getallatts at the beginning, slot_getattr is
		 * useless overhead here, and we should just access the array directly.
		 */
		BatchMetadataBuilder *builder = row_compressor->per_column[col].metadata_builder;
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		if (is_null)
		{
			compressor->append_null(compressor);
			if (builder != NULL)
			{
				builder->update_null(builder);
			}
		}
		else
		{
			compressor->append_val(compressor, val);
			if (builder != NULL)
			{
				builder->update_val(builder, val);
			}
		}
	}

	row_compressor->rows_compressed_into_current_value += 1;
}

HeapTuple
row_compressor_build_tuple(RowCompressor *row_compressor)
{
	MemoryContext old_cxt = MemoryContextSwitchTo(row_compressor->per_row_ctx);

	for (int col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Compressor *compressor;
		int16 compressed_col;
		if (column->compressor == NULL && column->segment_info == NULL)
			continue;

		compressor = column->compressor;
		compressed_col = row_compressor->uncompressed_col_to_compressed_col[col];

		Assert(compressed_col >= 0);

		if (compressor != NULL)
		{
			void *compressed_data;
			Assert(column->segment_info == NULL);

			compressed_data = compressor->finish(compressor);
			if (compressed_data == NULL)
			{
				if (ts_guc_enable_null_compression &&
					row_compressor->rows_compressed_into_current_value > 0)
					compressed_data = null_compressor_get_dummy_block();
			}
			row_compressor->compressed_is_null[compressed_col] = compressed_data == NULL;

			if (compressed_data != NULL)
				row_compressor->compressed_values[compressed_col] =
					PointerGetDatum(compressed_data);

			if (column->metadata_builder != NULL)
			{
				column->metadata_builder->insert_to_compressed_row(column->metadata_builder,
																   row_compressor);
			}
		}
		else if (column->segment_info != NULL)
		{
			row_compressor->compressed_values[compressed_col] = column->segment_info->val;
			row_compressor->compressed_is_null[compressed_col] = column->segment_info->is_null;
		}
	}

	row_compressor->compressed_values[row_compressor->count_metadata_column_offset] =
		Int32GetDatum(row_compressor->rows_compressed_into_current_value);
	row_compressor->compressed_is_null[row_compressor->count_metadata_column_offset] = false;

	MemoryContextSwitchTo(old_cxt);

	/* Build the tuple on the callers memory context */
	return heap_form_tuple(row_compressor->out_desc,
						   row_compressor->compressed_values,
						   row_compressor->compressed_is_null);
}

void
row_compressor_clear_batch(RowCompressor *row_compressor, bool changed_groups)
{
	MemoryContext old_cxt = MemoryContextSwitchTo(row_compressor->per_row_ctx);

	/* free the compressed values now that we're done with them (the old compressor is freed in
	 * finish()) */
	for (int col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		int16 compressed_col;
		if (column->compressor == NULL && column->segment_info == NULL)
			continue;

		compressed_col = row_compressor->uncompressed_col_to_compressed_col[col];
		Assert(compressed_col >= 0);
		if (row_compressor->compressed_is_null[compressed_col])
			continue;

		/* don't free the segment-bys if we've overflowed the row, we still need them */
		if (column->segment_info != NULL && !changed_groups)
			continue;

		if (column->compressor != NULL || !column->segment_info->typ_by_val)
			pfree(DatumGetPointer(row_compressor->compressed_values[compressed_col]));

		if (column->metadata_builder != NULL)
		{
			column->metadata_builder->reset(column->metadata_builder, row_compressor);
		}

		row_compressor->compressed_values[compressed_col] = 0;
		row_compressor->compressed_is_null[compressed_col] = true;
	}

	row_compressor->rowcnt_pre_compression += row_compressor->rows_compressed_into_current_value;
	row_compressor->num_compressed_rows++;
	row_compressor->rows_compressed_into_current_value = 0;

	MemoryContextSwitchTo(old_cxt);
	MemoryContextReset(row_compressor->per_row_ctx);
}

static void
row_compressor_flush(RowCompressor *row_compressor, BulkWriter *writer, bool changed_groups)
{
	HeapTuple compressed_tuple = row_compressor_build_tuple(row_compressor);
	MemoryContext old_cxt = MemoryContextSwitchTo(row_compressor->per_row_ctx);

	Assert(writer->bistate != NULL);
	heap_insert(writer->out_rel,
				compressed_tuple,
				writer->mycid,
				writer->insert_options /*=options*/,
				writer->bistate);
	if (writer->indexstate->ri_NumIndices > 0)
	{
		ts_catalog_index_insert(writer->indexstate, compressed_tuple);
	}

	heap_freetuple(compressed_tuple);

	if (row_compressor->on_flush)
		row_compressor->on_flush(row_compressor,
								 row_compressor->rows_compressed_into_current_value);

	MemoryContextSwitchTo(old_cxt);
	row_compressor_clear_batch(row_compressor, changed_groups);
}

void
row_compressor_reset(RowCompressor *row_compressor)
{
	row_compressor->first_iteration = true;
}

void
row_compressor_close(RowCompressor *row_compressor)
{
	pfree(row_compressor->compressed_is_null);
	pfree(row_compressor->compressed_values);
	pfree(row_compressor->per_column);
	pfree(row_compressor->uncompressed_col_to_compressed_col);
	FreeTupleDesc(row_compressor->out_desc);
}

/******************
 ** segment_info **
 ******************/

SegmentInfo *
segment_info_new(Form_pg_attribute column_attr)
{
	TypeCacheEntry *tce = lookup_type_cache(column_attr->atttypid, TYPECACHE_EQ_OPR_FINFO);

	if (!OidIsValid(tce->eq_opr_finfo.fn_oid))
		elog(ERROR, "no equality function for column \"%s\"", NameStr(column_attr->attname));

	SegmentInfo *segment_info = palloc(sizeof(*segment_info));

	*segment_info = (SegmentInfo){
		.typlen = column_attr->attlen,
		.typ_by_val = column_attr->attbyval,
	};

	fmgr_info_cxt(tce->eq_opr_finfo.fn_oid, &segment_info->eq_fn, CurrentMemoryContext);

	segment_info->eq_fcinfo = HEAP_FCINFO(2);
	segment_info->collation = column_attr->attcollation;
	InitFunctionCallInfoData(*segment_info->eq_fcinfo,
							 &segment_info->eq_fn /*=Flinfo*/,
							 2 /*=Nargs*/,
							 column_attr->attcollation /*=Collation*/,
							 NULL, /*=Context*/
							 NULL  /*=ResultInfo*/
	);

	return segment_info;
}

void
segment_info_update(SegmentInfo *segment_info, Datum val, bool is_null)
{
	segment_info->is_null = is_null;
	if (is_null)
		segment_info->val = 0;
	else
		segment_info->val = datumCopy(val, segment_info->typ_by_val, segment_info->typlen);
}

bool
segment_info_datum_is_in_group(SegmentInfo *segment_info, Datum datum, bool is_null)
{
	Datum data_is_eq;
	FunctionCallInfo eq_fcinfo;
	/* if one of the datums is null and the other isn't, we must be in a new group */
	if (segment_info->is_null != is_null)
		return false;

	/* they're both null */
	if (segment_info->is_null)
		return true;

	/* neither is null, call the eq function */
	eq_fcinfo = segment_info->eq_fcinfo;

	FC_SET_ARG(eq_fcinfo, 0, segment_info->val);
	FC_SET_ARG(eq_fcinfo, 1, datum);

	data_is_eq = FunctionCallInvoke(eq_fcinfo);

	if (eq_fcinfo->isnull)
		return false;

	return DatumGetBool(data_is_eq);
}

/*
 * Build a map from compressed attribute numbers to non-compressed attribute
 * numbers.
 */
static AttrMap *
build_decompress_attrmap(const TupleDesc noncompressed_desc, const TupleDesc compressed_desc,
						 AttrNumber *count_meta_attnum)
{
	AttrMap *attrMap;
	int outnatts;
	int innatts;
	int i;
	int nextindesc = -1;

	outnatts = compressed_desc->natts;
	innatts = noncompressed_desc->natts;

	attrMap = make_attrmap(outnatts);
	for (i = 0; i < outnatts; i++)
	{
		Form_pg_attribute outatt = TupleDescAttr(compressed_desc, i);
		char *attname;
		int j;

		if (outatt->attisdropped)
			continue;

		attname = NameStr(outatt->attname);

		if (strcmp(attname, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
		{
			*count_meta_attnum = outatt->attnum;
			/* No point in mapping this attribute since meta columns are not
			 * present in the non-compressed relation and will not be found
			 * below anyway. */
			continue;
		}
		else if (strncmp(attname,
						 COMPRESSION_COLUMN_METADATA_PREFIX,
						 strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
		{
			/* We can skip other meta attributes as well */
			continue;
		}

		for (j = 0; j < innatts; j++)
		{
			Form_pg_attribute inatt;

			nextindesc++;
			if (nextindesc >= innatts)
				nextindesc = 0;

			inatt = TupleDescAttr(noncompressed_desc, nextindesc);
			if (inatt->attisdropped)
				continue;
			if (strcmp(attname, NameStr(inatt->attname)) == 0)
			{
				attrMap->attnums[i] = inatt->attnum;
				break;
			}
		}
	}
	return attrMap;
}

BulkWriter
bulk_writer_build(Relation out_rel, int insert_options)
{
	BulkWriter writer = {
		.out_rel = out_rel,
		.indexstate = CatalogOpenIndexes(out_rel),
		.mycid = GetCurrentCommandId(true),
		.bistate = GetBulkInsertState(),
		.estate = CreateExecutorState(),
		.insert_options = insert_options,
	};

	return writer;
}

BulkWriter *
bulk_writer_alloc(Relation out_rel, int insert_options)
{
	BulkWriter *writer = palloc(sizeof(BulkWriter));
	writer->out_rel = out_rel;
	writer->indexstate = CatalogOpenIndexes(out_rel);
	writer->mycid = GetCurrentCommandId(true);
	writer->bistate = GetBulkInsertState();
	writer->estate = CreateExecutorState();
	writer->insert_options = insert_options;

	return writer;
}

void
bulk_writer_close(BulkWriter *writer)
{
	FreeBulkInsertState(writer->bistate);
	if (writer->indexstate)
		CatalogCloseIndexes(writer->indexstate);
	FreeExecutorState(writer->estate);
}

/**********************
 ** decompress_chunk **
 **********************/

RowDecompressor
build_decompressor(const TupleDesc in_desc, const TupleDesc out_desc)
{
	AttrNumber count_meta_attnum = InvalidAttrNumber;
	AttrMap *attrmap = build_decompress_attrmap(out_desc, in_desc, &count_meta_attnum);

	Assert(AttributeNumberIsValid(count_meta_attnum));

	RowDecompressor decompressor = {
		.count_compressed_attindex = AttrNumberGetAttrOffset(count_meta_attnum),
		.in_desc = CreateTupleDescCopyConstr(in_desc),
		.out_desc = CreateTupleDescCopyConstr(out_desc),
		.compressed_datums = palloc(sizeof(Datum) * in_desc->natts),
		.compressed_is_nulls = palloc(sizeof(bool) * in_desc->natts),

		/* cache memory used to store the decompressed datums/is_null for form_tuple */
		.decompressed_datums = palloc(sizeof(Datum) * out_desc->natts),
		.decompressed_is_nulls = palloc(sizeof(bool) * out_desc->natts),
		.per_compressed_row_ctx = AllocSetContextCreate(CurrentMemoryContext,
														"decompress chunk per-compressed row",
														ALLOCSET_DEFAULT_SIZES),
		.decompressed_slots =
			(TupleTableSlot **) palloc0(sizeof(void *) * TARGET_COMPRESSED_BATCH_SIZE),
		.attrmap = attrmap,
	};

	create_per_compressed_column(&decompressor);

	/*
	 * We need to make sure decompressed_is_nulls is in a defined state. While this
	 * will get written for normal columns it will not get written for dropped columns
	 * since dropped columns don't exist in the compressed chunk so we initialize
	 * with true here.
	 */
	memset(decompressor.decompressed_is_nulls, true, out_desc->natts);

	detoaster_init(&decompressor.detoaster, CurrentMemoryContext);

	return decompressor;
}

void
row_decompressor_reset(RowDecompressor *decompressor)
{
	MemoryContextReset(decompressor->per_compressed_row_ctx);
	decompressor->unprocessed_tuples = 0;
	decompressor->batches_decompressed = 0;
	decompressor->tuples_decompressed = 0;
}

void
row_decompressor_close(RowDecompressor *decompressor)
{
	MemoryContextDelete(decompressor->per_compressed_row_ctx);
	detoaster_close(&decompressor->detoaster);
	free_attrmap(decompressor->attrmap);
	FreeTupleDesc(decompressor->in_desc);
	FreeTupleDesc(decompressor->out_desc);
	pfree(decompressor->compressed_datums);
	pfree(decompressor->compressed_is_nulls);
	pfree(decompressor->decompressed_datums);
	pfree(decompressor->decompressed_is_nulls);
	pfree(decompressor->decompressed_slots);
	pfree(decompressor->per_compressed_cols);
}

void
decompress_chunk(Oid in_table, Oid out_table)
{
	/*
	 * Locks are taken in the order uncompressed table then compressed table
	 * for consistency with compress_chunk.
	 * We are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 * We want to prevent other decompressors from decompressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our decompression.
	 * We may as well allow readers to keep reading the compressed data while
	 * we are decompressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation out_rel = table_open(out_table, ExclusiveLock);
	Relation in_rel = table_open(in_table, ExclusiveLock);
	int64 nrows_processed = 0;

	BulkWriter writer = bulk_writer_build(out_rel, 0);
	RowDecompressor decompressor =
		build_decompressor(RelationGetDescr(in_rel), RelationGetDescr(out_rel));
	TupleTableSlot *slot = table_slot_create(in_rel, NULL);
	TableScanDesc scan = table_beginscan(in_rel, GetLatestSnapshot(), 0, (ScanKey) NULL);
	int64 report_reltuples = calculate_reltuples_to_report(in_rel->rd_rel->reltuples);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool should_free;
		HeapTuple tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);

		heap_deform_tuple(tuple,
						  decompressor.in_desc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		if (should_free)
			heap_freetuple(tuple);

		row_decompressor_decompress_row_to_table(&decompressor, &writer);

		if ((++nrows_processed % report_reltuples) == 0)
			elog(DEBUG2,
				 "decompressed " INT64_FORMAT " rows from \"%s\"",
				 nrows_processed,
				 RelationGetRelationName(in_rel));
	}

	elog(DEBUG1,
		 "finished decompressing " INT64_FORMAT " rows from \"%s\"",
		 nrows_processed,
		 RelationGetRelationName(in_rel));
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
	row_decompressor_close(&decompressor);
	bulk_writer_close(&writer);

	table_close(out_rel, NoLock);
	table_close(in_rel, NoLock);
}

static void
create_per_compressed_column(RowDecompressor *decompressor)
{
	Oid compressed_data_type_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;
	Assert(OidIsValid(compressed_data_type_oid));

	decompressor->per_compressed_cols =
		palloc(sizeof(*decompressor->per_compressed_cols) * decompressor->in_desc->natts);

	Assert(OidIsValid(compressed_data_type_oid));

	for (int col = 0; col < decompressor->in_desc->natts; col++)
	{
		Oid decompressed_type;
		bool is_compressed;
		int16 decompressed_column_offset;
		PerCompressedColumn *per_compressed_col = &decompressor->per_compressed_cols[col];
		Form_pg_attribute compressed_attr = TupleDescAttr(decompressor->in_desc, col);
		char *col_name = NameStr(compressed_attr->attname);

		/* find the mapping from compressed column to uncompressed column, setting
		 * the index of columns that don't have an uncompressed version
		 * (such as metadata) to -1
		 * Assumption: column names are the same on compressed and
		 *       uncompressed chunk.
		 */
		AttrNumber decompressed_colnum = decompressor->attrmap->attnums[col];

		if (!AttributeNumberIsValid(decompressed_colnum))
		{
			*per_compressed_col = (PerCompressedColumn){
				.decompressed_column_offset = -1,
			};
			continue;
		}

		decompressed_column_offset = AttrNumberGetAttrOffset(decompressed_colnum);

		decompressed_type =
			TupleDescAttr(decompressor->out_desc, decompressed_column_offset)->atttypid;

		/* determine if the data is compressed or not */
		is_compressed = compressed_attr->atttypid == compressed_data_type_oid;
		if (!is_compressed && compressed_attr->atttypid != decompressed_type)
			elog(ERROR,
				 "compressed table type '%s' does not match decompressed table type '%s' for "
				 "segment-by column \"%s\"",
				 format_type_be(compressed_attr->atttypid),
				 format_type_be(decompressed_type),
				 col_name);

		*per_compressed_col = (PerCompressedColumn){
			.decompressed_column_offset = decompressed_column_offset,
			.is_compressed = is_compressed,
			.decompressed_type = decompressed_type,
		};
	}
}

static void
init_iterator(RowDecompressor *decompressor, CompressedDataHeader *header, int input_column)
{
	Assert(decompressor->in_desc->natts > input_column);
	PerCompressedColumn *column_info = &decompressor->per_compressed_cols[input_column];

	/* Special compression block with the NULL compression algorithm,
	 * tells that all values in the compressed block are NULLs.
	 */
	if (header->compression_algorithm == COMPRESSION_ALGORITHM_NULL)
	{
		column_info->iterator = NULL;
		decompressor->compressed_is_nulls[input_column] = true;
		decompressor->decompressed_is_nulls[column_info->decompressed_column_offset] = true;
		return;
	}

	column_info->iterator =
		definitions[header->compression_algorithm]
			.iterator_init_forward(PointerGetDatum(header), column_info->decompressed_type);
}

static void
init_batch(RowDecompressor *decompressor, AttrNumber *attnos, int num_attnos)
{
	/*
	 * Set segmentbys and compressed columns with default value.
	 */
	for (int input_column = 0; input_column < decompressor->in_desc->natts; input_column++)
	{
		PerCompressedColumn *column_info = &decompressor->per_compressed_cols[input_column];
		const int output_index = column_info->decompressed_column_offset;

		/* Metadata column. */
		if (output_index < 0)
		{
			continue;
		}

		/* Segmentby column. */
		if (!column_info->is_compressed)
		{
			decompressor->decompressed_datums[output_index] =
				decompressor->compressed_datums[input_column];
			decompressor->decompressed_is_nulls[output_index] =
				decompressor->compressed_is_nulls[input_column];
			continue;
		}

		/* Compressed column with default value. */
		if (decompressor->compressed_is_nulls[input_column])
		{
			column_info->iterator = NULL;
			decompressor->decompressed_datums[output_index] =
				getmissingattr(decompressor->out_desc,
							   output_index + 1,
							   &decompressor->decompressed_is_nulls[output_index]);

			continue;
		}

		/* Only initialize required columns if specified. */
		bool found = num_attnos == 0;
		for (int i = 0; i < num_attnos; i++)
		{
			if (output_index == AttrNumberGetAttrOffset(attnos[i]))
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			column_info->iterator = NULL;
			continue;
		}

		/* Normal compressed column. */
		Datum compressed_datum = PointerGetDatum(
			detoaster_detoast_attr_copy((struct varlena *) DatumGetPointer(
											decompressor->compressed_datums[input_column]),
										&decompressor->detoaster,
										CurrentMemoryContext));
		CompressedDataHeader *header = get_compressed_data_header(compressed_datum);

		init_iterator(decompressor, header, input_column);
	}
}

/*
 * Decompresses the current compressed batch into decompressed_slots, and returns
 * the number of rows in batch.
 */
int
decompress_batch(RowDecompressor *decompressor)
{
	if (decompressor->unprocessed_tuples)
		return decompressor->unprocessed_tuples;

	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	init_batch(decompressor, NULL, 0);

	/*
	 * Set the number of batch rows from count metadata column.
	 */
	const int n_batch_rows =
		DatumGetInt32(decompressor->compressed_datums[decompressor->count_compressed_attindex]);
	CheckCompressedData(n_batch_rows > 0);
	CheckCompressedData(n_batch_rows <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	/*
	 * Decompress all compressed columns for each row of the batch.
	 */
	for (int current_row = 0; current_row < n_batch_rows; current_row++)
	{
		for (int col = 0; col < decompressor->in_desc->natts; col++)
		{
			PerCompressedColumn *column_info = &decompressor->per_compressed_cols[col];
			if (column_info->iterator == NULL)
			{
				continue;
			}
			Assert(column_info->is_compressed);

			const int output_index = column_info->decompressed_column_offset;
			const DecompressResult value = column_info->iterator->try_next(column_info->iterator);
			CheckCompressedData(!value.is_done);
			decompressor->decompressed_datums[output_index] = value.val;
			decompressor->decompressed_is_nulls[output_index] = value.is_null;
		}

		/*
		 * Form the heap tuple for this decompressed rows and save it for later
		 * processing.
		 */
		if (decompressor->decompressed_slots[current_row] == NULL)
		{
			MemoryContextSwitchTo(old_ctx);
			decompressor->decompressed_slots[current_row] =
				MakeSingleTupleTableSlot(decompressor->out_desc, &TTSOpsHeapTuple);
			MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);
		}
		else
		{
			ExecClearTuple(decompressor->decompressed_slots[current_row]);
		}

		TupleTableSlot *decompressed_slot = decompressor->decompressed_slots[current_row];

		HeapTuple decompressed_tuple = heap_form_tuple(decompressor->out_desc,
													   decompressor->decompressed_datums,
													   decompressor->decompressed_is_nulls);

		ExecStoreHeapTuple(decompressed_tuple, decompressed_slot, /* should_free = */ false);
	}

	/*
	 * Verify that all other columns have ended, i.e. their length is consistent
	 * with the count metadata column.
	 */
	for (int col = 0; col < decompressor->in_desc->natts; col++)
	{
		PerCompressedColumn *column_info = &decompressor->per_compressed_cols[col];
		if (column_info->iterator == NULL)
		{
			continue;
		}
		Assert(column_info->is_compressed);
		const DecompressResult value = column_info->iterator->try_next(column_info->iterator);
		CheckCompressedData(value.is_done);
	}
	MemoryContextSwitchTo(old_ctx);

	decompressor->batches_decompressed++;
	decompressor->tuples_decompressed += n_batch_rows;

	decompressor->unprocessed_tuples = n_batch_rows;

	return n_batch_rows;
}

/*
 * Decompresses a single row from current compressed batch
 * into decompressed_values and decompressed_is_nulls based on the
 * attnos provided.
 *
 * Returns true if the row was decompressed or false if it finished the batch.
 */
bool
decompress_batch_next_row(RowDecompressor *decompressor, AttrNumber *attnos, int num_attnos)
{
	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	if (decompressor->unprocessed_tuples > 0)
	{
		decompressor->unprocessed_tuples--;
		if (decompressor->unprocessed_tuples == 0)
		{
			MemoryContextSwitchTo(old_ctx);
			return false;
		}
	}
	else
	{
		decompressor->batches_decompressed++;
		init_batch(decompressor, attnos, num_attnos);

		/*
		 * Set the number of batch rows from count metadata column.
		 */
		decompressor->unprocessed_tuples =
			DatumGetInt32(decompressor->compressed_datums[decompressor->count_compressed_attindex]);
		CheckCompressedData(decompressor->unprocessed_tuples > 0);
		CheckCompressedData(decompressor->unprocessed_tuples <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	}

	for (int col = 0; col < decompressor->in_desc->natts; col++)
	{
		PerCompressedColumn *column_info = &decompressor->per_compressed_cols[col];
		if (column_info->iterator == NULL)
		{
			continue;
		}
		Assert(column_info->is_compressed);

		const int output_index = column_info->decompressed_column_offset;
		const DecompressResult value = column_info->iterator->try_next(column_info->iterator);
		Assert(!value.is_done);
		decompressor->decompressed_datums[output_index] = value.val;
		decompressor->decompressed_is_nulls[output_index] = value.is_null;
	}

	decompressor->tuples_decompressed++;

	MemoryContextSwitchTo(old_ctx);

	return true;
}

/* Decompress single column using vectorized decompression */
ArrowArray *
decompress_single_column(RowDecompressor *decompressor, AttrNumber attno, bool *single_value)
{
	int16 target_col = -1;
	PerCompressedColumn *column_info = NULL;

	for (int col = 0; col < decompressor->in_desc->natts; col++)
	{
		column_info = &decompressor->per_compressed_cols[col];
		if (!column_info->is_compressed)
			continue;

		if (column_info->decompressed_column_offset == AttrNumberGetAttrOffset(attno))
		{
			target_col = col;
			break;
		}
	}
	Assert(column_info && target_col > -1);

	if (decompressor->compressed_is_nulls[target_col])
	{
		/* Compressed column has a default value, handle it by generating
		 * a single-value ArrowArray based on the default value. This will have to
		 * be handled specially because of the assumption that the whole row has
		 * this default value.
		 */
		*single_value = true;
		bool isnull;
		Datum default_datum = getmissingattr(decompressor->out_desc, attno, &isnull);

		return make_single_value_arrow(column_info->decompressed_type, default_datum, isnull);
	}

	*single_value = false;

	Datum compressed_datum = PointerGetDatum(
		detoaster_detoast_attr_copy((struct varlena *) DatumGetPointer(
										decompressor->compressed_datums[target_col]),
									&decompressor->detoaster,
									CurrentMemoryContext));
	CompressedDataHeader *header = get_compressed_data_header(compressed_datum);

	/* Handle NULL compression algorithm */
	if (header->compression_algorithm == COMPRESSION_ALGORITHM_NULL)
	{
		*single_value = true;
		return make_single_value_arrow(column_info->decompressed_type, (Datum) NULL, true);
	}

	DecompressAllFunction decompress_all =
		tsl_get_decompress_all_function(header->compression_algorithm,
										column_info->decompressed_type);

	Assert(decompress_all);

	return decompress_all(compressed_datum,
						  column_info->decompressed_type,
						  decompressor->per_compressed_row_ctx);
}

int
row_decompressor_decompress_row_to_table(RowDecompressor *decompressor, BulkWriter *writer)
{
	const int n_batch_rows = decompress_batch(decompressor);

	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	/* Insert all decompressed rows into table using the bulk insert API. */
	table_multi_insert(writer->out_rel,
					   decompressor->decompressed_slots,
					   n_batch_rows,
					   writer->mycid,
					   /* options = */ 0,
					   writer->bistate);

	/*
	 * Now, update the indexes. If we have several indexes, we want to first
	 * insert the entire batch into one index, then into another, and so on.
	 * Working with one index at a time gives better data access locality,
	 * which reduces the load on shared buffers cache.
	 * The normal Postgres code inserts each row into all indexes, so to do it
	 * the other way around, we create a temporary ResultRelInfo that only
	 * references one index. Then we loop over indexes, and for each index we
	 * set it to this temporary ResultRelInfo, and insert all rows into this
	 * single index.
	 */
	if (writer->indexstate->ri_NumIndices > 0)
	{
		ResultRelInfo indexstate_copy = *writer->indexstate;
		Relation single_index_relation;
		IndexInfo *single_index_info;
		indexstate_copy.ri_NumIndices = 1;
		indexstate_copy.ri_IndexRelationDescs = &single_index_relation;
		indexstate_copy.ri_IndexRelationInfo = &single_index_info;
		for (int i = 0; i < writer->indexstate->ri_NumIndices; i++)
		{
			single_index_relation = writer->indexstate->ri_IndexRelationDescs[i];
			single_index_info = writer->indexstate->ri_IndexRelationInfo[i];
			for (int row = 0; row < n_batch_rows; row++)
			{
				TupleTableSlot *decompressed_slot = decompressor->decompressed_slots[row];
				EState *estate = writer->estate;
				ExprContext *econtext = GetPerTupleExprContext(estate);

				/* Arrange for econtext's scan tuple to be the tuple under test */
				econtext->ecxt_scantuple = decompressed_slot;
				ExecInsertIndexTuplesCompat(&indexstate_copy,
											decompressed_slot,
											estate,
											false,
											false,
											NULL,
											NIL,
											false);
			}
		}
	}

	MemoryContextSwitchTo(old_ctx);
	row_decompressor_reset(decompressor);

	return n_batch_rows;
}

void
row_decompressor_decompress_row_to_tuplesort(RowDecompressor *decompressor,
											 Tuplesortstate *tuplesortstate)
{
	const int n_batch_rows = decompress_batch(decompressor);

	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	for (int i = 0; i < n_batch_rows; i++)
	{
		tuplesort_puttupleslot(tuplesortstate, decompressor->decompressed_slots[i]);
	}

	MemoryContextSwitchTo(old_ctx);
	row_decompressor_reset(decompressor);
}

/********************/
/*** SQL Bindings ***/
/********************/

Datum
tsl_compressed_data_decompress_forward(PG_FUNCTION_ARGS)
{
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		header = get_compressed_data_header(PG_GETARG_DATUM(0));

		iter = definitions[header->compression_algorithm]
				   .iterator_init_forward(PointerGetDatum(header),
										  get_fn_expr_argtype(fcinfo->flinfo, 1));

		funcctx->user_fctx = iter;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	iter = funcctx->user_fctx;
	res = iter->try_next(iter);

	if (res.is_done)
		SRF_RETURN_DONE(funcctx);

	if (res.is_null)
		SRF_RETURN_NEXT_NULL(funcctx);

	SRF_RETURN_NEXT(funcctx, res.val);
}

Datum
tsl_compressed_data_decompress_reverse(PG_FUNCTION_ARGS)
{
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		header = get_compressed_data_header(PG_GETARG_DATUM(0));

		iter = definitions[header->compression_algorithm]
				   .iterator_init_reverse(PointerGetDatum(header),
										  get_fn_expr_argtype(fcinfo->flinfo, 1));

		funcctx->user_fctx = iter;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	iter = funcctx->user_fctx;
	res = iter->try_next(iter);

	if (res.is_done)
		SRF_RETURN_DONE(funcctx);

	if (res.is_null)
		SRF_RETURN_NEXT_NULL(funcctx);

	SRF_RETURN_NEXT(funcctx, res.val);
	;
}

Datum
tsl_compressed_data_send(PG_FUNCTION_ARGS)
{
	CompressedDataHeader *header = get_compressed_data_header(PG_GETARG_DATUM(0));
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendbyte(&buf, header->compression_algorithm);

	if (header->compression_algorithm != COMPRESSION_ALGORITHM_NULL)
	{
		definitions[header->compression_algorithm].compressed_data_send(header, &buf);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
tsl_compressed_data_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	CompressedDataHeader header = { .vl_len_ = { 0 } };

	header.compression_algorithm = pq_getmsgbyte(buf);

	if (header.compression_algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", header.compression_algorithm);

	return definitions[header.compression_algorithm].compressed_data_recv(buf);
}

extern Datum
tsl_compressed_data_in(PG_FUNCTION_ARGS)
{
	const char *input = PG_GETARG_CSTRING(0);
	size_t input_len = strlen(input);
	int decoded_len;
	char *decoded;
	StringInfoData data;
	Datum result;

	if (input_len > PG_INT32_MAX)
		elog(ERROR, "input too long");

	decoded_len = pg_b64_dec_len(input_len);
	decoded = palloc(decoded_len + 1);
	decoded_len = pg_b64_decode(input, input_len, decoded, decoded_len);

	if (decoded_len < 0)
		elog(ERROR, "could not decode base64-encoded compressed data");

	decoded[decoded_len] = '\0';
	data = (StringInfoData){
		.data = decoded,
		.len = decoded_len,
		.maxlen = decoded_len,
	};

	result = DirectFunctionCall1(tsl_compressed_data_recv, PointerGetDatum(&data));

	PG_RETURN_DATUM(result);
}

extern Datum
tsl_compressed_data_out(PG_FUNCTION_ARGS)
{
	Datum bytes_data = DirectFunctionCall1(tsl_compressed_data_send, PG_GETARG_DATUM(0));
	bytea *bytes = DatumGetByteaP(bytes_data);
	int raw_len = VARSIZE_ANY_EXHDR(bytes);
	const char *raw_data = VARDATA(bytes);
	int encoded_len = pg_b64_enc_len(raw_len);
	char *encoded = palloc(encoded_len + 1);
	encoded_len = pg_b64_encode(raw_data, raw_len, encoded, encoded_len);

	if (encoded_len < 0)
		elog(ERROR, "could not base64-encode compressed data");

	encoded[encoded_len] = '\0';

	PG_RETURN_CSTRING(encoded);
}

/* create_hypertable record attribute numbers */
enum Anum_compressed_info
{
	Anum_compressed_info_algorithm = 1,
	Anum_compressed_info_has_nulls,
	_Anum_compressed_info_max,
};

#define Natts_compressed_info (_Anum_compressed_info_max - 1)

extern Datum
tsl_compressed_data_info(PG_FUNCTION_ARGS)
{
	const CompressedDataHeader *header = get_compressed_data_header(PG_GETARG_DATUM(0));
	TupleDesc tupdesc;
	HeapTuple tuple;
	bool has_nulls = false;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	switch (header->compression_algorithm)
	{
		case COMPRESSION_ALGORITHM_GORILLA:
			has_nulls = gorilla_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_DICTIONARY:
			has_nulls = dictionary_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_DELTADELTA:
			has_nulls = deltadelta_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_ARRAY:
			has_nulls = array_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_BOOL:
			has_nulls = bool_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_NULL:
			has_nulls = true;
			break;
		case COMPRESSION_ALGORITHM_UUID:
			has_nulls = uuid_compressed_has_nulls(header);
			break;
		default:
			elog(ERROR, "unknown compression algorithm %d", header->compression_algorithm);
			break;
	}

	tupdesc = BlessTupleDesc(tupdesc);

	Datum values[Natts_compressed_info];
	bool nulls[Natts_compressed_info] = { false };

	values[AttrNumberGetAttrOffset(Anum_compressed_info_algorithm)] =
		NameGetDatum(compression_get_algorithm_name(header->compression_algorithm));
	values[AttrNumberGetAttrOffset(Anum_compressed_info_has_nulls)] = BoolGetDatum(has_nulls);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

extern Datum
tsl_compressed_data_has_nulls(PG_FUNCTION_ARGS)
{
	const CompressedDataHeader *header = get_compressed_data_header(PG_GETARG_DATUM(0));
	bool has_nulls = false;

	switch (header->compression_algorithm)
	{
		case COMPRESSION_ALGORITHM_GORILLA:
			has_nulls = gorilla_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_DICTIONARY:
			has_nulls = dictionary_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_DELTADELTA:
			has_nulls = deltadelta_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_ARRAY:
			has_nulls = array_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_BOOL:
			has_nulls = bool_compressed_has_nulls(header);
			break;
		case COMPRESSION_ALGORITHM_NULL:
			has_nulls = true;
			break;
		case COMPRESSION_ALGORITHM_UUID:
			has_nulls = uuid_compressed_has_nulls(header);
			break;
		default:
			elog(ERROR, "unknown compression algorithm %d", header->compression_algorithm);
			break;
	}

	return BoolGetDatum(has_nulls);
}

extern CompressionStorage
compression_get_toast_storage(CompressionAlgorithm algorithm)
{
	if (algorithm == _INVALID_COMPRESSION_ALGORITHM || algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);
	return definitions[algorithm].compressed_data_storage;
}

/*
 * Return a default compression algorithm suitable
 * for the type. The actual algorithm used for a
 * type might be different though since the compressor
 * can deviate from the default. The actual algorithm
 * used for a specific batch can only be determined
 * by reading the batch header.
 */
extern CompressionAlgorithm
compression_get_default_algorithm(Oid typeoid)
{
	switch (typeoid)
	{
		case INT4OID:
		case INT2OID:
		case INT8OID:
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return COMPRESSION_ALGORITHM_DELTADELTA;

		case FLOAT4OID:
		case FLOAT8OID:
			return COMPRESSION_ALGORITHM_GORILLA;

		case NUMERICOID:
			return COMPRESSION_ALGORITHM_ARRAY;

		case BOOLOID:
			if (ts_guc_enable_bool_compression)
				return COMPRESSION_ALGORITHM_BOOL;
			else
				return COMPRESSION_ALGORITHM_ARRAY;

		case UUIDOID:
			if (ts_guc_enable_uuid_compression)
				return COMPRESSION_ALGORITHM_UUID;
			else
				return COMPRESSION_ALGORITHM_DICTIONARY;

		default:
		{
			/* use dictionary if possible, otherwise use array */
			TypeCacheEntry *tentry =
				lookup_type_cache(typeoid, TYPECACHE_EQ_OPR_FINFO | TYPECACHE_HASH_PROC_FINFO);
			if (tentry->hash_proc_finfo.fn_addr == NULL || tentry->eq_opr_finfo.fn_addr == NULL)
				return COMPRESSION_ALGORITHM_ARRAY;
			return COMPRESSION_ALGORITHM_DICTIONARY;
		}
	}
}

const CompressionAlgorithmDefinition *
algorithm_definition(CompressionAlgorithm algo)
{
	Assert(algo > 0 && algo < _END_COMPRESSION_ALGORITHMS);
	return &definitions[algo];
}
