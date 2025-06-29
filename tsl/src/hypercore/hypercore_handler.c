/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/heapam.h>
#include <access/hio.h>
#include <access/htup_details.h>
#include <access/relscan.h>
#include <access/rewriteheap.h>
#include <access/sdir.h>
#include <access/skey.h>
#include <access/stratnum.h>
#include <access/tableam.h>
#include <access/transam.h>
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/index.h>
#include <catalog/pg_attribute.h>
#include <catalog/storage.h>
#include <commands/progress.h>
#include <commands/vacuum.h>
#include <common/relpath.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/plancat.h>
#include <parser/parsetree.h>
#include <pgstat.h>
#include <postgres_ext.h>
#include <storage/block.h>
#include <storage/buf.h>
#include <storage/bufmgr.h>
#include <storage/itemptr.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <storage/off.h>
#include <storage/procarray.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/sampling.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/tuplesort.h>
#include <utils/typcache.h>

#include <math.h>

#include "arrow_array.h"
#include "arrow_cache.h"
#include "arrow_tts.h"
#include "compression/api.h"
#include "compression/compression.h"
#include "compression/create.h"
#include "debug_assert.h"
#include "extension.h"
#include "guc.h"
#include "hypercore_handler.h"
#include "relstats.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"

#if PG17_GE
#include "import/analyze.h"
#endif

static const TableAmRoutine hypercore_methods;
static void convert_to_hypercore_finish(Oid relid);
static List *partially_compressed_relids = NIL; /* Relids that needs to have
												 * updated status set at end of
												 * transaction */
/*
 * For COPY <hypercore_rel> TO commands, track the relid of the hypercore
 * being copied from. It is needed to filter out compressed data in the COPY
 * scan so that pg_dump does not dump compressed data twice: once in
 * uncompressed format via the hypercore rel and once in compressed format in
 * the internal compressed rel that gets dumped separately.
 */
static Oid hypercore_skip_compressed_data_relid = InvalidOid;

/*
 * Open the compressed relation for a chunk.
 *
 * Note that opening a table can invalidate the rd_amcache field of the
 * RelationData structure (even for relations that are not opened) if an
 * invalidation occurs, which means that after using table_open(), we cannot
 * trust that the HypercoreInfo is valid any more.
 */
static Relation
hypercore_open_compressed(Relation relation, LOCKMODE mode)
{
	return table_open(RelationGetHypercoreInfo(relation)->compressed_relid, mode);
}

void
hypercore_skip_compressed_data_for_relation(Oid relid)
{
	hypercore_skip_compressed_data_relid = relid;
}

static bool hypercore_truncate_compressed = true;

/*
 * Configure whether a TRUNCATE on Hypercore TAM should truncate all data
 * (both compressed and non-compressed) or only non-compressed data.
 *
 * This is used during re-compression where non-compressed data gets folded
 * into existing compressed data. In that case, the existing compressed data
 * should remain, but the non-compressed data that got compressed should be
 * truncated.
 *
 * Note that this setting is sticky so it needs to be reset after the truncate
 * operation completes.
 */
bool
hypercore_set_truncate_compressed(bool onoff)
{
	bool old_value = hypercore_truncate_compressed;
	hypercore_truncate_compressed = onoff;
	return old_value;
}

#define HYPERCORE_AM_INFO_SIZE(natts)                                                              \
	(sizeof(HypercoreInfo) + (sizeof(ColumnCompressionSettings) * (natts)))

static void
check_guc_setting_compatible_with_scan()
{
	if (ts_guc_enable_transparent_decompression == 2)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("operation not compatible with current setting of %s",
						MAKE_EXTOPTION("enable_transparent_decompression")),
				 errhint("Set the GUC to true or false.")));
}

static const TableAmRoutine *
switch_to_heapam(Relation rel)
{
	const TableAmRoutine *tableam = rel->rd_tableam;
	Assert(tableam == hypercore_routine());
	rel->rd_tableam = GetHeapamTableAmRoutine();
	return tableam;
}

static void
create_proxy_vacuum_index(Relation rel, Oid compressed_relid)
{
	Oid compressed_namespaceid = get_rel_namespace(compressed_relid);
	char *compressed_namespace = get_namespace_name(compressed_namespaceid);
	char *compressed_relname = get_rel_name(compressed_relid);
	IndexElem elem = {
		.type = T_IndexElem,
		.name = COMPRESSION_COLUMN_METADATA_COUNT_NAME,
		.indexcolname = NULL,
	};
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = "hypercore_proxy",
		.idxcomment = "Hypercore vacuum proxy index",
		.idxname = psprintf("%s_ts_hypercore_proxy_idx", compressed_relname),
		.indexParams = list_make1(&elem),
		.relation = makeRangeVar(compressed_namespace, compressed_relname, -1),
	};

	DefineIndexCompat(compressed_relid,
					  &stmt,
					  InvalidOid,
					  InvalidOid,
					  InvalidOid,
					  -1,
					  false,
					  false,
					  false,
					  false,
					  true);
}

static void
create_compression_relation_size_stats(int32 chunk_id, Oid relid, int32 compress_chunk_id,
									   Oid compress_relid, RelationSize *before_size,
									   int64 num_rows_pre, int64 num_rows_post,
									   int64 num_rows_frozen)
{
	RelationSize after_size = ts_relation_size_impl(compress_relid);
	compression_chunk_size_catalog_insert(chunk_id,
										  before_size,
										  compress_chunk_id,
										  &after_size,
										  num_rows_pre,
										  num_rows_post,
										  num_rows_frozen);
}

static HypercoreInfo *
lazy_build_hypercore_info_cache(Relation rel, bool create_chunk_constraints,
								bool *compressed_relation_created)
{
	Assert(OidIsValid(rel->rd_id) && (!ts_extension_is_loaded() || !ts_is_hypertable(rel->rd_id)));

	PushActiveSnapshot(GetTransactionSnapshot());

	const CompressionSettings *settings;
	HypercoreInfo *hcinfo;
	TupleDesc tupdesc = RelationGetDescr(rel);
	Oid relid = RelationGetRelid(rel);

	/* Anything put in rel->rd_amcache must be a single memory chunk
	 * palloc'd in CacheMemoryContext since PostgreSQL expects to be able
	 * to free it with a single pfree(). */
	hcinfo = MemoryContextAllocZero(CacheMemoryContext, HYPERCORE_AM_INFO_SIZE(tupdesc->natts));
	hcinfo->compressed_relid = InvalidOid;
	hcinfo->num_columns = tupdesc->natts;

	settings = ts_compression_settings_get(relid);

	/* Create compressed chunk and set the created flag if it does not
	 * exist. */
	if (compressed_relation_created)
		*compressed_relation_created = (settings == NULL);

	if (settings == NULL)
	{
		/* Consider if we want to make it simpler to create the compressed
		 * table by just considering a normal side-relation with no strong
		 * connection to the original chunk. We do not need constraints,
		 * foreign keys, or any other things on this table since it never
		 * participate in any plans. */
		Chunk *chunk = ts_chunk_get_by_relid(rel->rd_id, true);
		Hypertable *ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
		Hypertable *ht_compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);

		if (NULL == ht_compressed)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("hypertable \"%s\" is missing compression settings",
							NameStr(ht->fd.table_name)),
					 errhint("Enable compression on the hypertable.")));

		Chunk *c_chunk = create_compress_chunk(ht_compressed, chunk, InvalidOid);

		ts_chunk_set_compressed_chunk(chunk, c_chunk->fd.id);

		if (create_chunk_constraints)
		{
			create_proxy_vacuum_index(rel, c_chunk->table_id);
			RelationSize before_size = ts_relation_size_impl(RelationGetRelid(rel));
			create_compression_relation_size_stats(chunk->fd.id,
												   RelationGetRelid(rel),
												   c_chunk->fd.id,
												   c_chunk->table_id,
												   &before_size,
												   0,
												   0,
												   0);
		}

		settings = ts_compression_settings_get(relid);
	}

	Ensure(settings,
		   "no compression settings for relation %s",
		   get_rel_name(RelationGetRelid(rel)));

	hcinfo->compressed_relid = settings->fd.compress_relid;
	hcinfo->count_cattno =
		get_attnum(hcinfo->compressed_relid, COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	Assert(hcinfo->count_cattno != InvalidAttrNumber);

	for (int i = 0; i < hcinfo->num_columns; i++)
	{
		const Form_pg_attribute attr = &tupdesc->attrs[i];
		ColumnCompressionSettings *colsettings = &hcinfo->columns[i];

		if (attr->attisdropped)
		{
			colsettings->attnum = InvalidAttrNumber;
			colsettings->cattnum = InvalidAttrNumber;
			colsettings->is_dropped = true;
			continue;
		}

		const char *attname = NameStr(attr->attname);
		int segmentby_pos = ts_array_position(settings->fd.segmentby, attname);
		int orderby_pos = ts_array_position(settings->fd.orderby, attname);

		namestrcpy(&colsettings->attname, attname);
		colsettings->attnum = attr->attnum;
		colsettings->typid = attr->atttypid;
		colsettings->is_segmentby = segmentby_pos > 0;
		colsettings->is_orderby = orderby_pos > 0;

		if (OidIsValid(hcinfo->compressed_relid))
			colsettings->cattnum = get_attnum(hcinfo->compressed_relid, attname);
		else
			colsettings->cattnum = InvalidAttrNumber;

		if (colsettings->is_orderby)
		{
			const char *min_attname = column_segment_min_name(orderby_pos);
			const char *max_attname = column_segment_max_name(orderby_pos);
			colsettings->cattnum_min = get_attnum(hcinfo->compressed_relid, min_attname);
			colsettings->cattnum_max = get_attnum(hcinfo->compressed_relid, max_attname);
		}
		else
		{
			const char *min_attname = compressed_column_metadata_name_v2("min", attname);
			const char *max_attname = compressed_column_metadata_name_v2("max", attname);
			colsettings->cattnum_min = get_attnum(hcinfo->compressed_relid, min_attname);
			colsettings->cattnum_max = get_attnum(hcinfo->compressed_relid, max_attname);
		}
	}
	PopActiveSnapshot();

	return hcinfo;
}

/*
 * Get hypercore info for relation.
 *
 * Note that the hypercore info can be freed unexpectedly and hence you cannot
 * rely on this over any PostgreSQL calls. In particular, table_open() can
 * invalidate rd_amcache, meaning that the data will be invalid.
 */
HypercoreInfo *
RelationGetHypercoreInfo(Relation rel)
{
	if (NULL == rel->rd_amcache)
		rel->rd_amcache = lazy_build_hypercore_info_cache(rel, true, NULL);

	Assert(rel->rd_amcache && OidIsValid(((HypercoreInfo *) rel->rd_amcache)->compressed_relid));
	return (HypercoreInfo *) rel->rd_amcache;
}

static void
build_segment_and_orderby_bms(const HypercoreInfo *hcinfo, Bitmapset **segmentby,
							  Bitmapset **orderby)
{
	*segmentby = NULL;
	*orderby = NULL;

	for (int i = 0; i < hcinfo->num_columns; i++)
	{
		const ColumnCompressionSettings *colsettings = &hcinfo->columns[i];

		if (colsettings->is_segmentby)
			*segmentby = bms_add_member(*segmentby, colsettings->attnum);

		if (colsettings->is_orderby)
			*orderby = bms_add_member(*orderby, colsettings->attnum);
	}
}

/* ------------------------------------------------------------------------
 * Slot related callbacks for Hypercore
 * ------------------------------------------------------------------------
 */
static const TupleTableSlotOps *
hypercore_slot_callbacks(Relation relation)
{
	return &TTSOpsArrowTuple;
}

#define FEATURE_NOT_SUPPORTED                                                                      \
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("%s not supported", __func__)))

#define pgstat_count_hypercore_scan(rel) pgstat_count_heap_scan(rel)

#define pgstat_count_hypercore_getnext(rel) pgstat_count_heap_getnext(rel)

typedef struct HypercoreParallelScanDescData
{
	ParallelBlockTableScanDescData pscandesc;
	ParallelBlockTableScanDescData cpscandesc;
} HypercoreParallelScanDescData;

typedef struct HypercoreParallelScanDescData *HypercoreParallelScanDesc;

typedef enum HypercoreScanState
{
	HYPERCORE_SCAN_START = 0,
	HYPERCORE_SCAN_COMPRESSED = HYPERCORE_SCAN_START,
	HYPERCORE_SCAN_NON_COMPRESSED = 1,
	HYPERCORE_SCAN_DONE = 2,
} HypercoreScanState;

const char *scan_state_name[] = {
	[HYPERCORE_SCAN_COMPRESSED] = "COMPRESSED",
	[HYPERCORE_SCAN_NON_COMPRESSED] = "NON_COMPRESSED",
	[HYPERCORE_SCAN_DONE] = "DONE",
};

typedef struct HypercoreScanDescData
{
	TableScanDescData rs_base;
	TableScanDesc uscan_desc; /* scan descriptor for non-compressed relation */
	Relation compressed_rel;
	TableScanDesc cscan_desc; /* scan descriptor for compressed relation */
	int64 returned_noncompressed_count;
	int64 returned_compressed_count;
	int32 compressed_row_count;
	HypercoreScanState hs_scan_state;
	bool reset;
	bool skip_compressed; /* Skip compressed data when scanning */
#if PG17_GE
	/* These fields are only used for ANALYZE */
	ReadStream *canalyze_read_stream;
	ReadStream *uanalyze_read_stream;
#endif
} HypercoreScanDescData;

typedef struct HypercoreScanDescData *HypercoreScanDesc;

static bool hypercore_getnextslot_noncompressed(HypercoreScanDesc scan, ScanDirection direction,
												TupleTableSlot *slot);
static bool hypercore_getnextslot_compressed(HypercoreScanDesc scan, ScanDirection direction,
											 TupleTableSlot *slot);

/*
 * Skip scanning compressed data in a table scan.
 *
 * This function can be called on a scan descriptor to skip scanning of
 * compressed data. Typically called directly after table_beginscan().
 */
void
hypercore_scan_set_skip_compressed(TableScanDesc scan, bool skip)
{
	HypercoreScanDesc hscan;

	if (!REL_IS_HYPERCORE(scan->rs_rd))
		return;

	hscan = (HypercoreScanDesc) scan;

	if (skip)
	{
		scan->rs_flags |= SO_HYPERCORE_SKIP_COMPRESSED;
		hscan->hs_scan_state = HYPERCORE_SCAN_NON_COMPRESSED;
	}
	else
	{
		scan->rs_flags &= ~SO_HYPERCORE_SKIP_COMPRESSED;
		hscan->hs_scan_state = HYPERCORE_SCAN_START;
	}
}

#if PG17_GE
static int
compute_targrows(Relation rel)
{
	MemoryContext analyze_context =
		AllocSetContextCreate(CurrentMemoryContext, "Hypercore Analyze", ALLOCSET_DEFAULT_SIZES);

	VacAttrStats **vacattrstats;
	int attr_cnt = hypercore_analyze_compute_vacattrstats(rel, &vacattrstats, analyze_context);
	int targrows = 100;
	for (int i = 0; i < attr_cnt; i++)
	{
		if (targrows < vacattrstats[i]->minrows)
		{
			targrows = vacattrstats[i]->minrows;
		}
	}
	MemoryContextDelete(analyze_context);
	return targrows;
}
#endif

static void
scankey_init(const TypeCacheEntry *tce, const ScanKey oldkey, ScanKey newkey,
			 const AttrNumber newattno, StrategyNumber newstrategy)
{
	Oid opno = get_opfamily_member(tce->btree_opf, tce->type_id, oldkey->sk_subtype, newstrategy);
	ScanKeyEntryInitialize(newkey,
						   0,
						   newattno,
						   oldkey->sk_strategy,
						   oldkey->sk_subtype,
						   oldkey->sk_collation,
						   get_opcode(opno),
						   oldkey->sk_argument);
}

/*
 * Initialization common for beginscan and rescan.
 */
static void
initscan(HypercoreScanDesc scan, ScanKey keys, int nkeys)
{
	int nvalidkeys = 0;

	/*
	 * Translate any scankeys to the corresponding scankeys on the compressed
	 * relation.
	 *
	 * It is only possible to use scankeys in the following two cases:
	 *
	 * 1. The scankey is for a segmentby column
	 * 2. The scankey is for a column that has min/max metadata (e.g., orderby column).
	 *
	 * For case (2), it is necessary to translate the scankey on the
	 * non-compressed relation to two min and max scankeys on the compressed
	 * relation.
	 *
	 * Note that scankeys should only contain btree strategies for heap
	 * scans. ColumnarScan is currently the only node pushing down scankeys
	 * and it always creates btree strategies.
	 */
	if (NULL != keys && nkeys > 0)
	{
		const HypercoreInfo *hcinfo = RelationGetHypercoreInfo(scan->rs_base.rs_rd);

		for (int i = 0; i < nkeys; i++)
		{
			const ScanKey key = &keys[i];

			for (int j = 0; j < hcinfo->num_columns; j++)
			{
				const ColumnCompressionSettings *column = &hcinfo->columns[j];

				if (column->is_segmentby && key->sk_attno == column->attnum)
				{
					scan->rs_base.rs_key[nvalidkeys] = *key;
					/* Remap the attribute number to the corresponding
					 * compressed rel attribute number */
					scan->rs_base.rs_key[nvalidkeys].sk_attno = column->cattnum;
					nvalidkeys++;
					break;
				}

				/* Transform equality to min/max on metadata columns */
				else if (key->sk_attno == column->attnum && hypercore_column_has_minmax(column))
				{
					const TypeCacheEntry *tce =
						lookup_type_cache(column->typid, TYPECACHE_BTREE_OPFAMILY);

					/* Type cache never returns NULL */
					Assert(tce);

					/* Assert that the scankey's strategy is indeed a btree
					 * strategy by checking that the key's function is a btree
					 * function. */
					Assert(key->sk_func.fn_oid ==
						   get_opcode(get_opfamily_member(tce->btree_opf,
														  tce->type_id,
														  key->sk_subtype,
														  key->sk_strategy)));
					switch (key->sk_strategy)
					{
						case BTLessStrategyNumber:
						case BTLessEqualStrategyNumber:
						{
							/*
							 * The operators '<' and '<=' translate to the
							 * same operators on the min metadata column
							 */
							scankey_init(tce,
										 key,
										 &scan->rs_base.rs_key[nvalidkeys++],
										 column->cattnum_min,
										 key->sk_strategy);
							break;
						}
						case BTEqualStrategyNumber:
						{
							/*
							 * Equality ('=') translates to:
							 *
							 * x <= min_col AND x >= max_col
							 */

							scankey_init(tce,
										 key,
										 &scan->rs_base.rs_key[nvalidkeys++],
										 column->cattnum_min,
										 BTLessEqualStrategyNumber);
							scankey_init(tce,
										 key,
										 &scan->rs_base.rs_key[nvalidkeys++],
										 column->cattnum_max,
										 BTGreaterEqualStrategyNumber);
							break;
						}
						case BTGreaterEqualStrategyNumber:
						case BTGreaterStrategyNumber:
						{
							/*
							 * The operators '>' and '>=' translate to the
							 * same operators on the max metadata column
							 */

							scankey_init(tce,
										 key,
										 &scan->rs_base.rs_key[nvalidkeys++],
										 column->cattnum_max,
										 key->sk_strategy);
							break;
						}
						default:
							pg_unreachable();
							Assert(false);
							break;
					}

					break;
				}
			}
		}
	}

	scan->rs_base.rs_nkeys = nvalidkeys;

	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_hypercore_scan(scan->rs_base.rs_rd);
}

#ifdef TS_DEBUG
static const char *
get_scan_type(uint32 flags)
{
	if (flags & SO_TYPE_TIDSCAN)
		return "TID";
	if (flags & SO_TYPE_TIDRANGESCAN)
		return "TID range";
	if (flags & SO_TYPE_BITMAPSCAN)
		return "bitmap";
	if (flags & SO_TYPE_SAMPLESCAN)
		return "sample";
	if (flags & SO_TYPE_ANALYZE)
		return "analyze";
	if (flags & SO_TYPE_SEQSCAN)
		return "sequence";
	return "unknown";
}
#endif

static inline bool
should_skip_compressed_data(const TableScanDesc scan)
{
	/*
	 * Skip compressed data in a scan if any of these apply:
	 *
	 * 1. Transparent decompression (DecompressChunk) is enabled for
	 *    Hypercore TAM.
	 *
	 * 2. The scan was started with a flag indicating no compressed data
	 *    should be returned.
	 *
	 * 3. A COPY <hypercore> TO <file> on the Hypercore TAM table is executed
	 *    and we want to ensure such commands issued by pg_dump doesn't lead
	 *    to dumping compressed data twice.
	 */
	return (ts_guc_enable_transparent_decompression == 2) ||
		   RelationGetRelid(scan->rs_rd) == hypercore_skip_compressed_data_relid ||
		   (scan->rs_flags & SO_HYPERCORE_SKIP_COMPRESSED);
}

static TableScanDesc
hypercore_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey keys,
					ParallelTableScanDesc parallel_scan, uint32 flags)
{
	HypercoreScanDesc scan;
	HypercoreParallelScanDesc cpscan = (HypercoreParallelScanDesc) parallel_scan;

	RelationIncrementReferenceCount(relation);

	TS_DEBUG_LOG("starting %s scan of relation %s parallel_scan=%p nkeys=%d",
				 get_scan_type(flags),
				 RelationGetRelationName(relation),
				 parallel_scan,
				 nkeys);

	scan = palloc0(sizeof(HypercoreScanDescData));
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	/* Allocate double the scan keys to account for some being transformed to
	 * two min/max keys */
	scan->rs_base.rs_key = nkeys > 0 ? palloc0(sizeof(ScanKeyData) * nkeys * 2) : NULL;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;
	scan->returned_noncompressed_count = 0;
	scan->returned_compressed_count = 0;
	scan->compressed_row_count = 0;
	scan->reset = true;

	if (ts_is_hypertable(relation->rd_id))
	{
		/* If this is a hypertable, there is nothing for us to scan */
		scan->hs_scan_state = HYPERCORE_SCAN_DONE;
		return &scan->rs_base;
	}

	scan->compressed_rel = hypercore_open_compressed(relation, AccessShareLock);

	if (should_skip_compressed_data(&scan->rs_base))
	{
		/*
		 * Don't read compressed data if transparent decompression is enabled
		 * or it is requested by the scan.
		 *
		 * Transparent decompression reads compressed data itself, directly
		 * from the compressed chunk, so avoid reading it again here.
		 */
		hypercore_scan_set_skip_compressed(&scan->rs_base, true);
	}

	initscan(scan, keys, nkeys);

	ParallelTableScanDesc ptscan =
		parallel_scan ? (ParallelTableScanDesc) &cpscan->pscandesc : NULL;

	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	scan->uscan_desc =
		relation->rd_tableam->scan_begin(relation, snapshot, nkeys, keys, ptscan, flags);
	relation->rd_tableam = oldtam;

	if (parallel_scan)
	{
		/* Parallel workers use a serialized snapshot that they get from the
		 * coordinator. The snapshot will be marked as a temp snapshot so that
		 * endscan() knows to deregister it. However, if we pass the snapshot
		 * to both sub-scans marked as a temp snapshot it will be deregistered
		 * twice. Therefore remove the temp flag for the second scan. */
		flags &= ~SO_TEMP_SNAPSHOT;
	}

	ParallelTableScanDesc cptscan =
		parallel_scan ? (ParallelTableScanDesc) &cpscan->cpscandesc : NULL;

	scan->cscan_desc = scan->compressed_rel->rd_tableam->scan_begin(scan->compressed_rel,
																	snapshot,
																	scan->rs_base.rs_nkeys,
																	scan->rs_base.rs_key,
																	cptscan,
																	flags);

	return &scan->rs_base;
}

static void
hypercore_rescan(TableScanDesc sscan, ScanKey key, bool set_params, bool allow_strat,
				 bool allow_sync, bool allow_pagemode)
{
	HypercoreScanDesc scan = (HypercoreScanDesc) sscan;

	initscan(scan, key, scan->rs_base.rs_nkeys);
	scan->reset = true;

	if (sscan->rs_flags & SO_HYPERCORE_SKIP_COMPRESSED)
		scan->hs_scan_state = HYPERCORE_SCAN_NON_COMPRESSED;
	else
		scan->hs_scan_state = HYPERCORE_SCAN_START;

	if (scan->cscan_desc)
		table_rescan(scan->cscan_desc, key);

	Relation relation = scan->uscan_desc->rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam
		->scan_rescan(scan->uscan_desc, key, set_params, allow_strat, allow_sync, allow_pagemode);
	relation->rd_tableam = oldtam;
}

static void
hypercore_endscan(TableScanDesc sscan)
{
	HypercoreScanDesc scan = (HypercoreScanDesc) sscan;

	RelationDecrementReferenceCount(sscan->rs_rd);

	if (scan->cscan_desc)
		table_endscan(scan->cscan_desc);
	if (scan->compressed_rel)
		table_close(scan->compressed_rel, AccessShareLock);
#if PG17_GE
	if (scan->canalyze_read_stream)
		read_stream_end(scan->canalyze_read_stream);
	if (scan->uanalyze_read_stream)
		read_stream_end(scan->uanalyze_read_stream);
#endif

	Relation relation = sscan->rs_rd;

	if (scan->uscan_desc)
	{
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		relation->rd_tableam->scan_end(scan->uscan_desc);
		relation->rd_tableam = oldtam;
	}

	TS_DEBUG_LOG("scanned " INT64_FORMAT " tuples (" INT64_FORMAT " compressed, " INT64_FORMAT
				 " noncompressed) in rel %s",
				 scan->returned_compressed_count + scan->returned_noncompressed_count,
				 scan->returned_compressed_count,
				 scan->returned_noncompressed_count,
				 RelationGetRelationName(sscan->rs_rd));

	if (scan->rs_base.rs_key)
		pfree(scan->rs_base.rs_key);

	pfree(scan);

	/* Clear the COPY TO filter state */
	hypercore_skip_compressed_data_relid = InvalidOid;
}

static bool
hypercore_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	if (arrow_slot_try_getnext(slot, direction))
	{
		slot->tts_tableOid = RelationGetRelid(sscan->rs_rd);
		return true;
	}

	HypercoreScanDesc scan = (HypercoreScanDesc) sscan;

	TS_DEBUG_LOG("relid: %d, relation: %s, reset: %s, scan_state: %s",
				 sscan->rs_rd->rd_id,
				 get_rel_name(sscan->rs_rd->rd_id),
				 yes_no(scan->reset),
				 scan_state_name[scan->hs_scan_state]);

	switch (scan->hs_scan_state)
	{
		case HYPERCORE_SCAN_DONE:
			return false; /* Nothing more to scan */
		case HYPERCORE_SCAN_NON_COMPRESSED:
			return hypercore_getnextslot_noncompressed(scan, direction, slot);
		case HYPERCORE_SCAN_COMPRESSED:
			return hypercore_getnextslot_compressed(scan, direction, slot);
	}
	return false; /* To keep compiler happy */
}

static bool
hypercore_getnextslot_noncompressed(HypercoreScanDesc scan, ScanDirection direction,
									TupleTableSlot *slot)
{
	TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
	Relation relation = scan->rs_base.rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	bool result = relation->rd_tableam->scan_getnextslot(scan->uscan_desc, direction, child_slot);
	relation->rd_tableam = oldtam;

	if (result)
	{
		scan->returned_noncompressed_count++;
		slot->tts_tableOid = RelationGetRelid(relation);
		ExecStoreArrowTuple(slot, InvalidTupleIndex);
	}
	else if (direction == BackwardScanDirection)
	{
		scan->hs_scan_state = HYPERCORE_SCAN_COMPRESSED;
		return hypercore_getnextslot(&scan->rs_base, direction, slot);
	}

	return result;
}

static inline bool
should_read_new_compressed_slot(TupleTableSlot *slot, ScanDirection direction)
{
	/* Scans are never invoked with NoMovementScanDirection */
	Assert(direction != NoMovementScanDirection);

	/* A slot can be empty if just started the scan or (or moved back to the
	 * start due to backward scan) */
	if (TTS_EMPTY(slot) || arrow_slot_is_consumed(slot))
		return true;

	if (likely(direction == ForwardScanDirection))
	{
		if (arrow_slot_is_last(slot))
			return true;
	}
	else if (direction == BackwardScanDirection)
	{
		/* Check if backward scan reached the start or the slot values */
		if (arrow_slot_is_first(slot))
			return true;
	}

	return false;
}

static bool
hypercore_getnextslot_compressed(HypercoreScanDesc scan, ScanDirection direction,
								 TupleTableSlot *slot)
{
	TupleTableSlot *child_slot =
		arrow_slot_get_compressed_slot(slot, RelationGetDescr(scan->compressed_rel));

	if (scan->reset || should_read_new_compressed_slot(slot, direction))
	{
		scan->reset = false;

		if (!table_scan_getnextslot(scan->cscan_desc, direction, child_slot))
		{
			ExecClearTuple(slot);

			if (likely(direction == ForwardScanDirection))
			{
				scan->hs_scan_state = HYPERCORE_SCAN_NON_COMPRESSED;
				return hypercore_getnextslot(&scan->rs_base, direction, slot);
			}
			else
			{
				Assert(direction == BackwardScanDirection);
				return false;
			}
		}

		Assert(ItemPointerIsValid(&child_slot->tts_tid));
		ExecStoreArrowTuple(slot, direction == ForwardScanDirection ? 1 : MaxTupleIndex);
		scan->compressed_row_count = arrow_slot_total_row_count(slot);
	}
	else if (direction == ForwardScanDirection)
	{
		ExecStoreNextArrowTuple(slot);
	}
	else
	{
		Assert(direction == BackwardScanDirection);
		ExecStorePreviousArrowTuple(slot);
	}

	slot->tts_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
	scan->returned_compressed_count++;
	pgstat_count_hypercore_getnext(scan->rs_base.rs_rd);
	return true;
}

static Size
hypercore_parallelscan_estimate(Relation rel)
{
	return sizeof(HypercoreParallelScanDescData);
}

/*
 * Initialize ParallelTableScanDesc for a parallel scan of this relation.
 * `pscan` will be sized according to parallelscan_estimate() for the same
 * relation.
 */
static Size
hypercore_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	HypercoreParallelScanDesc cpscan = (HypercoreParallelScanDesc) pscan;

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	table_block_parallelscan_initialize(rel, (ParallelTableScanDesc) &cpscan->pscandesc);
	rel->rd_tableam = oldtam;

	Relation crel = hypercore_open_compressed(rel, AccessShareLock);
	table_block_parallelscan_initialize(crel, (ParallelTableScanDesc) &cpscan->cpscandesc);
	table_close(crel, NoLock);

	return sizeof(HypercoreParallelScanDescData);
}

/*
 * Reinitialize `pscan` for a new scan. `rel` will be the same relation as
 * when `pscan` was initialized by parallelscan_initialize.
 */
static void
hypercore_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	HypercoreParallelScanDesc cpscan = (HypercoreParallelScanDesc) pscan;

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	table_block_parallelscan_reinitialize(rel, (ParallelTableScanDesc) &cpscan->pscandesc);
	rel->rd_tableam = oldtam;

	Relation crel = hypercore_open_compressed(rel, AccessShareLock);
	table_block_parallelscan_reinitialize(crel, (ParallelTableScanDesc) &cpscan->cpscandesc);
	table_close(crel, NoLock);
}

static void
hypercore_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
	HypercoreScanDesc scan = (HypercoreScanDesc) sscan;

	if (is_compressed_tid(tid))
	{
		ItemPointerData decoded_tid;
		uint16 tuple_index = hypercore_tid_decode(&decoded_tid, tid);
		const Relation rel = scan->cscan_desc->rs_rd;
		rel->rd_tableam->tuple_get_latest_tid(scan->cscan_desc, &decoded_tid);
		hypercore_tid_encode(tid, &decoded_tid, tuple_index);
	}
	else
	{
		const Relation rel = scan->uscan_desc->rs_rd;
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		rel->rd_tableam->tuple_get_latest_tid(scan->uscan_desc, tid);
		rel->rd_tableam = oldtam;
	}
}

static void
hypercore_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples, CommandId cid,
					   int options, BulkInsertStateData *bistate)
{
	/* Inserts only supported in non-compressed relation, so simply forward to the heap AM */
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->multi_insert(relation, slots, ntuples, cid, options, bistate);
	relation->rd_tableam = oldtam;

	MemoryContext oldmcxt = MemoryContextSwitchTo(CurTransactionContext);
	partially_compressed_relids =
		list_append_unique_oid(partially_compressed_relids, RelationGetRelid(relation));
	MemoryContextSwitchTo(oldmcxt);
}

enum SegmentbyIndexStatus
{
	SEGMENTBY_INDEX_UNKNOWN = -1,
	SEGMENTBY_INDEX_FALSE = 0,
	SEGMENTBY_INDEX_TRUE = 1,
};

typedef struct IndexFetchComprData
{
	IndexFetchTableData h_base; /* AM independent part of the descriptor */
	IndexFetchTableData *compr_hscan;
	IndexFetchTableData *uncompr_hscan;
	Relation compr_rel;
	ItemPointerData tid;
	int64 num_decompressions;
	uint64 return_count;
	enum SegmentbyIndexStatus segindex;
	bool call_again;		  /* Used to remember the previous value of call_again in
							   * index_fetch_tuple */
	bool internal_call_again; /* Call again passed on to compressed heap */
} IndexFetchComprData;

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for Hypercore
 * ------------------------------------------------------------------------
 */
static IndexFetchTableData *
hypercore_index_fetch_begin(Relation rel)
{
	IndexFetchComprData *cscan = palloc0(sizeof(IndexFetchComprData));
	Relation crel = hypercore_open_compressed(rel, AccessShareLock);
	cscan->segindex = SEGMENTBY_INDEX_UNKNOWN;
	cscan->return_count = 0;
	cscan->h_base.rel = rel;
	cscan->compr_rel = crel;
	cscan->compr_hscan = crel->rd_tableam->index_fetch_begin(crel);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	cscan->uncompr_hscan = rel->rd_tableam->index_fetch_begin(rel);
	rel->rd_tableam = oldtam;

	ItemPointerSetInvalid(&cscan->tid);

	return &cscan->h_base;
}

static void
hypercore_index_fetch_reset(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	Relation rel = scan->rel;

	/* There is no need to reset segindex since there is no change in indexes
	 * used for the index scan when resetting the scan, but we need to reset
	 * the tid since we are restarting an index scan. */
	ItemPointerSetInvalid(&cscan->tid);

	cscan->compr_rel->rd_tableam->index_fetch_reset(cscan->compr_hscan);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->index_fetch_reset(cscan->uncompr_hscan);
	rel->rd_tableam = oldtam;
}

static void
hypercore_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	Relation rel = scan->rel;

	Relation crel = cscan->compr_rel;
	crel->rd_tableam->index_fetch_end(cscan->compr_hscan);
	table_close(crel, AccessShareLock);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->index_fetch_end(cscan->uncompr_hscan);
	rel->rd_tableam = oldtam;
	pfree(cscan);
}

/*
 * Check if the index scan is only on segmentby columns.
 *
 * To identify a segmentby index scan (an index scan only using segmentby
 * columns), it is necessary to know the columns (attributes) indexed by the
 * index. Unfortunately, the TAM does not have access to information about the
 * index being scanned, so this information is instead captured at the start
 * of the scan (using the executor start hook) and stored in the
 * ArrowTupleTableSlot.
 *
 * For EXPLAINs (without ANALYZE), the index attributes in the slot might not
 * be set because the index is never really opened. For such a case, when
 * nothing is actually scanned, it is OK to return "false" even though the
 * query is using a segmentby index.
 *
 * Since the columns scanned by an index scan does not change during a scan,
 * we cache this information to avoid re-computing it each time.
 */
static inline bool
is_segmentby_index_scan(IndexFetchComprData *cscan, TupleTableSlot *slot)
{
	enum SegmentbyIndexStatus segindex = cscan->segindex;
	if (segindex == SEGMENTBY_INDEX_UNKNOWN)
	{
		ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
		const HypercoreInfo *hcinfo = RelationGetHypercoreInfo(cscan->h_base.rel);
		int16 attno = -1;

		if (bms_is_empty(aslot->index_attrs))
			segindex = SEGMENTBY_INDEX_FALSE;
		else
		{
			/* True unless we discover that there is one attribute in the index
			 * that is not on a segment-by */
			segindex = SEGMENTBY_INDEX_TRUE;
			while ((attno = bms_next_member(aslot->index_attrs, attno)) >= 0)
				if (!hcinfo->columns[AttrNumberGetAttrOffset(attno)].is_segmentby)
				{
					segindex = SEGMENTBY_INDEX_FALSE;
					break;
				}
		}
		cscan->segindex = segindex;
	}

	/* To avoid a warning, we compare with the enum value. */
	Assert(segindex == SEGMENTBY_INDEX_TRUE || segindex == SEGMENTBY_INDEX_FALSE);
	return (segindex == SEGMENTBY_INDEX_TRUE);
}

/*
 * Return tuple for given TID via index scan.
 *
 * An index scan calls this function to fetch the "heap" tuple with the given
 * TID from the index.
 *
 * The TID points to a tuple either in the regular (non-compressed) or the
 * compressed relation. The data is fetched from the identified relation.
 *
 * If the index only indexes segmentby column(s), the index is itself
 * "compressed" and there is only one TID per compressed segment/tuple. In
 * that case, the "call_again" parameter is used to make sure the index scan
 * calls this function until all the rows in a compressed tuple is
 * returned. This "unwrapping" only happens in the case of segmentby indexes.
 */
static bool
hypercore_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot,
							TupleTableSlot *slot, bool *call_again, bool *all_dead)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	TupleTableSlot *child_slot;
	Relation rel = scan->rel;
	Relation crel = cscan->compr_rel;

	ItemPointerData decoded_tid;

	if (!is_compressed_tid(tid))
	{
		child_slot = arrow_slot_get_noncompressed_slot(slot);
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		bool result = rel->rd_tableam->index_fetch_tuple(cscan->uncompr_hscan,
														 tid,
														 snapshot,
														 child_slot,
														 call_again,
														 all_dead);
		rel->rd_tableam = oldtam;

		if (result)
		{
			slot->tts_tableOid = RelationGetRelid(scan->rel);
			ExecStoreArrowTuple(slot, InvalidTupleIndex);
		}

		cscan->return_count++;
		return result;
	}

	/* Compressed tuples not visible through this TAM when scanned by
	 * transparent decompression enabled since DecompressChunk already scanned
	 * that data. */
	if (ts_guc_enable_transparent_decompression == 2)
		return false;

	bool is_segmentby_index = is_segmentby_index_scan(cscan, slot);

	/* Fast path for segmentby index scans. If the compressed tuple is still
	 * being consumed, just increment the tuple index and return. */
	if (is_segmentby_index && cscan->call_again)
	{
		ExecStoreNextArrowTuple(slot);
		slot->tts_tableOid = RelationGetRelid(scan->rel);
		cscan->call_again = !arrow_slot_is_last(slot);
		*call_again = cscan->call_again || cscan->internal_call_again;
		cscan->return_count++;
		return true;
	}

	/* Recreate the original TID for the compressed table */
	uint16 tuple_index = hypercore_tid_decode(&decoded_tid, tid);
	Assert(tuple_index != InvalidTupleIndex);
	child_slot = arrow_slot_get_compressed_slot(slot, RelationGetDescr(cscan->compr_rel));

	/*
	 * Avoid decompression if the new TID from the index points to the same
	 * compressed tuple as the previous call to this function.
	 *
	 * There are cases, however, we're the index scan jumps between the same
	 * compressed tuples to get the right order, which will lead to
	 * decompressing the same compressed tuple multiple times. This happens,
	 * for example, when there's a segmentby column and orderby on
	 * time. Returning data in time order requires interleaving rows from two
	 * or more compressed tuples with different segmenby values. It is
	 * possible to optimize that case further by retaining a window/cache of
	 * decompressed tuples, keyed on TID.
	 */
	if (!TTS_EMPTY(child_slot) && !TTS_EMPTY(slot) && ItemPointerIsValid(&cscan->tid) &&
		ItemPointerEquals(&cscan->tid, &decoded_tid))
	{
		/* Still in the same compressed tuple, so just update tuple index and
		 * return the same Arrow slot */
		ExecStoreArrowTuple(slot, tuple_index);
		slot->tts_tableOid = RelationGetRelid(scan->rel);
		cscan->return_count++;
		return true;
	}

	bool result = crel->rd_tableam->index_fetch_tuple(cscan->compr_hscan,
													  &decoded_tid,
													  snapshot,
													  child_slot,
													  &cscan->internal_call_again,
													  all_dead);

	if (result)
	{
		slot->tts_tableOid = RelationGetRelid(scan->rel);
		ExecStoreArrowTuple(slot, tuple_index);
		/* Save the current compressed TID */
		ItemPointerCopy(&decoded_tid, &cscan->tid);
		cscan->num_decompressions++;

		if (is_segmentby_index)
		{
			Assert(tuple_index == 1);
			cscan->call_again = !arrow_slot_is_last(slot);
			*call_again = cscan->call_again || cscan->internal_call_again;
		}

		cscan->return_count++;
	}

	return result;
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for Hypercore
 * ------------------------------------------------------------------------
 */

static bool
hypercore_fetch_row_version(Relation relation, ItemPointer tid, Snapshot snapshot,
							TupleTableSlot *slot)
{
	bool result;
	uint16 tuple_index = InvalidTupleIndex;

	if (!is_compressed_tid(tid))
	{
		/*
		 * For non-compressed tuples, we fetch the tuple and copy it into the
		 * destination slot.
		 *
		 * We need to have a new slot for the call since the heap AM expects a
		 * BufferHeap TTS and we cannot pass down our Arrow TTS.
		 */
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		result = relation->rd_tableam->tuple_fetch_row_version(relation, tid, snapshot, child_slot);
		relation->rd_tableam = oldtam;
	}
	else
	{
		ItemPointerData decoded_tid;
		Relation child_rel = hypercore_open_compressed(relation, AccessShareLock);
		TupleTableSlot *child_slot =
			arrow_slot_get_compressed_slot(slot, RelationGetDescr(child_rel));

		tuple_index = hypercore_tid_decode(&decoded_tid, tid);
		result = table_tuple_fetch_row_version(child_rel, &decoded_tid, snapshot, child_slot);
		table_close(child_rel, NoLock);
	}

	if (result)
	{
		slot->tts_tableOid = RelationGetRelid(relation);
		ExecStoreArrowTuple(slot, tuple_index);
	}

	return result;
}

static bool
hypercore_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	HypercoreScanDescData *cscan = (HypercoreScanDescData *) scan;
	ItemPointerData ctid;

	if (!is_compressed_tid(tid))
	{
		Relation rel = scan->rs_rd;
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		bool valid = rel->rd_tableam->tuple_tid_valid(cscan->uscan_desc, tid);
		rel->rd_tableam = oldtam;
		return valid;
	}

	(void) hypercore_tid_decode(&ctid, tid);
	return cscan->compressed_rel->rd_tableam->tuple_tid_valid(cscan->cscan_desc, &ctid);
}

static bool
hypercore_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	bool result;

	if (is_compressed_tid(&slot->tts_tid))
	{
		Relation crel = hypercore_open_compressed(rel, AccessShareLock);
		TupleTableSlot *child_slot = arrow_slot_get_compressed_slot(slot, NULL);
		result = crel->rd_tableam->tuple_satisfies_snapshot(crel, child_slot, snapshot);
		table_close(crel, AccessShareLock);
	}
	else
	{
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		result = rel->rd_tableam->tuple_satisfies_snapshot(rel, child_slot, snapshot);
		rel->rd_tableam = oldtam;
	}
	return result;
}

/*
 * Determine which index tuples are safe to delete.
 *
 * The Index AM asks the Table AM about which given index tuples (as
 * referenced by TID) are safe to delete. Given that the array of TIDs to
 * delete ("delTIDs") may reference either the compressed or non-compressed
 * relation within Hypercore, it is necessary to split the information in the
 * TM_IndexDeleteOp in two: one for each relation. Then the operation can be
 * relayed to the standard heapAM method to do the heavy lifting for each
 * relation.
 *
 * In order to call the heapAM method on the compressed relation, it is
 * necessary to first "decode" the compressed TIDs to "normal" TIDs that
 * reference compressed tuples. A complication, however, is that multiple
 * distinct "compressed" TIDs may decode to the same TID, i.e., they reference
 * the same compressed tuple in the TAM's compressed relation, and the heapAM
 * method for index_delete_tuples() expects only unique TIDs. Therefore, it is
 * necessary to deduplicate TIDs before calling the heapAM method on the
 * compressed relation and then restore the result array of decoded delTIDs
 * after the method returns. Note that the returned delTID array might be
 * smaller than the input delTID array since only the TIDs that are safe to
 * delete should remain. Thus, if a decoded TID is not safe to delete, then
 * all compressed TIDs that reference that compressed tuple are also not safe
 * to delete.
 */
static TransactionId
hypercore_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	TM_IndexDeleteOp noncompr_delstate = *delstate;
	TM_IndexDeleteOp compr_delstate = *delstate;
	/* Hash table setup for TID deduplication */
	typedef struct TidEntry
	{
		ItemPointerData tid;
		List *tuple_indexes;
		List *status_indexes;
	} TidEntry;
	struct HASHCTL hctl = {
		.keysize = sizeof(ItemPointerData),
		.entrysize = sizeof(TidEntry),
		.hcxt = CurrentMemoryContext,
	};
	unsigned int total_knowndeletable_compressed = 0;
	unsigned int total_knowndeletable_non_compressed = 0;

	/*
	 * Setup separate TM_IndexDeleteOPs for the compressed and non-compressed
	 * relations. Note that it is OK to reference the original status array
	 * because it is accessed via the "id" index in the TM_IndexDelete struct,
	 * so it doesn't need the same length and order as the deltids array. This
	 * is because the deltids array is going to be sorted during processing
	 * anyway so the "same-array-index" mappings for the status and deltids
	 * arrays will be lost in any case.
	 */
	noncompr_delstate.deltids = palloc(sizeof(TM_IndexDelete) * delstate->ndeltids);
	noncompr_delstate.ndeltids = 0;
	compr_delstate.deltids = palloc(sizeof(TM_IndexDelete) * delstate->ndeltids);
	compr_delstate.ndeltids = 0;

	/* Hash table to deduplicate compressed TIDs that point to the same
	 * compressed tuple */
	HTAB *tidhash = hash_create("IndexDelete deduplication",
								delstate->ndeltids,
								&hctl,
								HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	/*
	 * Stage 1: preparation.
	 *
	 * Split the deltids array based on the two relations and deduplicate
	 * compressed TIDs at the same time. When deduplicating, it is necessary
	 * to "remember" the lost information when decoding (e.g., index into a
	 * compressed tuple).
	 */
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		const TM_IndexDelete *deltid = &delstate->deltids[i];
		const TM_IndexStatus *status = &delstate->status[deltid->id];

		/* If this is a compressed TID, decode and deduplicate
		 * first. Otherwise just add to the non-compressed deltids array */
		if (is_compressed_tid(&deltid->tid))
		{
			ItemPointerData decoded_tid;
			bool found;
			TidEntry *tidentry;
			uint16 tuple_index;

			tuple_index = hypercore_tid_decode(&decoded_tid, &deltid->tid);
			tidentry = hash_search(tidhash, &decoded_tid, HASH_ENTER, &found);

			if (status->knowndeletable)
				total_knowndeletable_compressed++;

			if (!found)
			{
				/* Add to compressed IndexDelete array */
				TM_IndexDelete *deltid_compr = &compr_delstate.deltids[compr_delstate.ndeltids];
				deltid_compr->id = deltid->id;
				ItemPointerCopy(&decoded_tid, &deltid_compr->tid);

				/* Remember the information for the compressed TID so that the
				 * deltids array can be restored later */
				tidentry->tuple_indexes = list_make1_int(tuple_index);
				tidentry->status_indexes = list_make1_int(deltid->id);
				compr_delstate.ndeltids++;
			}
			else
			{
				/* Duplicate TID, so just append info that needs to be remembered */
				tidentry->tuple_indexes = lappend_int(tidentry->tuple_indexes, tuple_index);
				tidentry->status_indexes = lappend_int(tidentry->status_indexes, deltid->id);
			}
		}
		else
		{
			TM_IndexDelete *deltid_noncompr =
				&noncompr_delstate.deltids[noncompr_delstate.ndeltids];

			*deltid_noncompr = *deltid;
			noncompr_delstate.ndeltids++;

			if (status->knowndeletable)
				total_knowndeletable_non_compressed++;
		}
	}

	Assert((total_knowndeletable_non_compressed + total_knowndeletable_compressed) > 0 ||
		   delstate->bottomup);

	/*
	 * Stage 2: call heapAM method for each relation and recreate the deltids
	 * array with the result.
	 *
	 * The heapAM method implements various assumptions and asserts around the
	 * contents of the deltids array depending on whether the index AM is
	 * doing simple index tuple deletion or bottom up deletion (as indicated
	 * by delstate->bottomup). For example, in the simple index deletion case,
	 * it seems the deltids array should have at least have one known
	 * deletable entry or otherwise the heapAM might prune the array to zero
	 * length which leads to an assertion failure because it can only be zero
	 * length in the bottomup case. Since we split the original deltids array
	 * across the compressed and non-compressed relations, we might end up in
	 * a situation where we call one relation without any knowndeletable TIDs
	 * in the simple deletion case, leading to an assertion
	 * failure. Therefore, only call heapAM if there is at least one
	 * knowndeletable or we are doing bottomup deletion.
	 *
	 * Note, also, that the function should return latestRemovedXid
	 * transaction ID, so need to remember those for each call and then return
	 * the latest removed of those.
	 */
	TransactionId xid_noncompr = InvalidTransactionId;
	TransactionId xid_compr = InvalidTransactionId;

	/* Reset the deltids array before recreating it with the result */
	delstate->ndeltids = 0;

	if (noncompr_delstate.ndeltids > 0 &&
		(total_knowndeletable_non_compressed > 0 || delstate->bottomup))
	{
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		xid_noncompr = rel->rd_tableam->index_delete_tuples(rel, &noncompr_delstate);
		rel->rd_tableam = oldtam;
		memcpy(delstate->deltids,
			   noncompr_delstate.deltids,
			   noncompr_delstate.ndeltids * sizeof(TM_IndexDelete));
		delstate->ndeltids = noncompr_delstate.ndeltids;
	}

	if (compr_delstate.ndeltids > 0 && (total_knowndeletable_compressed > 0 || delstate->bottomup))
	{
		/* Assume RowExclusivelock since this involves deleting tuples */
		Relation compr_rel = hypercore_open_compressed(rel, RowExclusiveLock);

		xid_compr = compr_rel->rd_tableam->index_delete_tuples(compr_rel, &compr_delstate);

		for (int i = 0; i < compr_delstate.ndeltids; i++)
		{
			const TM_IndexDelete *deltid_compr = &compr_delstate.deltids[i];
			const TM_IndexStatus *status_compr = &delstate->status[deltid_compr->id];
			ListCell *lc_id, *lc_tupindex;
			TidEntry *tidentry;
			bool found;

			tidentry = hash_search(tidhash, &deltid_compr->tid, HASH_FIND, &found);

			Assert(found);

			forboth (lc_id, tidentry->status_indexes, lc_tupindex, tidentry->tuple_indexes)
			{
				int id = lfirst_int(lc_id);
				uint16 tuple_index = lfirst_int(lc_tupindex);
				TM_IndexDelete *deltid = &delstate->deltids[delstate->ndeltids];
				TM_IndexStatus *status = &delstate->status[deltid->id];

				deltid->id = id;
				/* Assume that all index tuples pointing to the same heap
				 * compressed tuple are deletable if one is
				 * deletable. Otherwise leave status as before. */
				if (status_compr->knowndeletable)
					status->knowndeletable = true;

				hypercore_tid_encode(&deltid->tid, &deltid_compr->tid, tuple_index);
				delstate->ndeltids++;
			}
		}

		table_close(compr_rel, NoLock);
	}

	hash_destroy(tidhash);
	pfree(compr_delstate.deltids);
	pfree(noncompr_delstate.deltids);

#ifdef USE_ASSERT_CHECKING
	do
	{
		int ndeletable = 0;

		for (int i = 0; i < delstate->ndeltids; i++)
		{
			const TM_IndexDelete *deltid = &delstate->deltids[i];
			const TM_IndexStatus *status = &delstate->status[deltid->id];

			if (status->knowndeletable)
				ndeletable++;
		}

		Assert(ndeletable > 0 || delstate->ndeltids == 0);
	} while (0);
#endif

	/* Return the latestremovedXid. TransactionIdFollows can handle
	 * InvalidTransactionid. */
	return TransactionIdFollows(xid_noncompr, xid_compr) ? xid_noncompr : xid_compr;
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for Hypercore.
 * ----------------------------------------------------------------------------
 */

typedef struct ConversionState
{
	Oid relid;
	RelationSize before_size;
	Tuplesortstate *tuplesortstate;
	MemoryContext mcxt;
	MemoryContextCallback cb;
} ConversionState;

static ConversionState *conversionstate = NULL;

static void
hypercore_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid, int options,
					   BulkInsertStateData *bistate)
{
	if (conversionstate)
	{
		if (conversionstate->tuplesortstate)
		{
			tuplesort_puttupleslot(conversionstate->tuplesortstate, slot);
			return;
		}

		/* If no tuplesortstate is set, conversion is happening from legacy
		 * compression where a compressed relation already exists. Therefore,
		 * there is no need to recompress; just insert the non-compressed data
		 * into the new heap. */
	}

	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->tuple_insert(relation, slot, cid, options, bistate);
	relation->rd_tableam = oldtam;

	MemoryContext oldmcxt = MemoryContextSwitchTo(CurTransactionContext);
	partially_compressed_relids =
		list_append_unique_oid(partially_compressed_relids, RelationGetRelid(relation));
	MemoryContextSwitchTo(oldmcxt);
}

static void
hypercore_tuple_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
								   int options, BulkInsertStateData *bistate, uint32 specToken)
{
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam
		->tuple_insert_speculative(relation, slot, cid, options, bistate, specToken);
	relation->rd_tableam = oldtam;
}

static void
hypercore_tuple_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken,
									 bool succeeded)
{
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->tuple_complete_speculative(relation, slot, specToken, succeeded);
	relation->rd_tableam = oldtam;
}

/*
 * WholeSegmentDeleteState is used to enforce the invariant that only whole
 * compressed segments can be deleted. See the delete handler function below
 * for more information.
 */
typedef struct WholeSegmentDeleteState
{
	ItemPointerData ctid;	  /* Original TID of compressed tuple (decoded) */
	CommandId cid;			  /* Command ID for the query doing the deletion */
	int32 count;			  /* The number of values/rows in compressed tuple */
	Bitmapset *tuple_indexes; /* The values/rows of the compressed tuple deleted so far */
	MemoryContextCallback end_of_query_cb;
	MemoryContext mcxt;
} WholeSegmentDeleteState;

static WholeSegmentDeleteState *delete_state = NULL;

static bool
whole_segment_delete_state_clear(void)
{
	if (delete_state)
	{
		/* Only reset the global pointer to indicate this delete state is
		 * reset. The actual memory is freed when the PortalContext is
		 * reset */
		delete_state = NULL;
		return true;
	}
	return false;
}

#define RAISE_DELETION_ERROR()                                                                     \
	ereport(ERROR,                                                                                 \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),                                               \
			 errmsg("only whole-segment deletes are possible on compressed data"),                 \
			 errhint("Try deleting based on segment_by key.")));

/*
 * Callback invoked at the end of a query (command).
 *
 * Ensure that the query only deleted whole segments of compressed
 * data. Otherwise, raise an error.
 *
 * The callback is attached to the PortalContext memory context which is
 * always cleared at the end of a query.
 */
static void
whole_segment_delete_callback(void *arg)
{
	/* Clear delete state, but only raise error if we aren't already aborted */
	if (whole_segment_delete_state_clear() && IsTransactionState())
		RAISE_DELETION_ERROR();
}

/*
 * Create a new delete state.
 *
 * Construct the delete state and tie it to the current query via the
 * PortalContext's callback. This context is reset at the end of a query,
 * which is a good point to check that delete invariants hold.
 */
static WholeSegmentDeleteState *
whole_segment_delete_state_create(Relation rel, Relation crel, CommandId cid, ItemPointer ctid)
{
	WholeSegmentDeleteState *state;
	HeapTupleData tp;
	Page page;
	BlockNumber block;
	Buffer buffer;
	ItemId lp;
	bool isnull;
	Datum d;

	state = MemoryContextAllocZero(PortalContext, sizeof(WholeSegmentDeleteState));
	state->mcxt = PortalContext;
	state->end_of_query_cb.func = whole_segment_delete_callback;
	ItemPointerCopy(ctid, &state->ctid);
	state->cid = cid;
	MemoryContextRegisterResetCallback(state->mcxt, &state->end_of_query_cb);

	/* Need to construct a tuple in order to read out the "count" from the
	 * compressed segment */
	block = ItemPointerGetBlockNumber(ctid);
	buffer = ReadBuffer(crel, block);
	page = BufferGetPage(buffer);

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	lp = PageGetItemId(page, ItemPointerGetOffsetNumber(ctid));
	Assert(ItemIdIsNormal(lp));

	tp.t_tableOid = RelationGetRelid(crel);
	tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	tp.t_len = ItemIdGetLength(lp);
	tp.t_self = *ctid;

	d = heap_getattr(&tp,
					 RelationGetHypercoreInfo(rel)->count_cattno,
					 RelationGetDescr(crel),
					 &isnull);
	state->count = DatumGetInt32(d);
	UnlockReleaseBuffer(buffer);

	return state;
}

static void
whole_segment_delete_state_add_row(WholeSegmentDeleteState *state, uint16 tuple_index)
{
	MemoryContext oldmcxt = MemoryContextSwitchTo(state->mcxt);
	state->tuple_indexes = bms_add_member(state->tuple_indexes, tuple_index);
	MemoryContextSwitchTo(oldmcxt);
}

/*
 * Check if a delete violates the "whole segment" invariant.
 *
 * The function will keep accumulating deleted TIDs as long as the following
 * holds:
 *
 * 1. The delete is part of a segment that is the same segment as the previous delete.
 * 2. The command ID is the same as the previous delete (i.e., still in same query).
 * 3. The segment still contains rows that haven't been deleted.
 *
 * The function raises an error if any of 1 or 2 above is violated.
 *
 * Returns true if the whole segment has been deleted, otherwise false.
 */
static bool
is_whole_segment_delete(Relation rel, Relation crel, CommandId cid, ItemPointer ctid,
						uint16 tuple_index)
{
	if (delete_state == NULL)
		delete_state = whole_segment_delete_state_create(rel, crel, cid, ctid);

	/* Check if any invariant is violated */
	if (delete_state->cid != cid || !ItemPointerEquals(&delete_state->ctid, ctid))
	{
		whole_segment_delete_state_clear();
		RAISE_DELETION_ERROR();
	}

	whole_segment_delete_state_add_row(delete_state, tuple_index);

	/* Check if the whole segment is deleted. If so, cleanup. */
	bool is_whole_segment = bms_num_members(delete_state->tuple_indexes) == delete_state->count;

	if (is_whole_segment)
		whole_segment_delete_state_clear();

	return is_whole_segment;
}

/*
 * Delete handler function.
 *
 * The TAM delete handler is invoked for individual rows referenced by TID,
 * and these TIDs can point to either non-compressed data or into a compressed
 * segment tuple. For TIDs pointing to non-compressed data, the row can be
 * deleted directly. However, a TID pointing into a compressed tuple cannot
 * lead to a delete of the whole compressed tuple unless also all the other
 * rows in it should be deleted.
 *
 * It is tempting to simply disallow deletes directly on compressed
 * data. However, Hypercore needs to support such deletes in some cases, for
 * example, to support foreign key cascading deletes.
 *
 * Fortunately, some deletes of compressed data can be supported as long as
 * the delete involves all rows in a compressed segment.
 *
 * The WholeSegmentDeleteState is used to track that this invariant is not
 * violated.
 */
static TM_Result
hypercore_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot snapshot,
					   Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart)
{
	TM_Result result = TM_Ok;

	if (is_compressed_tid(tid) && hypercore_truncate_compressed)
	{
		Relation crel = hypercore_open_compressed(relation, RowExclusiveLock);
		ItemPointerData decoded_tid;
		uint16 tuple_index = hypercore_tid_decode(&decoded_tid, tid);

		/*
		 * It is only possible to delete the compressed segment if all rows in
		 * it are deleted. Note that we need to fetch the caminfo again here
		 * since it could have been invalidated by a table_open() call.
		 */
		if (is_whole_segment_delete(relation, crel, cid, &decoded_tid, tuple_index))
		{
			result = crel->rd_tableam->tuple_delete(crel,
													&decoded_tid,
													cid,
													snapshot,
													crosscheck,
													wait,
													tmfd,
													changingPart);

			if (result == TM_SelfModified)
			{
				/* The compressed tuple was already deleted by other means in
				 * the same transaction. This can happen because compression
				 * DML implemented the optimization to delete whole compressed
				 * segments after whole-segment deletes were implemented in
				 * the TAM. Trying to delete again should not hurt, and if it
				 * is already deleted, we ignore it. */
				result = TM_Ok;
			}
		}
		table_close(crel, NoLock);
	}
	else
	{
		/* Just pass this on to regular heap AM */
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		result =
			relation->rd_tableam
				->tuple_delete(relation, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart);
		relation->rd_tableam = oldtam;
	}

	return result;
}

/*
 * Decompress a segment that contains the row given by ctid.
 *
 * This function is called during an upsert (ON CONFLICT DO UPDATE), where the
 * conflicting row points to a compressed segment that needs to be
 * decompressed before the update can take place. This function is used to
 * decompress that segment into a set of individual rows and insert them into
 * the non-compressed region.
 *
 * Returns the number of rows in the segment that were decompressed, or 0 if
 * the TID pointed to a regular (non-compressed) tuple. If any rows are
 * decompressed, the TID of the de-compressed conflicting row is returned via
 * "new_ctid". If no rows were decompressed, the value of "new_ctid" is
 * undefined.
 */
int
hypercore_decompress_update_segment(Relation relation, const ItemPointer ctid, TupleTableSlot *slot,
									Snapshot snapshot, ItemPointer new_ctid)
{
	Relation crel;
	TupleTableSlot *cslot;
	ItemPointerData decoded_tid;
	TM_Result result;
	TM_FailureData tmfd;
	int n_batch_rows = 0;
	uint16 tuple_index;
	bool should_free;
	CommandId cid = GetCurrentCommandId(true);

	/* Nothing to do if this is not a compressed segment */
	if (!is_compressed_tid(ctid))
		return 0;

	Assert(TTS_IS_ARROWTUPLE(slot));
	Assert(!TTS_EMPTY(slot));
	Assert(ItemPointerEquals(ctid, &slot->tts_tid));

	crel = hypercore_open_compressed(relation, RowExclusiveLock);
	tuple_index = hypercore_tid_decode(&decoded_tid, ctid);
	cslot = arrow_slot_get_compressed_slot(slot, NULL);
	HeapTuple tuple = ExecFetchSlotHeapTuple(cslot, false, &should_free);

	RowDecompressor decompressor =
		build_decompressor(RelationGetDescr(crel), RelationGetDescr(relation));
	BulkWriter writer = bulk_writer_build(relation, 0);

	heap_deform_tuple(tuple,
					  RelationGetDescr(crel),
					  decompressor.compressed_datums,
					  decompressor.compressed_is_nulls);

	/* Must delete the segment before calling the decompression function below
	 * or otherwise index updates will lead to conflicts */
	result = table_tuple_delete(crel,
								&cslot->tts_tid,
								cid,
								snapshot,
								InvalidSnapshot,
								true,
								&tmfd,
								false);

	Ensure(result == TM_Ok, "could not delete compressed segment, result: %u", result);

	n_batch_rows = row_decompressor_decompress_row_to_table(&decompressor, &writer);
	/* Return the TID of the decompressed conflicting tuple. Tuple index is
	 * 1-indexed, so subtract 1. */
	slot = decompressor.decompressed_slots[tuple_index - 1];
	ItemPointerCopy(&slot->tts_tid, new_ctid);

	/* Need to make decompressed (and deleted segment) visible */
	CommandCounterIncrement();
	row_decompressor_close(&decompressor);
	bulk_writer_close(&writer);
	table_close(crel, NoLock);

	return n_batch_rows;
}

#if PG16_LT
typedef bool TU_UpdateIndexes;
#endif

static TM_Result
hypercore_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
					   Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
					   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	if (!is_compressed_tid(otid))
	{
		/* Just pass this on to regular heap AM */
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		TM_Result result = relation->rd_tableam->tuple_update(relation,
															  otid,
															  slot,
															  cid,
															  snapshot,
															  crosscheck,
															  wait,
															  tmfd,
															  lockmode,
															  update_indexes);
		relation->rd_tableam = oldtam;
		return result;
	}

	/* This shouldn't happen because hypertable_modify should have
	 * decompressed the data to be deleted already. It can happen, however, if
	 * UPDATE is run directly on a hypertable chunk, because that case isn't
	 * handled in the current code for DML on compressed chunks. */
	elog(ERROR, "cannot update compressed tuple");

	return TM_Ok;
}

static TM_Result
hypercore_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
					 CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags,
					 TM_FailureData *tmfd)
{
	TM_Result result;

	if (is_compressed_tid(tid))
	{
		/* SELECT FOR UPDATE takes RowShareLock, so assume this
		 * lockmode. Another option to consider is take same lock as currently
		 * held on the non-compressed relation */
		Relation crel = hypercore_open_compressed(relation, RowShareLock);
		TupleTableSlot *child_slot = arrow_slot_get_compressed_slot(slot, RelationGetDescr(crel));
		ItemPointerData decoded_tid;

		uint16 tuple_index = hypercore_tid_decode(&decoded_tid, tid);
		result = crel->rd_tableam->tuple_lock(crel,
											  &decoded_tid,
											  snapshot,
											  child_slot,
											  cid,
											  mode,
											  wait_policy,
											  flags,
											  tmfd);

		if (result == TM_Ok)
		{
			slot->tts_tableOid = RelationGetRelid(relation);
			ExecStoreArrowTuple(slot, tuple_index);
		}

		table_close(crel, NoLock);
	}
	else
	{
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		result = relation->rd_tableam->tuple_lock(relation,
												  tid,
												  snapshot,
												  child_slot,
												  cid,
												  mode,
												  wait_policy,
												  flags,
												  tmfd);
		relation->rd_tableam = oldtam;

		if (result == TM_Ok)
		{
			slot->tts_tableOid = RelationGetRelid(relation);
			ExecStoreArrowTuple(slot, InvalidTupleIndex);
		}
	}

	return result;
}

static void
hypercore_finish_bulk_insert(Relation rel, int options)
{
	if (conversionstate)
		convert_to_hypercore_finish(RelationGetRelid(rel));
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for Hypercore.
 * ------------------------------------------------------------------------
 */

#if PG16_LT
/* Account for API differences in pre-PG16 versions */
typedef RelFileNode RelFileLocator;
#define relation_set_new_filelocator relation_set_new_filenode
#endif

static void
hypercore_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator,
									   char persistence, TransactionId *freezeXid,
									   MultiXactId *minmulti)
{
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
#if PG16_GE
	rel->rd_tableam->relation_set_new_filelocator(rel,
												  newrlocator,
												  persistence,
												  freezeXid,
												  minmulti);
#else
	rel->rd_tableam->relation_set_new_filenode(rel, newrlocator, persistence, freezeXid, minmulti);
#endif
	rel->rd_tableam = oldtam;

	/* If the chunk has a compressed chunk associated with it, then we need to
	 * change the rel file number for it as well. This can happen if you, for
	 * example, execute a transactional TRUNCATE. */
	const CompressionSettings *settings = ts_compression_settings_get(RelationGetRelid(rel));

	if (settings && OidIsValid(settings->fd.compress_relid) && hypercore_truncate_compressed)
	{
		Relation compressed_rel = table_open(settings->fd.compress_relid, AccessExclusiveLock);
#if PG16_GE
		RelationSetNewRelfilenumber(compressed_rel, compressed_rel->rd_rel->relpersistence);
#else
		RelationSetNewRelfilenode(compressed_rel, compressed_rel->rd_rel->relpersistence);
#endif
		table_close(compressed_rel, NoLock);
	}
}

static void
hypercore_relation_nontransactional_truncate(Relation rel)
{
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	const CompressionSettings *settings = ts_compression_settings_get(RelationGetRelid(rel));

	rel->rd_tableam->relation_nontransactional_truncate(rel);
	rel->rd_tableam = oldtam;

	if (settings && OidIsValid(settings->fd.compress_relid) && hypercore_truncate_compressed)
	{
		Relation crel = table_open(settings->fd.compress_relid, AccessShareLock);
		crel->rd_tableam->relation_nontransactional_truncate(crel);
		table_close(crel, NoLock);
	}
}

static void
hypercore_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	FEATURE_NOT_SUPPORTED;
}

static void
on_compression_progress(RowCompressor *rowcompress, uint64 ntuples)
{
	pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN, ntuples);
}

/*
 * Rewrite a relation and compress at the same time.
 *
 * Note that all tuples are frozen when compressed to make sure they are
 * visible to concurrent transactions after the rewrite. This isn't MVCC
 * compliant and does not work for isolation levels of repeatable read or
 * higher. Ideally, we should check visibility of each original tuple that we
 * roll up into a compressed tuple and transfer visibility information (XID)
 * based on that, just like done in heap when it is using a rewrite state.
 */
static Oid
compress_and_swap_heap(Relation rel, Tuplesortstate *tuplesort, TransactionId *xid_cutoff,
					   MultiXactId *multi_cutoff)
{
	Oid old_compressed_relid = RelationGetHypercoreInfo(rel)->compressed_relid;
	const CompressionSettings *settings = ts_compression_settings_get(RelationGetRelid(rel));
	Relation old_compressed_rel = hypercore_open_compressed(rel, AccessExclusiveLock);
	Oid accessMethod = old_compressed_rel->rd_rel->relam;
	Oid tableSpace = old_compressed_rel->rd_rel->reltablespace;
	char relpersistence = old_compressed_rel->rd_rel->relpersistence;
	Oid new_compressed_relid = make_new_heap(old_compressed_relid,
											 tableSpace,
											 accessMethod,
											 relpersistence,
											 AccessExclusiveLock);
	Relation new_compressed_rel = table_open(new_compressed_relid, AccessExclusiveLock);
	RowCompressor row_compressor;
	BulkWriter writer;
	double reltuples;
	int32 relpages;

	/* Initialize the compressor. */
	Assert(settings->fd.relid == RelationGetRelid(rel));
	row_compressor_init(&row_compressor,
						settings,
						RelationGetDescr(rel),
						RelationGetDescr(new_compressed_rel));

	writer = bulk_writer_build(new_compressed_rel, HEAP_INSERT_FROZEN);
	row_compressor.on_flush = on_compression_progress;
	row_compressor_append_sorted_rows(&row_compressor, tuplesort, old_compressed_rel, &writer);
	reltuples = row_compressor.num_compressed_rows;
	relpages = RelationGetNumberOfBlocks(new_compressed_rel);
	row_compressor_close(&row_compressor);
	bulk_writer_close(&writer);

	table_close(new_compressed_rel, NoLock);
	table_close(old_compressed_rel, NoLock);

	/*
	 * Update stats for the compressed relation.
	 *
	 * We have an AccessExclusivelock from above so no tuple lock is needed
	 * during update of the pg_class catalog table.
	 */
	AssertSufficientPgClassUpdateLockHeld(new_compressed_relid);

	Relation relRelation = table_open(RelationRelationId, RowExclusiveLock);
	HeapTuple reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(new_compressed_relid));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", new_compressed_relid);
	Form_pg_class relform = (Form_pg_class) GETSTRUCT(reltup);

	relform->relpages = relpages;
	relform->reltuples = reltuples;

	CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);

	/* Clean up. */
	heap_freetuple(reltup);
	table_close(relRelation, RowExclusiveLock);

	/* Make the update visible */
	CommandCounterIncrement();

	/* Finish the heap swap for the compressed relation. Note that it is not
	 * possible to swap toast content since new tuples were generated via
	 * compression. */
	finish_heap_swap(old_compressed_relid,
					 new_compressed_relid,
					 false /* is_system_catalog */,
					 false /* swap_toast_by_content */,
					 false,
					 true,
					 *xid_cutoff,
					 *multi_cutoff,
					 relpersistence);

	return new_compressed_relid;
}

/*
 * Rewrite/compress the relation for CLUSTER or VACUUM FULL.
 *
 * The copy_for_cluster() callback is called during a CLUSTER or VACUUM FULL,
 * and performs a heap swap/rewrite. The code is based on the heap's
 * copy_for_cluster(), with changes to handle two heaps and compressed tuples.
 *
 * For Hypercore, two heap swaps are performed: one on the non-compressed
 * (user-visible) relation, which is managed by PostgreSQL and passed on to
 * this callback, and one on the compressed relation that is implemented
 * within the callback.
 *
 * The Hypercore implementation of copy_for_cluster() is similar to the one
 * for Heap. However, instead of "rewriting" tuples into the new heap (while
 * at the same time handling freezing and visibility), Hypercore will
 * compress all the data and write it to a new compressed relation. Since the
 * compression is based on the previous compression implementation, visibility
 * of recently deleted tuples and freezing of tuples is not correctly handled,
 * at least not for higher isolation levels than read committed. Changes to
 * handle higher isolation levels should be considered in a future update of
 * this code.
 *
 * Some things missing includes the handling of recently dead tuples that need
 * to be transferred to the new heap since they might still be visible to some
 * ongoing transactions. PostgreSQL's heap implementation handles this via the
 * heap rewrite module. It should also be possible to write frozen compressed
 * tuples if all rows it compresses are also frozen.
 */
static void
hypercore_relation_copy_for_cluster(Relation OldHypercore, Relation NewCompression,
									Relation OldIndex, bool use_sort, TransactionId OldestXmin,
									TransactionId *xid_cutoff, MultiXactId *multi_cutoff,
									double *num_tuples, double *tups_vacuumed,
									double *tups_recently_dead)
{
	HypercoreScanDesc cscan;
	HeapScanDesc chscan;
	HeapScanDesc uhscan;
	Tuplesortstate *tuplesort;
	TableScanDesc tscan;
	TupleTableSlot *slot;
	ArrowTupleTableSlot *aslot;
	BufferHeapTupleTableSlot *hslot;
	BlockNumber prev_cblock = InvalidBlockNumber;
	BlockNumber startblock;
	BlockNumber nblocks;

	if (ts_is_hypertable(RelationGetRelid(OldHypercore)))
		return;

	check_guc_setting_compatible_with_scan();

	/* Error out if this is a CLUSTER. It would be possible to CLUSTER only
	 * the non-compressed relation, but utility of this is questionable as
	 * most of the data should be compressed (and ordered) anyway. */
	if (OldIndex != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster a hypercore table"),
				 errdetail("A hypercore table is already ordered by compression.")));

	CompressionSettings *settings = ts_compression_settings_get(RelationGetRelid(OldHypercore));
	tuplesort = compression_create_tuplesort_state(settings, OldHypercore);

	/* In scan-and-sort mode and also VACUUM FULL, set phase */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP);

	/* This will scan via the Hypercore callbacks, getting tuples from both
	 * compressed and non-compressed relations */
	tscan = table_beginscan(OldHypercore, SnapshotAny, 0, (ScanKey) NULL);
	cscan = (HypercoreScanDesc) tscan;
	chscan = (HeapScanDesc) cscan->cscan_desc;
	uhscan = (HeapScanDesc) cscan->uscan_desc;
	slot = table_slot_create(OldHypercore, NULL);
	startblock = chscan->rs_startblock + uhscan->rs_startblock;
	nblocks = chscan->rs_nblocks + uhscan->rs_nblocks;

	/* Set total heap blocks */
	pgstat_progress_update_param(PROGRESS_CLUSTER_TOTAL_HEAP_BLKS, nblocks);

	aslot = (ArrowTupleTableSlot *) slot;

	for (;;)
	{
		HeapTuple tuple;
		Buffer buf;
		bool isdead;
		BlockNumber cblock;

		CHECK_FOR_INTERRUPTS();

		if (!table_scan_getnextslot(tscan, ForwardScanDirection, slot))
		{
			/*
			 * If the last pages of the scan were empty, we would go to
			 * the next phase while heap_blks_scanned != heap_blks_total.
			 * Instead, to ensure that heap_blks_scanned is equivalent to
			 * total_heap_blks after the table scan phase, this parameter
			 * is manually updated to the correct value when the table
			 * scan finishes.
			 */
			pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_BLKS_SCANNED, nblocks);
			break;
		}
		/*
		 * In scan-and-sort mode and also VACUUM FULL, set heap blocks
		 * scanned
		 *
		 * Note that heapScan may start at an offset and wrap around, i.e.
		 * rs_startblock may be >0, and rs_cblock may end with a number
		 * below rs_startblock. To prevent showing this wraparound to the
		 * user, we offset rs_cblock by rs_startblock (modulo rs_nblocks).
		 */
		cblock = chscan->rs_cblock + uhscan->rs_cblock;

		if (prev_cblock != cblock)
		{
			pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_BLKS_SCANNED,
										 ((cblock + nblocks - startblock) % nblocks) + 1);
			prev_cblock = cblock;
		}
		/* Get the actual tuple from the child slot (either compressed or
		 * non-compressed). The tuple has all the visibility information. */
		tuple = ExecFetchSlotHeapTuple(aslot->child_slot, false, NULL);
		hslot = (BufferHeapTupleTableSlot *) aslot->child_slot;

		buf = hslot->buffer;

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		switch (HeapTupleSatisfiesVacuum(tuple, OldestXmin, buf))
		{
			case HEAPTUPLE_DEAD:
				/* Definitely dead */
				isdead = true;
				break;
			case HEAPTUPLE_RECENTLY_DEAD:
				/* Note: This case is treated as "dead" in Hypercore,
				 * although some of these tuples might still be visible to
				 * some transactions. For strict correctness, recently dead
				 * tuples should be transferred to the new heap if they are
				 * still visible to some transactions (e.g. under repeatable
				 * read). However, this is tricky since multiple rows with
				 * potentially different visibility is rolled up into one
				 * compressed row with singular visibility. */
				isdead = true;
				break;
			case HEAPTUPLE_LIVE:
				/* Live or recently dead, must copy it */
				isdead = false;
				break;
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * Since we hold exclusive lock on the relation, normally the
				 * only way to see this is if it was inserted earlier in our
				 * own transaction.  However, it can happen in system
				 * catalogs, since we tend to release write lock before commit
				 * there. Still, system catalogs don't use Hypercore.
				 */
				if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple->t_data)))
					elog(WARNING,
						 "concurrent insert in progress within table \"%s\"",
						 RelationGetRelationName(OldHypercore));
				/* treat as live */
				isdead = false;
				break;
			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * Similar situation to INSERT_IN_PROGRESS case.
				 */
				if (!TransactionIdIsCurrentTransactionId(
						HeapTupleHeaderGetUpdateXid(tuple->t_data)))
					elog(WARNING,
						 "concurrent delete in progress within table \"%s\"",
						 RelationGetRelationName(OldHypercore));
				/* Note: This case is treated as "dead" in Hypercore,
				 * although this is "recently dead" in heap */
				isdead = true;
				break;
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				isdead = false; /* keep compiler quiet */
				break;
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (isdead)
		{
			*tups_vacuumed += 1;

			/* Skip whole segment if a dead compressed tuple */
			if (arrow_slot_is_compressed(slot))
				arrow_slot_mark_consumed(slot);
			continue;
		}

		while (!arrow_slot_is_last(slot))
		{
			*num_tuples += 1;
			tuplesort_puttupleslot(tuplesort, slot);
			ExecStoreNextArrowTuple(slot);
		}

		*num_tuples += 1;
		tuplesort_puttupleslot(tuplesort, slot);

		/* Report increase in number of tuples scanned */
		pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED, *num_tuples);
	}

	table_endscan(tscan);
	ExecDropSingleTupleTableSlot(slot);

	/* Report that we are now sorting tuples */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SORT_TUPLES);

	/* Sort and recreate compressed relation */
	tuplesort_performsort(tuplesort);

	/* Report that we are now writing new heap */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP);

	compress_and_swap_heap(OldHypercore, tuplesort, xid_cutoff, multi_cutoff);
	tuplesort_end(tuplesort);
}

/*
 * VACUUM (not VACUUM FULL).
 *
 * Vacuum the hypercore by calling vacuum on both the non-compressed and
 * compressed relations.
 *
 * Indexes on a heap are normally vacuumed as part of vacuuming the
 * heap. However, a hypercore index is defined on the non-compressed relation
 * and contains tuples from both the non-compressed and compressed relations
 * and therefore dead tuples vacuumed on the compressed relation won't be
 * removed from a hypercore index by default. The vacuuming of dead
 * compressed tuples from the hypercore index therefore requires special
 * handling, which is triggered via a proxy index (hypercore_proxy) that relays the
 * clean up to the "correct" hypercore indexes. (See hypercore_proxy.c)
 *
 * For future: It would make sense to (re-)compress all non-compressed data as
 * part of vacuum since (re-)compression is a kind of cleanup but also leaves
 * a lot of garbage.
 */
static void
hypercore_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	Oid relid = RelationGetRelid(rel);
	RelStats relstats;
	VacuumParams cparams;

	memcpy(&cparams, params, sizeof(cparams));
	relstats_fetch(relid, &relstats);

	/* Vacuum the non-compressed relation */
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->relation_vacuum(rel, params, bstrategy);
	rel->rd_tableam = oldtam;

	/*
	 * The parent table doesn't hold any data, but it still needs to be
	 * vacuumed to advance relfrozenxid. It doesn't have any compressed data,
	 * so that part can be skipped.
	 */
	if (ts_is_hypertable(relid))
		return;

	LOCKMODE lmode =
		(params->options & VACOPT_FULL) ? AccessExclusiveLock : ShareUpdateExclusiveLock;

	/* Vacuum the compressed relation */
	Relation crel = vacuum_open_relation(RelationGetHypercoreInfo(rel)->compressed_relid,
										 NULL,
										 cparams.options,
										 cparams.log_min_duration >= 0,
										 lmode);

	if (crel)
	{
		crel->rd_tableam->relation_vacuum(crel, &cparams, bstrategy);
		table_close(crel, NoLock);
	}

	/* Unfortunately, relstats are currently incorrectly updated when
	 * vacuuming, because we vacuum the non-compressed rel separately, and, it
	 * will only update stats based on the data in that table. Therefore, as a
	 * work-around, it is better to restore relstats to what it was before
	 * vacuuming.
	 */
	relstats_update(relid, &relstats);
}

/*
 * Analyze the next block with the given blockno.
 *
 * The underlying ANALYZE functionality that calls this function samples
 * blocks in the relation. To be able to analyze all the blocks across both
 * the non-compressed and the compressed relations, we need to make sure that
 * both underlying relations are sampled.
 *
 * For versions before PG17, this function relies on the TAM giving the
 * impression that the total number of blocks is the sum of compressed and
 * non-compressed blocks. This is done by returning the sum of the total
 * number of blocks across both relations in the relation_size() TAM callback.
 *
 * The non-compressed relation is sampled first, and, only once the blockno
 * increases beyond the number of blocks in the non-compressed relation, the
 * compressed relation is sampled.
 *
 * For versions starting with PG17 a new interface was introduced based on a
 * ReadStream API, which allows blocks to be read from the relation using a
 * dedicated set of functions.
 *
 * The ReadStream is usually set up in beginscan for the table access method,
 * but for the ANALYZE command there is an exception and it sets up the
 * ReadStream itself and uses that when scanning the blocks. The relation
 * used is the default relation, which is the uncompressed relation, but we
 * need a read stream for the compressed relation as well.
 *
 * When returning blocks, we can first sample the uncompressed relation and then
 * continue with sampling the compressed relation when we have exhausted the
 * uncompressed relation.
 */
#if PG17_LT
static bool
hypercore_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								  BufferAccessStrategy bstrategy)
{
	HypercoreScanDescData *cscan = (HypercoreScanDescData *) scan;
#if PG17_GE
	HeapScanDesc chscan = (HeapScanDesc) cscan->cscan_desc;
#endif
	HeapScanDesc uhscan = (HeapScanDesc) cscan->uscan_desc;

	/* If blockno is past the blocks in the non-compressed relation, we should
	 * analyze the compressed relation */
	if (blockno >= uhscan->rs_nblocks)
	{
		/* Get the compressed rel blockno by subtracting the number of
		 * non-compressed blocks */
		blockno -= uhscan->rs_nblocks;
		return cscan->compressed_rel->rd_tableam->scan_analyze_next_block(cscan->cscan_desc,
																		  blockno,
																		  bstrategy);
	}

	Relation rel = scan->rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	bool result = rel->rd_tableam->scan_analyze_next_block(cscan->uscan_desc, blockno, bstrategy);
	rel->rd_tableam = oldtam;

	return result;
}
#else
static ReadStream *
hypercore_setup_read_stream(Relation rel, BufferAccessStrategy bstrategy)
{
	Assert(rel != NULL);
	BlockSampler block_sampler = palloc(sizeof(BlockSamplerData));
	const BlockNumber totalblocks = RelationGetNumberOfBlocks(rel);
	const uint32 randseed = pg_prng_uint32(&pg_global_prng_state);
	const int targrows = compute_targrows(rel);
	const BlockNumber nblocks = BlockSampler_Init(block_sampler, totalblocks, targrows, randseed);
	pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL, nblocks);

	TS_DEBUG_LOG("set up ReadStream for %s (%d), filenode: %d, pages: %d",
				 RelationGetRelationName(rel),
				 RelationGetRelid(rel),
				 rel->rd_rel->relfilenode,
				 rel->rd_rel->relpages);

	return read_stream_begin_relation(READ_STREAM_MAINTENANCE,
									  bstrategy,
									  rel,
									  MAIN_FORKNUM,
									  hypercore_block_sampling_read_stream_next,
									  block_sampler,
									  0);
}

static bool
hypercore_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	HypercoreScanDescData *cscan = (HypercoreScanDescData *) scan;
	HeapScanDesc uhscan = (HeapScanDesc) cscan->uscan_desc;
	HeapScanDesc PG_USED_FOR_ASSERTS_ONLY chscan = (HeapScanDesc) cscan->cscan_desc;

	/* We do not analyze parent table of hypertables. There is no data there. */
	if (ts_is_hypertable(scan->rs_rd->rd_id))
		return false;

	BufferAccessStrategy bstrategy;
	BlockNumber blockno = read_stream_next_block(stream, &bstrategy);
	TS_DEBUG_LOG("blockno %d, uhscan->rs_nblocks: %d, chscan->rs_nblocks: %d",
				 blockno,
				 uhscan->rs_nblocks,
				 chscan->rs_nblocks);

	if (!cscan->canalyze_read_stream)
	{
		Assert(cscan->compressed_rel);
		cscan->canalyze_read_stream = hypercore_setup_read_stream(cscan->compressed_rel, bstrategy);
	}

	if (!cscan->uanalyze_read_stream)
	{
		const TableAmRoutine *oldtam = switch_to_heapam(scan->rs_rd);
		cscan->uanalyze_read_stream = hypercore_setup_read_stream(scan->rs_rd, bstrategy);
		scan->rs_rd->rd_tableam = oldtam;
	}

	/*
	 * If the block number is above the number of block in the uncompressed
	 * relation, we need to fetch a block from the compressed relation.
	 *
	 * Note that we have a different readstream for the compressed relation,
	 * so we are not sampling the block that is provided to the function, but
	 * we sample the correct number of blocks in for each relation.
	 */
	if (blockno >= uhscan->rs_nblocks)
	{
		TS_DEBUG_LOG("reading block %d from compressed relation", blockno);
		return cscan->compressed_rel->rd_tableam
			->scan_analyze_next_block(cscan->cscan_desc, cscan->canalyze_read_stream);
	}

	TS_DEBUG_LOG("reading block %d from non-compressed relation", blockno - chscan->rs_nblocks);
	Assert(blockno < uhscan->rs_nblocks + chscan->rs_nblocks);

	Relation rel = scan->rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	bool result =
		rel->rd_tableam->scan_analyze_next_block(cscan->uscan_desc, cscan->uanalyze_read_stream);
	rel->rd_tableam = oldtam;
	return result;
}
#endif

/*
 * Get the next tuple to sample during ANALYZE.
 *
 * Since the sampling happens across both the non-compressed and compressed
 * relations, it is necessary to determine from which relation to return a
 * tuple. This is driven by scan_analyze_next_block() above.
 *
 * When sampling from the compressed relation, a compressed segment is read
 * and it is then necessary to return all tuples in the segment.
 *
 * NOTE: the function currently relies on heapAM's scan_analyze_next_tuple()
 * to read compressed segments. This can lead to misrepresenting liverows and
 * deadrows numbers since heap AM might skip tuples that are dead or
 * concurrently inserted, but still count them in liverows or deadrows. Each
 * compressed tuple represents many rows, but heapAM only counts each
 * compressed tuple as one row. The only way to fix this is to either check
 * the diff between the count before and after calling the heap AM function
 * and then estimate the actual number of rows from that, or, reimplement the
 * heapam_scan_analyze_next_tuple() function so that it can properly account
 * for compressed tuples.
 */
static bool
hypercore_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin, double *liverows,
								  double *deadrows, TupleTableSlot *slot)
{
	HypercoreScanDescData *cscan = (HypercoreScanDescData *) scan;
	HeapScanDesc chscan = (HeapScanDesc) cscan->cscan_desc;
	uint16 tuple_index;
	bool result;

	/*
	 * Since non-compressed blocks are always sampled first, the current
	 * buffer for the compressed relation will be invalid until we reach the
	 * end of the non-compressed blocks.
	 */
	if (chscan->rs_cbuf != InvalidBuffer)
	{
		/* Keep on returning tuples from the compressed segment until it is
		 * consumed */
		if (!TTS_EMPTY(slot))
		{
			tuple_index = arrow_slot_row_index(slot);

			if (tuple_index != InvalidTupleIndex && !arrow_slot_is_last(slot))
			{
				ExecIncrArrowTuple(slot, 1);
				*liverows += 1;
				return true;
			}
		}

		TupleTableSlot *child_slot =
			arrow_slot_get_compressed_slot(slot, RelationGetDescr(cscan->compressed_rel));

		result = cscan->compressed_rel->rd_tableam->scan_analyze_next_tuple(cscan->cscan_desc,
																			OldestXmin,
																			liverows,
																			deadrows,
																			child_slot);
		/* Need to pick a row from the segment to sample. Might as well pick
		 * the first one, but might consider picking a random one. */
		tuple_index = 1;
	}
	else
	{
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		Relation rel = scan->rs_rd;
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		result = rel->rd_tableam->scan_analyze_next_tuple(cscan->uscan_desc,
														  OldestXmin,
														  liverows,
														  deadrows,
														  child_slot);
		rel->rd_tableam = oldtam;
		tuple_index = InvalidTupleIndex;
	}

	if (result)
	{
		slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
		ExecStoreArrowTuple(slot, tuple_index);
	}
	else
		ExecClearTuple(slot);

	return result;
}

typedef struct IndexBuildCallbackState
{
	/* Original callback and state state */
	IndexBuildCallback callback;
	void *orig_state;

	/* The table building an index over and original index info */
	Relation rel;
	IndexInfo *index_info;

	/* Expression state and slot for predicate evaluation when building
	 * partial indexes */
	EState *estate;
	ExprContext *econtext;
	ExprState *predicate;
	TupleTableSlot *slot;
	int num_non_index_predicates;

	/* Information needed to process values from compressed data */
	int16 tuple_index;
	double ntuples;
	Bitmapset *segmentby_cols;
	Bitmapset *orderby_cols;
	bool is_segmentby_index;
	MemoryContext decompression_mcxt;
	MemoryContext batch_mcxt;
	ArrowArray **arrow_columns;
} IndexBuildCallbackState;

/*
 * Callback for index builds on compressed relation.
 *
 * See hypercore_index_build_range_scan() for general overview.
 *
 * When building an index, this function is called once for every compressed
 * tuple. To build an index over the original (non-compressed) values, it is
 * necessary to "unwrap" the compressed data. Therefore, the function calls
 * the original index build callback once for every value in the compressed
 * tuple.
 *
 * Note that, when the index covers only segmentby columns and the value is
 * the same for all original rows in the segment, the index storage is
 * optimized to only index the compressed row and then unwrapping it during
 * scanning instead.
 */
static void
hypercore_index_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull,
							   bool tupleIsAlive, void *state)
{
	IndexBuildCallbackState *icstate = state;
	const TupleDesc tupdesc = RelationGetDescr(icstate->rel);
	const Bitmapset *segmentby_cols = icstate->segmentby_cols;
	/* We expect the compressed rel scan to produce a datum array that first
	 * includes the index columns, then any columns referenced in index
	 * predicates that are not index columns. */
	const int natts = icstate->index_info->ii_NumIndexAttrs + icstate->num_non_index_predicates;
	/* Read the actual number of rows in the compressed tuple from the count
	 * column. The count column is appended directly after the index
	 * attributes. */
	const int32 num_actual_rows = DatumGetInt32(values[natts]);
	int32 num_rows = num_actual_rows; /* Num rows to index. For segmentby
									   * indexes, we might change this from
									   * the actual number of rows to indexing
									   * only one row per segment. */

	MemoryContext old_mcxt = MemoryContextSwitchTo(icstate->batch_mcxt);
	MemoryContextReset(icstate->batch_mcxt);

	/* Update ntuples for accurate statistics. When building the index, the
	 * relation's reltuples is updated based on this count. */
	if (tupleIsAlive)
		icstate->ntuples += num_actual_rows;

	/*
	 * Phase 1: Prepare to process the compressed segment.
	 *
	 * We need to figure out the number of rows in the segment, which is
	 * usually given by the "count" column (num_actual_rows). But for
	 * segmentby indexes, we only index whole segments (so num_rows = 1).
	 *
	 * For non-segmentby indexes, we need to go through all attribute values
	 * and decompress segments into multiple rows in columnar arrow array
	 * format.
	 */
	if (icstate->is_segmentby_index)
	{
		/* A segment index will index only the full segment. */
		num_rows = 1;

#ifdef USE_ASSERT_CHECKING
		/* A segment index can only index segmentby columns */
		for (int i = 0; i < natts; i++)
		{
			const AttrNumber attno = icstate->index_info->ii_IndexAttrNumbers[i];
			Assert(bms_is_member(attno, segmentby_cols));
		}
#endif
	}
	else
	{
		for (int i = 0; i < natts; i++)
		{
			const AttrNumber attno = icstate->index_info->ii_IndexAttrNumbers[i];

			if (bms_is_member(attno, segmentby_cols))
			{
				/*
				 * For a segmentby column, there is nothing to decompress, so just
				 * return the non-compressed value.
				 */
			}
			else if (!isnull[i])
			{
				const Form_pg_attribute attr =
					TupleDescAttr(tupdesc, AttrNumberGetAttrOffset(attno));
				icstate->arrow_columns[i] = arrow_from_compressed(values[i],
																  attr->atttypid,
																  icstate->batch_mcxt,
																  icstate->decompression_mcxt);

				/* The number of elements in the arrow array should be the
				 * same as the number of rows in the segment (count
				 * column), except when we use the NULL compression method
				 * to signify all values are NULLs. In this case the
				 * arrow_column value is NULL.
				 */
				Assert(icstate->arrow_columns[i] == NULL ||
					   num_rows == icstate->arrow_columns[i]->length);
			}
			else
			{
				icstate->arrow_columns[i] = NULL;
			}
		}
	}

	Assert((!icstate->is_segmentby_index && num_rows > 0) ||
		   (icstate->is_segmentby_index && num_rows == 1));

	/*
	 * Phase 2: Loop over all "unwrapped" rows in the arrow arrays, build
	 * index tuples, and index them unless they fail predicate checks.
	 */

	/* Table slot for predicate checks. We need to re-create a slot in table
	 * format to be able to do predicate checks once we have decompressed the
	 * values. */
	TupleTableSlot *slot = icstate->slot;

	for (int rownum = 0; rownum < num_rows; rownum++)
	{
		/* The slot is a table slot, not index slot. But we only fill in the
		 * columns needed for the index and predicate checks. Therefore, make sure
		 * other columns are initialized to "null" */
		MemSet(slot->tts_isnull, true, sizeof(bool) * slot->tts_tupleDescriptor->natts);
		ExecClearTuple(slot);

		for (int colnum = 0; colnum < natts; colnum++)
		{
			const AttrNumber attno = icstate->index_info->ii_IndexAttrNumbers[colnum];

			if (bms_is_member(attno, segmentby_cols))
			{
				/* Segmentby columns are not compressed, so the datum in the
				 * values array is already set and valid */
			}
			else if (icstate->arrow_columns[colnum] != NULL)
			{
				const Form_pg_attribute attr =
					TupleDescAttr(tupdesc, AttrNumberGetAttrOffset(attno));
				NullableDatum datum = arrow_get_datum(icstate->arrow_columns[colnum],
													  attr->atttypid,
													  attr->attlen,
													  rownum);
				values[colnum] = datum.value;
				isnull[colnum] = datum.isnull;
			}
			else
			{
				/* No arrow array so all values are NULL */
				values[colnum] = 0;
				isnull[colnum] = true;
			}

			/* Fill in the values in the table slot for predicate checks */
			slot->tts_values[AttrNumberGetAttrOffset(attno)] = values[colnum];
			slot->tts_isnull[AttrNumberGetAttrOffset(attno)] = isnull[colnum];
		}

		ItemPointerData index_tid;
		hypercore_tid_encode(&index_tid, tid, rownum + 1);
		Assert(!icstate->is_segmentby_index || rownum == 0);

		/* Reset memory for predicate checks */
		MemoryContextReset(icstate->econtext->ecxt_per_tuple_memory);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (icstate->predicate)
		{
			/* Mark the slot as valid */
			ExecStoreVirtualTuple(slot);

			if (!ExecQual(icstate->predicate, icstate->econtext))
				continue;
		}

		/* Call the original callback on the original memory context */
		MemoryContextSwitchTo(old_mcxt);
		icstate->callback(index, &index_tid, values, isnull, tupleIsAlive, icstate->orig_state);
		MemoryContextSwitchTo(icstate->batch_mcxt);
	}

	MemoryContextSwitchTo(old_mcxt);
}

/*
 * Build an index over a Hypercore table.
 *
 * The task of this function is to scan all tuples in the table and then,
 * after visibility and predicate checks, pass the tuple to the "index build
 * callback" to have it indexed.
 *
 * Since a Hypercore table technically consists of two heaps: one
 * non-compressed and one compressed, it is necessary to scan both of them. To
 * avoid rewriting/copying the heap code, we make use of the heap AM's
 * machinery. However, that comes with some complications when dealing with
 * compressed tuples. To build an index over compressed tuples, we need to
 * first decompress the segments into individual values. To make this work, we
 * replace the given index build callback with our own, so that we can first
 * decompress the data and then call the real index build callback.
 *
 * Partial indexes present an additional complication because every tuple
 * scanned needs to be checked against the index predicate to know whether it
 * should be part of the index or not. However, the index build callback only
 * gets the values of the indexed columns, not the original table tuple. That
 * won't work for predicates on non-indexed column. Therefore, before calling
 * the heap AM machinery, we change the index definition so that also
 * non-indexed predicate columns will be included in the values array passed
 * on to the "our" index build callback. Then we can reconstruct a table tuple
 * from those values in order to do the predicate check.
 */
static double
hypercore_index_build_range_scan(Relation relation, Relation indexRelation, IndexInfo *indexInfo,
								 bool allow_sync, bool anyvisible, bool progress,
								 BlockNumber start_blockno, BlockNumber numblocks,
								 IndexBuildCallback callback, void *callback_state,
								 TableScanDesc scan)
{
	HypercoreInfo *hcinfo;
	TransactionId OldestXmin;
	bool need_unregister_snapshot = false;
	Snapshot snapshot;

	/*
	 * We can be called from ProcessUtility with a hypertable because we need
	 * to process all ALTER TABLE commands in the list to set options
	 * correctly for the hypertable.
	 *
	 * If we are called on a hypertable, we just skip scanning for tuples and
	 * say that the relation was empty.
	 */
	if (ts_is_hypertable(relation->rd_id))
		return 0.0;

	for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
	{
		const AttrNumber attno = indexInfo->ii_IndexAttrNumbers[i];

		/*
		 * User-defined attributes always have a positive attribute number (1
		 * or larger) and these are the only ones we support, so we check for
		 * that here and raise an error if it is not a user-defined attribute.
		 */
		if (!AttrNumberIsForUserDefinedAttr(attno))
		{
			/*
			 * If the attribute number if zero, it means that we have an
			 * expression index in this column and need to call the
			 * corresponding expression tree in ii_Expressions to compute the
			 * value to store in the index.
			 *
			 * If the attribute number is negative, it means that we have a
			 * reference to a system attribute (see sysattr.h), which we do
			 * not support either.
			 */
			if (attno == InvalidAttrNumber)
				ereport(ERROR,
						errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("expression indexes not supported"));
			else
				ereport(ERROR,
						errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot index system columns"));
		}
	}

	hcinfo = RelationGetHypercoreInfo(relation);

	/*
	 * In accordance with the heapam implementation, setup the scan
	 * descriptor. Do it here instead of letting the heapam handler do it
	 * since we want a hypercore scan descriptor that includes the state for
	 * both the non-compressed and compressed relations.
	 *
	 * Prepare for scan of the base relation. In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples). In a
	 * concurrent build, or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 *
	 * Hypercore is not used during bootstrap so skip that check.
	 */
	OldestXmin = InvalidTransactionId;

	/* okay to ignore lazy VACUUMs here */
	if (!indexInfo->ii_Concurrent)
	{
		OldestXmin = GetOldestNonRemovableTransactionId(relation);
	}

	if (!scan)
	{
		/*
		 * Serial index build.
		 *
		 * Must begin our own heap scan in this case.  We may also need to
		 * register a snapshot whose lifetime is under our direct control.
		 */
		if (!TransactionIdIsValid(OldestXmin))
		{
			snapshot = RegisterSnapshot(GetTransactionSnapshot());
			need_unregister_snapshot = true;
		}
		else
			snapshot = SnapshotAny;

		scan = table_beginscan_strat(relation,	  /* relation */
									 snapshot,	  /* snapshot */
									 0,			  /* number of keys */
									 NULL,		  /* scan key */
									 true,		  /* buffer access strategy OK */
									 allow_sync); /* syncscan OK? */
	}
	else
	{
		/*
		 * Parallel index build.
		 *
		 * Parallel case never registers/unregisters own snapshot.  Snapshot
		 * is taken from parallel heap scan, and is SnapshotAny or an MVCC
		 * snapshot, based on same criteria as serial case.
		 */
		Assert(allow_sync);
		snapshot = scan->rs_snapshot;
	}

	HypercoreScanDescData *hscan = (HypercoreScanDescData *) scan;
	EState *estate = CreateExecutorState();
	Relation crel = hscan->compressed_rel;
	IndexBuildCallbackState icstate = {
		.callback = callback,
		.orig_state = callback_state,
		.rel = relation,
		.estate = estate,
		.econtext = GetPerTupleExprContext(estate),
		.slot = MakeSingleTupleTableSlot(RelationGetDescr(relation), &TTSOpsVirtual),
		.index_info = indexInfo,
		.tuple_index = -1,
		.ntuples = 0,
		.batch_mcxt = AllocSetContextCreate(CurrentMemoryContext,
											"Compressed batch for index build",
											ALLOCSET_DEFAULT_SIZES),
		.decompression_mcxt = AllocSetContextCreate(CurrentMemoryContext,
													"Bulk decompression for index build",
													/* minContextSize = */ 0,
													/* initBlockSize = */ 64 * 1024,
													/* maxBlockSize = */ 64 * 1024),
		/* Allocate arrow array for all attributes in the relation although
		 * index might need only a subset. This is to accommodate any extra
		 * predicate attributes (see below). */
		.arrow_columns =
			(ArrowArray **) palloc(sizeof(ArrowArray *) * RelationGetDescr(relation)->natts),
		.is_segmentby_index = true,
	};

	/* IndexInfo copy to use when processing compressed relation. It will be
	 * modified slightly since the compressed rel has different attribute
	 * number mappings. It is also not possible to do all index processing on
	 * compressed tuples, e.g., predicate checks (see below). */
	IndexInfo compress_iinfo = *indexInfo;

	build_segment_and_orderby_bms(hcinfo, &icstate.segmentby_cols, &icstate.orderby_cols);

	/* Translate index attribute numbers for the compressed relation */
	for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
	{
		const AttrNumber attno = indexInfo->ii_IndexAttrNumbers[i];
		const AttrNumber cattno = hcinfo->columns[AttrNumberGetAttrOffset(attno)].cattnum;

		compress_iinfo.ii_IndexAttrNumbers[i] = cattno;
		icstate.arrow_columns[i] = NULL;

		/* If the indexed column is not a segmentby column, then this is not a
		 * segmentby index */
		if (!bms_is_member(attno, icstate.segmentby_cols))
			icstate.is_segmentby_index = false;
	}

	Assert(indexInfo->ii_NumIndexAttrs == compress_iinfo.ii_NumIndexAttrs);

	/* If there are predicates, it's a partial index build. It is necessary to
	 * find any columns referenced in the predicates that are not included in
	 * the index. We need to make sure that the heap AM will include these
	 * columns when building an index tuple so that we can later do predicate
	 * checks on them. */
	if (indexInfo->ii_Predicate != NIL)
	{
		const List *vars = pull_vars_of_level((Node *) indexInfo->ii_Predicate, 0);
		ListCell *lc;

		/* Check if the predicate attribute is already part of the index or
		 * not. If not, append it to the end of the index attributes. */
		foreach (lc, vars)
		{
			const Var *v = lfirst_node(Var, lc);
			bool found = false;

			for (int i = 0; i < compress_iinfo.ii_NumIndexAttrs; i++)
			{
				AttrNumber attno = compress_iinfo.ii_IndexAttrNumbers[i];

				if (v->varattno == attno)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				/* Need to translate attribute number for compressed rel */
				const int offset = AttrNumberGetAttrOffset(v->varattno);
				const AttrNumber cattno = hcinfo->columns[offset].cattnum;
				const int num_index_attrs =
					compress_iinfo.ii_NumIndexAttrs + icstate.num_non_index_predicates;

				Ensure(compress_iinfo.ii_NumIndexAttrs < INDEX_MAX_KEYS,
					   "too many predicate attributes in index");

				/* If the predicate column is not part of the index, we need
				 * to include it in the index info passed to heap AM when
				 * scanning the compressed relation. */
				compress_iinfo.ii_IndexAttrNumbers[num_index_attrs] = cattno;

				/* We also add the attribute mapping to the original index
				 * info, but we don't increase indexInfo->ii_NumIndexAttrs
				 * because that will change the index definition. Instead we
				 * track the number of additional predicate attributes in
				 * icstate.num_non_index_predicates. */
				const int natts = indexInfo->ii_NumIndexAttrs + icstate.num_non_index_predicates;
				indexInfo->ii_IndexAttrNumbers[natts] = v->varattno;
				icstate.num_non_index_predicates++;
			}
		}

		/* Can't evaluate predicates on compressed tuples. This is done in
		 * hypercore_index_build_callback instead. */
		compress_iinfo.ii_Predicate = NULL;

		/* Set final number of index attributes. Includes original number of
		 * attributes plus the new predicate attributes */
		compress_iinfo.ii_NumIndexAttrs =
			compress_iinfo.ii_NumIndexAttrs + icstate.num_non_index_predicates;

		/* Set up predicate evaluation, including the slot for econtext */
		icstate.econtext->ecxt_scantuple = icstate.slot;
		icstate.predicate = ExecPrepareQual(indexInfo->ii_Predicate, icstate.estate);
	}

	/* Make sure the count column is included last in the index tuple
	 * generated by the heap AM machinery. It is needed to know the
	 * uncompressed tuple count in case of building an index on the segmentby
	 * column. */
	Ensure(compress_iinfo.ii_NumIndexAttrs < INDEX_MAX_KEYS,
		   "too many predicate attributes in index");
	compress_iinfo.ii_IndexAttrNumbers[compress_iinfo.ii_NumIndexAttrs++] = hcinfo->count_cattno;

	/* Call heap's index_build_range_scan() on the compressed relation. The
	 * custom callback we give it will "unwrap" the compressed segments into
	 * individual tuples. Therefore, we cannot use the tuple count returned by
	 * the function since it only represents the number of compressed
	 * tuples. Instead, tuples are counted in the callback state. */
	crel->rd_tableam->index_build_range_scan(crel,
											 indexRelation,
											 &compress_iinfo,
											 allow_sync,
											 anyvisible,
											 progress,
											 start_blockno,
											 numblocks,
											 hypercore_index_build_callback,
											 &icstate,
											 hscan->cscan_desc);

	/* Heap's index_build_range_scan() ended the scan, so set the scan
	 * descriptor to NULL here in order to not try to close it again in our
	 * own table_endscan(). */
	hscan->cscan_desc = NULL;

	FreeExecutorState(icstate.estate);
	ExecDropSingleTupleTableSlot(icstate.slot);
	MemoryContextDelete(icstate.decompression_mcxt);
	MemoryContextDelete(icstate.batch_mcxt);
	pfree((void *) icstate.arrow_columns);
	bms_free(icstate.segmentby_cols);
	bms_free(icstate.orderby_cols);

	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	double ntuples = relation->rd_tableam->index_build_range_scan(relation,
																  indexRelation,
																  indexInfo,
																  allow_sync,
																  anyvisible,
																  progress,
																  start_blockno,
																  numblocks,
																  callback,
																  callback_state,
																  hscan->uscan_desc);
	/* Heap's index_build_range_scan() should have ended the scan, so set the
	 * scan descriptor to NULL here in order to not try to close it again in
	 * our own table_endscan(). */
	hscan->uscan_desc = NULL;
	relation->rd_tableam = oldtam;
	table_endscan(scan);

	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	return icstate.ntuples + ntuples;
}

/*
 * Validate index.
 *
 * Used for concurrent index builds.
 */
static void
hypercore_index_validate_scan(Relation compressionRelation, Relation indexRelation,
							  IndexInfo *indexInfo, Snapshot snapshot, ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("concurrent index creation on is not supported on tables using hypercore table "
					"access method")));
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the Hypercore
 * ------------------------------------------------------------------------
 */
static bool
hypercore_relation_needs_toast_table(Relation rel)
{
	return false;
}

static Oid
hypercore_relation_toast_am(Relation rel)
{
	FEATURE_NOT_SUPPORTED;
	return InvalidOid;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the Hypercore
 * ------------------------------------------------------------------------
 */

/*
 * Return the relation size in bytes.
 *
 * The relation size in bytes is computed from the number of blocks in the
 * relation multiplied by the block size.
 *
 * However, since the compression TAM is a "meta" relation over separate
 * non-compressed and compressed heaps, the total size is actually the sum of
 * the number of blocks in both heaps.
 *
 * To get the true size of the TAM (non-compressed) relation, it is possible
 * to use switch_to_heapam() and bypass the TAM callbacks.
 */
static uint64
hypercore_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 ubytes = table_block_relation_size(rel, forkNumber);
	int32 hyper_id = ts_chunk_get_hypertable_id_by_reloid(rel->rd_id);

	if (hyper_id == INVALID_HYPERTABLE_ID)
		return ubytes;

	/* For ANALYZE, need to return sum for both relations. */
	Relation crel =
		try_relation_open(RelationGetHypercoreInfo(rel)->compressed_relid, AccessShareLock);

	if (crel == NULL)
		return ubytes;

	uint64 cbytes = table_block_relation_size(crel, forkNumber);
	relation_close(crel, NoLock);

	return ubytes + cbytes;
}

#define HEAP_OVERHEAD_BYTES_PER_TUPLE (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))
#define HEAP_USABLE_BYTES_PER_PAGE (BLCKSZ - SizeOfPageHeaderData)

/*
 * Calculate fraction of visible pages.
 *
 * Same calculation as in PG's table_block_relation_estimate_size().
 */
static double
calc_allvisfrac(BlockNumber curpages, BlockNumber relallvisible)
{
	double allvisfrac;

	if (relallvisible == 0 || curpages <= 0)
		allvisfrac = 0;
	else if ((double) relallvisible >= curpages)
		allvisfrac = 1;
	else
		allvisfrac = (double) relallvisible / curpages;

	return allvisfrac;
}

/*
 * Get the number of blocks on disk of a relation.
 *
 * Bypasses hypercore_relation_size()/RelationGetNumberOfBlocks(), which
 * return the aggregate size (compressed + non-compressed).
 */
static BlockNumber
relation_number_of_disk_blocks(Relation rel)
{
	uint64 szbytes = table_block_relation_size(rel, MAIN_FORKNUM);
	return (szbytes + (BLCKSZ - 1)) / BLCKSZ;
}

/*
 * Estimate the size of a Hypercore relation.
 *
 * For "heap", PostgreSQL estimates the number of tuples based on the
 * difference between the as-of-this-instant number of blocks on disk and the
 * current pages in relstats (relpages). In other words, if there are more
 * blocks on disk than pages according to relstats, the relation grew and the
 * number of tuples can be extrapolated from the previous "tuple density" in
 * relstats (reltuples / relpages).
 *
 * However, this extrapolation doesn't work well for a Hypercore since there
 * are situations where a relation can shrink in terms of pages, but grow in
 * terms of data. For example, simply compressing a hypercore (with no
 * previous compressed data), will shrink the number of blocks significantly
 * while there was no change in number of tuples. The standard PostgreSQL
 * estimate will believe that a lot of data was deleted, thus vastly
 * underestimating the number of tuples. Conversely, decompression will lead
 * to overestimating since the number of pages increase drastically.
 *
 * Note that a hypercore stores the aggregate stats (compressed +
 * non-compressed) in the non-compressed relation. So, reltuples is the actual
 * number of tuples as of the last ANALYZE (or similar operation that updates
 * relstats). Therefore, when estimating tuples, using the normal PG function,
 * compute an "average" tuple that represents something in-between a
 * non-compressed tuple and a compressed one, based on the fraction of
 * compressed vs non-compressed pages. Once there's an estimation of the
 * number of "average" tuples, multiply the fraction of compressed tuples with
 * the target size of a compressed batch to get the final tuple count.
 *
 * An alternative approach could be to calculate each relation's estimate
 * separately and then add the results. However, that requires having stats
 * for each separate relation, but, currently, there are often no stats for
 * the compressed relation (this could be fixed, though). However, even if
 * there were stats for the compressed relation, those stats would only have
 * an accurate compressed tuple count, and the actual number of tuples would
 * have to be estimated from that.
 *
 * Another option is to store custom stats outside relstats where it is
 * possible to maintain accurate tuple counts for each relation.
 *
 * However, until there's a better way to figure out whether data was actually
 * added, removed, or stayed the same, it is better to just return the current
 * stats, if they exist. Ideally, a hypercore should not be mutated often and
 * be mostly (if not completely) compressed. When compressing or
 * decompressing, relstats should also be updated. Therefore, the relstats
 * should be quite accurate.
 */
static void
hypercore_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages,
								 double *tuples, double *allvisfrac)
{
	/*
	 * We can be called from ProcessUtility with a hypertable because we need
	 * to process all ALTER TABLE commands in the list to set options
	 * correctly for the hypertable.
	 *
	 * If we are called on a hypertable, we just say that the hypertable does
	 * not have any pages or tuples.
	 */
	if (ts_is_hypertable(rel->rd_id))
	{
		*pages = 0;
		*allvisfrac = 0;
		*tuples = 0;
		return;
	}

	const Form_pg_class form = RelationGetForm(rel);
	Size overhead_bytes_per_tuples = HEAP_OVERHEAD_BYTES_PER_TUPLE;
	Relation crel = hypercore_open_compressed(rel, AccessShareLock);
	BlockNumber nblocks = relation_number_of_disk_blocks(rel);
	BlockNumber cnblocks = relation_number_of_disk_blocks(crel);

	table_close(crel, AccessShareLock);

	if (nblocks == 0 && cnblocks == 0)
	{
		*pages = 0;
		*allvisfrac = 0;
		*tuples = 0;
		return;
	}

	double frac_noncompressed = 0;

	if (form->reltuples >= 0)
	{
		/*
		 * There's stats, use it.
		 */
		*pages = form->relpages;
		*tuples = form->reltuples;
		*allvisfrac = calc_allvisfrac(nblocks + cnblocks, form->relallvisible);

		TS_DEBUG_LOG("(stats) pages %u tuples %lf allvisfrac %f", *pages, *tuples, *allvisfrac);
		return;
	}
	else if (nblocks == 0 && cnblocks > 0)
		frac_noncompressed = 0;
	else if (nblocks > 0 && cnblocks == 0)
		frac_noncompressed = 1;
	else
	{
		Assert(cnblocks != 0);
		/* Try to figure out the fraction of data that is compressed vs
		 * non-compressed. */
		frac_noncompressed = ((double) nblocks / (cnblocks * TARGET_COMPRESSED_BATCH_SIZE));
	}

	/* The overhead will be 0 for mostly compressed data, which is fine
	 * because compared to non-compressed data the overhead is negligible
	 * anyway. */
	overhead_bytes_per_tuples = rint(HEAP_OVERHEAD_BYTES_PER_TUPLE * frac_noncompressed);

	/*
	 * Compute an estimate based on the "aggregate" relation.
	 *
	 * Note that this function gets the number of blocks of the relation in
	 * order to extrapolate a new tuple count based on the "tuple
	 * density". This works for the hypercore relation because
	 * RelationGetNumberOfBlocks() returns the aggregate block count of both
	 * relations. Also note that using the attr_widths for the non-compressed
	 * rel won't be very representative for mostly compressed data. Should
	 * probably compute new "average" attr_widths based on the fraction. But
	 * that is left for the future.
	 */
	table_block_relation_estimate_size(rel,
									   attr_widths,
									   pages,
									   tuples,
									   allvisfrac,
									   overhead_bytes_per_tuples,
									   HEAP_USABLE_BYTES_PER_PAGE);

	*tuples =
		(*tuples * frac_noncompressed) + ((1 - frac_noncompressed) * TARGET_COMPRESSED_BATCH_SIZE);

	TS_DEBUG_LOG("(estimated) pages %u tuples %lf allvisfrac %f frac_noncompressed %lf",
				 *pages,
				 *tuples,
				 *allvisfrac,
				 frac_noncompressed);
}

static void
hypercore_fetch_toast_slice(Relation toastrel, Oid valueid, int32 attrsize, int32 sliceoffset,
							int32 slicelength, struct varlena *result)
{
	FEATURE_NOT_SUPPORTED;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the Hypercore
 * ------------------------------------------------------------------------
 */

static bool
hypercore_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static bool
hypercore_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								 TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

/*
 * Cleanup callback for Hypercore conversion state.
 *
 * The cleanup happens when the conversion state's memory context is
 * destroyed. It ensures cleanup of tuplesort state, unless it was already
 * performed.
 *
 * This is to cover two cases:
 *
 * 1. The tuplesort state is released due to normal processing and a call to
 * tuplesort_end(), in which case the tuplesort state is released before the
 * conversion state. This callback should therefore not call tuplesort_end().
 *
 * 2. The tuplesort state is released as a result of an ERROR, in which case
 * the tuplesort_end() is called in this callback as a result of the
 * conversion state being cleaned up.
 */
static void
conversionstate_cleanup(void *arg)
{
	ConversionState *state = arg;

	if (state->tuplesortstate)
	{
		tuplesort_end(state->tuplesortstate);
		state->tuplesortstate = NULL;
	}

	if (conversionstate)
	{
		Assert(state == conversionstate);
		conversionstate = NULL;
	}
}

static ConversionState *
conversionstate_create(const HypercoreInfo *hcinfo, const Relation rel)
{
	CompressionSettings *settings = ts_compression_settings_get(RelationGetRelid(rel));
	Tuplesortstate *tuplesortstate;
	MemoryContext mcxt;
	MemoryContext oldmcxt;
	ConversionState *state;

	oldmcxt = MemoryContextSwitchTo(PortalContext);
	/*
	 * We want to ensure the tuplesort state is cleaned up by calling
	 * tuplesort_end() in case of failures. This is necessary to release disk
	 * resources.
	 *
	 * A memory context callback is used for this purpose. The callback is
	 * attached to the Hypercore conversion state. The tuplesort state will
	 * allocate its own child context, but they cannot be children of the
	 * conversion memory context because children are freed before the
	 * parent. Instead, make both the tuplesort and conversion state children
	 * of PortalContext. Since they are destroyed in reverse order, the memory
	 * context callback for the conversion state can. in case of error, call
	 * tuplesort_end() before the tuplesort is freed.
	 */
	tuplesortstate = compression_create_tuplesort_state(settings, rel);
	mcxt = AllocSetContextCreate(PortalContext, "Hypercore conversion", ALLOCSET_DEFAULT_SIZES);

	state = MemoryContextAlloc(mcxt, sizeof(ConversionState));
	state->mcxt = mcxt;
	state->before_size = ts_relation_size_impl(RelationGetRelid(rel));
	state->tuplesortstate = tuplesortstate;
	Assert(state->tuplesortstate);
	state->relid = RelationGetRelid(rel);
	state->cb.arg = state;
	state->cb.func = conversionstate_cleanup;
	conversionstate = state;
	MemoryContextRegisterResetCallback(state->mcxt, &state->cb);
	MemoryContextSwitchTo(oldmcxt);

	return state;
}

/*
 * Convert a table to Hypercore.
 *
 * Need to setup the conversion state used to compress the data.
 */
static void
convert_to_hypercore(Oid relid)
{
	Relation relation = table_open(relid, AccessShareLock);
	bool compress_chunk_created;
	HypercoreInfo *hcinfo = lazy_build_hypercore_info_cache(relation,
															false /* create constraints */,
															&compress_chunk_created);

	if (!compress_chunk_created)
	{
		/* A compressed relation already exists, so converting from legacy
		 * compression. It is only necessary to create the proxy vacuum
		 * index. */
		create_proxy_vacuum_index(relation, hcinfo->compressed_relid);
		table_close(relation, AccessShareLock);
		return;
	}

	conversionstate = conversionstate_create(hcinfo, relation);
	table_close(relation, NoLock);
}

void
hypercore_xact_event(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		{
			ListCell *lc;

			/* Check for relations that might now be partially compressed and
			 * update their status */
			foreach (lc, partially_compressed_relids)
			{
				Oid relid = lfirst_oid(lc);
				Relation rel = table_open(relid, AccessShareLock);
				/* Calling RelationGetHypercoreInfo() here will create the
				 * compressed relation if not already created. */
				Ensure(OidIsValid(RelationGetHypercoreInfo(rel)->compressed_relid),
					   "hypercore \"%s\" has no compressed data relation",
					   get_rel_name(relid));

				Chunk *chunk = ts_chunk_get_by_relid(relid, true);
				ts_chunk_set_partial(chunk);
				table_close(rel, NoLock);
			}
			break;
		}
		default:
			break;
	}

	if (partially_compressed_relids != NIL)
	{
		list_free(partially_compressed_relids);
		partially_compressed_relids = NIL;
	}
}

static void
convert_to_hypercore_finish(Oid relid)
{
	if (!conversionstate)
	{
		/* Without a tuple sort state, conversion happens from legacy
		 * compression where a compressed relation (chunk) already
		 * exists. There's nothing more to do. */
		return;
	}

#ifdef USE_ASSERT_CHECKING
	/* Blow away relation cache to test that the tuple sort state works across
	 * relcache invalidations. Previously there was sometimes a crash here
	 * because the tuple sort state had a reference to a tuple descriptor in
	 * the relcache. */
	RelationCacheInvalidate(false);
#endif

	Chunk *chunk = ts_chunk_get_by_relid(conversionstate->relid, true);
	Relation relation = table_open(conversionstate->relid, AccessShareLock);

	if (!chunk)
		elog(ERROR, "could not find uncompressed chunk for relation %s", get_rel_name(relid));
	Hypertable *ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
	Hypertable *ht_compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);

	tuplesort_performsort(conversionstate->tuplesortstate);

	/*
	 * The compressed chunk should have been created in
	 * convert_to_hypercore_start() if it didn't already exist.
	 */
	Chunk *c_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
	Relation compressed_rel = table_open(c_chunk->table_id, RowExclusiveLock);
	const CompressionSettings *settings = ts_compression_settings_get(conversionstate->relid);
	RowCompressor row_compressor;

	Assert(settings->fd.relid == RelationGetRelid(relation));
	Assert(settings->fd.compress_relid == RelationGetRelid(compressed_rel));

	row_compressor_init(&row_compressor,
						settings,
						RelationGetDescr(relation),
						RelationGetDescr(compressed_rel));
	BulkWriter writer = bulk_writer_build(compressed_rel, HEAP_INSERT_FROZEN);

	row_compressor_append_sorted_rows(&row_compressor,
									  conversionstate->tuplesortstate,
									  compressed_rel,
									  &writer);

	row_compressor_close(&row_compressor);
	bulk_writer_close(&writer);
	tuplesort_end(conversionstate->tuplesortstate);
	conversionstate->tuplesortstate = NULL;

	/* Copy chunk constraints (including fkey) to compressed chunk.
	 * Do this after compressing the chunk to avoid holding strong, unnecessary locks on the
	 * referenced table during compression.
	 */
	ts_chunk_constraints_create(ht_compressed, c_chunk);
	ts_trigger_create_all_on_chunk(c_chunk);
	create_proxy_vacuum_index(relation, RelationGetRelid(compressed_rel));
	/* We use makeInteger since makeBoolean does not exist prior to PG15 */
	List *options = list_make1(makeDefElem("autovacuum_enabled", (Node *) makeInteger(0), -1));
	ts_relation_set_reloption(compressed_rel, options, RowExclusiveLock);

	table_close(relation, NoLock);
	table_close(compressed_rel, NoLock);

	/* Update compression statistics */
	create_compression_relation_size_stats(chunk->fd.id,
										   chunk->table_id,
										   c_chunk->fd.id,
										   c_chunk->table_id,
										   &conversionstate->before_size,
										   row_compressor.rowcnt_pre_compression,
										   row_compressor.num_compressed_rows,
										   row_compressor.num_compressed_rows);

	MemoryContextDelete(conversionstate->mcxt);
	/* Deleting the memorycontext should reset the global conversionstate pointer to NULL */
	Assert(conversionstate == NULL);
}

void
hypercore_alter_access_method_begin(Oid relid, bool to_other_am)
{
	if (to_other_am)
		check_guc_setting_compatible_with_scan();
	else
		convert_to_hypercore(relid);
}

/*
 * Called at the end of converting a chunk to a table access method.
 */
void
hypercore_alter_access_method_finish(Oid relid, bool to_other_am)
{
	Chunk *chunk = ts_chunk_get_by_relid(relid, false);

	/* If this is not a chunk, we just abort since there is nothing to do */
	if (!chunk)
		return;

	if (to_other_am)
	{
		Chunk *compress_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, false);

		ts_compression_chunk_size_delete(chunk->fd.id);
		ts_chunk_clear_compressed_chunk(chunk);

		if (compress_chunk)
		{
			ts_compression_settings_delete(relid);
			ts_chunk_drop(compress_chunk, DROP_RESTRICT, -1);
		}
	}

	/* Finishing the conversion to Hypercore is handled in the
	 * finish_bulk_insert callback */
}

/*
 * Convert any index-only scans on segmentby indexes to regular index scans
 * since index-only scans are not supported on segmentby indexes.
 *
 * Indexes on segmentby columns are optimized to store only one index
 * reference per segment instead of one per value in each segment. This relies
 * on "unwrapping" the segment during scanning. However, with an
 * IndexOnlyScan, Hypercore's index_fetch_tuple() is not be called to fetch
 * the heap tuple (since the scan returns directly from the index), and there
 * is no opportunity to unwrap the tuple. Therefore, turn IndexOnlyScans into
 * regular IndexScans on segmentby indexes.
 */
static void
convert_index_only_scans(Relation rel, List *pathlist)
{
	ListCell *lc;

	foreach (lc, pathlist)
	{
		Path *path = lfirst(lc);
		bool is_segmentby_index = true;

		if (path->pathtype == T_IndexOnlyScan)
		{
			IndexPath *ipath = (IndexPath *) path;
			Relation irel = relation_open(ipath->indexinfo->indexoid, AccessShareLock);
			const int2vector *indkeys = &irel->rd_index->indkey;

			for (int i = 0; i < indkeys->dim1; i++)
			{
				const AttrNumber attno = indkeys->values[i];
				const HypercoreInfo *hcinfo = RelationGetHypercoreInfo(rel);
				if (!hcinfo->columns[AttrNumberGetAttrOffset(attno)].is_segmentby)
				{
					is_segmentby_index = false;
					break;
				}
			}

			/* Convert this IndexOnlyScan to a regular IndexScan since
			 * segmentby indexes do not support IndexOnlyScans */
			if (is_segmentby_index)
				path->pathtype = T_IndexScan;

			relation_close(irel, AccessShareLock);
		}
	}
}

void
hypercore_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht)
{
	const RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	Relation relation = table_open(rte->relid, AccessShareLock);
	convert_index_only_scans(relation, rel->pathlist);
	convert_index_only_scans(relation, rel->partial_pathlist);
	table_close(relation, AccessShareLock);
}

/* ------------------------------------------------------------------------
 * Definition of the Hypercore table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine hypercore_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = hypercore_slot_callbacks,

	.scan_begin = hypercore_beginscan,
	.scan_end = hypercore_endscan,
	.scan_rescan = hypercore_rescan,
	.scan_getnextslot = hypercore_getnextslot,
	/*-----------
	 * Optional functions to provide scanning for ranges of ItemPointers.
	 * Implementations must either provide both of these functions, or neither
	 * of them.
	 */
	.scan_set_tidrange = NULL,
	.scan_getnextslot_tidrange = NULL,
	/* ------------------------------------------------------------------------
	 * Parallel table scan related functions.
	 * ------------------------------------------------------------------------
	 */
	.parallelscan_estimate = hypercore_parallelscan_estimate,
	.parallelscan_initialize = hypercore_parallelscan_initialize,
	.parallelscan_reinitialize = hypercore_parallelscan_reinitialize,

	/* ------------------------------------------------------------------------
	 * Index Scan Callbacks
	 * ------------------------------------------------------------------------
	 */
	.index_fetch_begin = hypercore_index_fetch_begin,
	.index_fetch_reset = hypercore_index_fetch_reset,
	.index_fetch_end = hypercore_index_fetch_end,
	.index_fetch_tuple = hypercore_index_fetch_tuple,

	/* ------------------------------------------------------------------------
	 * Manipulations of physical tuples.
	 * ------------------------------------------------------------------------
	 */
	.tuple_insert = hypercore_tuple_insert,
	.tuple_insert_speculative = hypercore_tuple_insert_speculative,
	.tuple_complete_speculative = hypercore_tuple_complete_speculative,
	.multi_insert = hypercore_multi_insert,
	.tuple_delete = hypercore_tuple_delete,
	.tuple_update = hypercore_tuple_update,
	.tuple_lock = hypercore_tuple_lock,

	.finish_bulk_insert = hypercore_finish_bulk_insert,

	/* ------------------------------------------------------------------------
	 * Callbacks for non-modifying operations on individual tuples
	 * ------------------------------------------------------------------------
	 */
	.tuple_fetch_row_version = hypercore_fetch_row_version,

	.tuple_get_latest_tid = hypercore_get_latest_tid,
	.tuple_tid_valid = hypercore_tuple_tid_valid,
	.tuple_satisfies_snapshot = hypercore_tuple_satisfies_snapshot,
	.index_delete_tuples = hypercore_index_delete_tuples,

/* ------------------------------------------------------------------------
 * DDL related functionality.
 * ------------------------------------------------------------------------
 */
#if PG16_GE
	.relation_set_new_filelocator = hypercore_relation_set_new_filelocator,
#else
	.relation_set_new_filenode = hypercore_relation_set_new_filelocator,
#endif
	.relation_nontransactional_truncate = hypercore_relation_nontransactional_truncate,
	.relation_copy_data = hypercore_relation_copy_data,
	.relation_copy_for_cluster = hypercore_relation_copy_for_cluster,
	.relation_vacuum = hypercore_vacuum_rel,
	.scan_analyze_next_block = hypercore_scan_analyze_next_block,
	.scan_analyze_next_tuple = hypercore_scan_analyze_next_tuple,
	.index_build_range_scan = hypercore_index_build_range_scan,
	.index_validate_scan = hypercore_index_validate_scan,

	/* ------------------------------------------------------------------------
	 * Miscellaneous functions.
	 * ------------------------------------------------------------------------
	 */
	.relation_size = hypercore_relation_size,
	.relation_needs_toast_table = hypercore_relation_needs_toast_table,
	.relation_toast_am = hypercore_relation_toast_am,
	.relation_fetch_toast_slice = hypercore_fetch_toast_slice,

	/* ------------------------------------------------------------------------
	 * Planner related functions.
	 * ------------------------------------------------------------------------
	 */
	.relation_estimate_size = hypercore_relation_estimate_size,

	/* ------------------------------------------------------------------------
	 * Executor related functions.
	 * ------------------------------------------------------------------------
	 */

	/* We do not support bitmap heap scan at this point. */
	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,

	.scan_sample_next_block = hypercore_scan_sample_next_block,
	.scan_sample_next_tuple = hypercore_scan_sample_next_tuple,
};

const TableAmRoutine *
hypercore_routine(void)
{
	return &hypercore_methods;
}

Datum
hypercore_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&hypercore_methods);
}
