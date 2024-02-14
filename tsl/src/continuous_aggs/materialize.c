/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <executor/spi.h>
#include <executor/tstoreReceiver.h>
#include <fmgr.h>
#include <parser/analyze.h>
#include <tcop/utility.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/date.h>
#include <utils/snapmgr.h>

#include <scanner.h>
#include <compat/compat.h>
#include <scan_iterator.h>
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"
#include <time_utils.h>
#include "debug_assert.h"

#include "materialize.h"

#define CHUNKIDFROMRELID "chunk_id_from_relid"
#define CONTINUOUS_AGG_CHUNK_ID_COL_NAME "chunk_id"

static bool ranges_overlap(InternalTimeRange invalidation_range,
						   InternalTimeRange new_materialization_range);
static TimeRange internal_time_range_to_time_range(InternalTimeRange internal);
static int64 range_length(const InternalTimeRange range);
static Datum internal_to_time_value_or_infinite(int64 internal, Oid time_type,
												bool *is_infinite_out);

static PlannedStmt *
sql_create_plan(const char *queryString)
{
	List *raw_parsetree_list;
	raw_parsetree_list = pg_parse_query(queryString);

	Ensure(list_length(raw_parsetree_list) == 1, "multi statement SQL string is not allowed");

	RawStmt *rawstmt;
	ParseState *pstate;
	List *rewritten;
	PlannedStmt *plan;
	Query *query;
	Query *copied_query;

	/* Transform ParseTree into QueryTree */
	rawstmt = (RawStmt *) linitial(raw_parsetree_list);
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;
	query = transformTopLevelStmt(pstate, rawstmt);
	free_parsestate(pstate);

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for sql_create_plan function");
	query = (Query *) linitial(rewritten);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushActiveSnapshot(GetTransactionSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Plan the query which will generate data for the refresh. */
	plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, NULL);

	return plan;
}

static QueryDesc *
sql_execute_plan(const char *queryString, PlannedStmt *plan, DestReceiver *receiver)
{
	QueryDesc *queryDesc;

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan,
								queryString,
								GetActiveSnapshot(),
								InvalidSnapshot,
								receiver,
								NULL,
								NULL,
								0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0, true);

	return queryDesc;
}

static void
sql_close_plan(QueryDesc *queryDesc, DestReceiver *receiver)
{
	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	/* Clean up the receiver. */
	(*receiver->rDestroy)(receiver);

	PopActiveSnapshot();
}

/***************************
 * materialization support *
 ***************************/

static void spi_update_materializations(Hypertable *mat_ht, SchemaAndName partial_view,
										SchemaAndName materialization_table,
										const NameData *time_column_name,
										TimeRange invalidation_range, const int32 chunk_id);
static void spi_delete_materializations(SchemaAndName materialization_table,
										const NameData *time_column_name,
										TimeRange invalidation_range,
										const char *const chunk_condition);
static void spi_insert_materializations(Hypertable *mat_ht, SchemaAndName partial_view,
										SchemaAndName materialization_table,
										const NameData *time_column_name,
										TimeRange materialization_range,
										const char *const chunk_condition);

void
continuous_agg_update_materialization(Hypertable *mat_ht, SchemaAndName partial_view,
									  SchemaAndName materialization_table,
									  const NameData *time_column_name,
									  InternalTimeRange new_materialization_range,
									  InternalTimeRange invalidation_range, int32 chunk_id)
{
	InternalTimeRange combined_materialization_range = new_materialization_range;
	bool materialize_invalidations_separately = range_length(invalidation_range) > 0;
	int res;

	/* Lock down search_path */
	res = SPI_exec("SET LOCAL search_path TO pg_catalog, pg_temp", 0);
	if (res < 0)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), (errmsg("could not set search_path"))));

	/* pin the start of new_materialization to the end of new_materialization,
	 * we are not allowed to materialize beyond that point
	 */
	if (new_materialization_range.start > new_materialization_range.end)
		new_materialization_range.start = new_materialization_range.end;

	if (range_length(invalidation_range) > 0)
	{
		Assert(invalidation_range.start <= invalidation_range.end);

		/* we never materialize beyond the new materialization range */
		if (invalidation_range.start >= new_materialization_range.end ||
			invalidation_range.end > new_materialization_range.end)
			elog(ERROR, "internal error: invalidation range ahead of new materialization range");

		/* If the invalidation and new materialization ranges overlap, materialize in one go */
		materialize_invalidations_separately =
			!ranges_overlap(invalidation_range, new_materialization_range);

		combined_materialization_range.start =
			int64_min(invalidation_range.start, new_materialization_range.start);
	}

	/* Then insert the materializations.
	 * We insert them in two groups:
	 * [lowest_invalidated, greatest_invalidated] and
	 * [start_of_new_materialization, end_of_new_materialization]
	 * eventually, we may want more precise deletions and insertions for the invalidated ranges.
	 * if greatest_invalidated == end_of_new_materialization then we only perform 1 insertion.
	 * to prevent values from being inserted multiple times.
	 */
	if (range_length(invalidation_range) == 0 || !materialize_invalidations_separately)
	{
		spi_update_materializations(mat_ht,
									partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(
										combined_materialization_range),
									chunk_id);
	}
	else
	{
		spi_update_materializations(mat_ht,
									partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(invalidation_range),
									chunk_id);

		spi_update_materializations(mat_ht,
									partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(new_materialization_range),
									chunk_id);
	}
}

static bool
ranges_overlap(InternalTimeRange invalidation_range, InternalTimeRange new_materialization_range)
{
	Assert(invalidation_range.start <= invalidation_range.end);
	Assert(new_materialization_range.start <= new_materialization_range.end);
	return !(invalidation_range.end < new_materialization_range.start ||
			 new_materialization_range.end < invalidation_range.start);
}

static int64
range_length(const InternalTimeRange range)
{
	Assert(range.end >= range.start);

	return int64_saturating_sub(range.end, range.start);
}

static Datum
time_range_internal_to_min_time_value(Oid type)
{
	switch (type)
	{
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOBEGIN);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOBEGIN);
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOBEGIN);
		default:
			return ts_internal_to_time_value(PG_INT64_MIN, type);
	}
}

static Datum
time_range_internal_to_max_time_value(Oid type)
{
	switch (type)
	{
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOEND);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOEND);
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOEND);
			break;
		default:
			return ts_internal_to_time_value(PG_INT64_MAX, type);
	}
}

static Datum
internal_to_time_value_or_infinite(int64 internal, Oid time_type, bool *is_infinite_out)
{
	/* MIN and MAX can occur due to NULL thresholds, or due to a lack of invalidations. Since our
	 * regular conversion function errors in those cases, and we want to use those as markers for an
	 * open threshold in one direction, we special case this here*/
	if (internal == PG_INT64_MIN)
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = true;
		return time_range_internal_to_min_time_value(time_type);
	}
	else if (internal == PG_INT64_MAX)
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = true;
		return time_range_internal_to_max_time_value(time_type);
	}
	else
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = false;
		return ts_internal_to_time_value(internal, time_type);
	}
}

static TimeRange
internal_time_range_to_time_range(InternalTimeRange internal)
{
	TimeRange range;
	range.type = internal.type;

	range.start = internal_to_time_value_or_infinite(internal.start, internal.type, NULL);
	range.end = internal_to_time_value_or_infinite(internal.end, internal.type, NULL);

	return range;
}

static void
spi_update_materializations(Hypertable *mat_ht, SchemaAndName partial_view,
							SchemaAndName materialization_table, const NameData *time_column_name,
							TimeRange invalidation_range, const int32 chunk_id)
{
	StringInfo chunk_condition = makeStringInfo();

	/*
	 * chunk_id is valid if the materializaion update should be done only on the given chunk.
	 * This is used currently for refresh on chunk drop only. In other cases, manual
	 * call to refresh_continuous_aggregate or call from a refresh policy, chunk_id is
	 * not provided, i.e., invalid.
	 */
	if (chunk_id != INVALID_CHUNK_ID)
		appendStringInfo(chunk_condition, "AND chunk_id = %d", chunk_id);

	spi_delete_materializations(materialization_table,
								time_column_name,
								invalidation_range,
								chunk_condition->data);
	spi_insert_materializations(mat_ht,
								partial_view,
								materialization_table,
								time_column_name,
								invalidation_range,
								chunk_condition->data);
}

static void
spi_delete_materializations(SchemaAndName materialization_table, const NameData *time_column_name,
							TimeRange invalidation_range, const char *const chunk_condition)
{
	StringInfo command = makeStringInfo();
	Oid out_fn;
	bool type_is_varlena;
	char *invalidation_start;
	char *invalidation_end;

	getTypeOutputInfo(invalidation_range.type, &out_fn, &type_is_varlena);
	invalidation_start = OidOutputFunctionCall(out_fn, invalidation_range.start);
	invalidation_end = OidOutputFunctionCall(out_fn, invalidation_range.end);

	appendStringInfo(command,
					 "DELETE FROM %s.%s AS D WHERE "
					 "D.%s >= %s AND D.%s < %s %s;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(invalidation_start),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(invalidation_end),
					 chunk_condition);

	int64 rows_processed = 0;
	PlannedStmt *plan = sql_create_plan(command->data);
	DestReceiver *receiver = CreateDestReceiver(DestNone);
	QueryDesc *queryDesc;
	
	PG_TRY();
	{
		queryDesc = sql_execute_plan(command->data, plan, receiver);
		rows_processed = queryDesc->estate->es_processed;
	}
	PG_CATCH();
	{
		rows_processed = -1;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (queryDesc)
		sql_close_plan(queryDesc, receiver);

	if (rows_processed < 0)
		elog(ERROR,
			 "could not delete old values from materialization table \"%s.%s\"",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
	else
		elog(LOG,
			 "deleted " UINT64_FORMAT " row(s) from materialization table \"%s.%s\"",
			 SPI_processed,
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
}

static void
spi_insert_materializations(Hypertable *mat_ht, SchemaAndName partial_view,
							SchemaAndName materialization_table, const NameData *time_column_name,
							TimeRange materialization_range, const char *const chunk_condition)
{
	StringInfo command = makeStringInfo();
	Oid out_fn;
	bool type_is_varlena;
	char *materialization_start;
	char *materialization_end;

	getTypeOutputInfo(materialization_range.type, &out_fn, &type_is_varlena);
	materialization_start = OidOutputFunctionCall(out_fn, materialization_range.start);
	materialization_end = OidOutputFunctionCall(out_fn, materialization_range.end);

	appendStringInfo(command,
					 "INSERT INTO %s.%s SELECT * FROM %s.%s AS I "
					 "WHERE I.%s >= %s AND I.%s < %s %s;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*partial_view.schema)),
					 quote_identifier(NameStr(*partial_view.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(materialization_start),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(materialization_end),
					 chunk_condition);

	int64 rows_processed = 0;
	PlannedStmt *plan = sql_create_plan(command->data);
	DestReceiver *receiver = CreateDestReceiver(DestNone);
	QueryDesc *queryDesc;
	
	PG_TRY();
	{
		queryDesc = sql_execute_plan(command->data, plan, receiver);
		rows_processed = queryDesc->estate->es_processed;
	}
	PG_CATCH();
	{
		rows_processed = -1;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (queryDesc)
		sql_close_plan(queryDesc, receiver);

	if (rows_processed < 0)
		elog(ERROR,
			 "could not materialize values into the materialization table \"%s.%s\"",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
	else
		elog(LOG,
			 "inserted " UINT64_FORMAT " row(s) into materialization table \"%s.%s\"",
			 rows_processed,
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));

	/* Get the max(time_dimension) of the materialized data */
	if (rows_processed > 0)
	{
		resetStringInfo(command);
		appendStringInfo(command,
						 "SELECT %s FROM %s.%s AS I "
						 "WHERE I.%s >= %s %s "
						 "ORDER BY 1 DESC LIMIT 1;",
						 quote_identifier(NameStr(*time_column_name)),
						 quote_identifier(NameStr(*materialization_table.schema)),
						 quote_identifier(NameStr(*materialization_table.name)),
						 quote_identifier(NameStr(*time_column_name)),
						 quote_literal_cstr(materialization_start),
						 chunk_condition);

		rows_processed = 0;
		plan = sql_create_plan(command->data);
		receiver = CreateDestReceiver(DestTuplestore);

		Tuplestorestate *tuplestore = tuplestore_begin_heap(false, false, work_mem);
		SetTuplestoreDestReceiverParams(receiver,
										tuplestore,
										CurrentMemoryContext,
										false,
										NULL,
										NULL);

		PG_TRY();
		{		
			TupleTableSlot *slot;
			MemoryContext oldctx;
			bool found;
			int64 watermark;
			bool isnull;
			Datum maxdat;

			queryDesc = sql_execute_plan(command->data, plan, receiver);
			oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(queryDesc->estate));
			slot = MakeSingleTupleTableSlot(queryDesc->tupDesc, &TTSOpsMinimalTuple);
			found = tuplestore_gettupleslot(tuplestore, true, false, slot);

			if (found)
			{
				maxdat = slot_getattr(slot, 1, &isnull);

				if (!isnull)
				{
					watermark = ts_time_value_to_internal(maxdat, materialization_range.type);
					ts_cagg_watermark_update(mat_ht, watermark, isnull, false);
				}
			}

			MemoryContextSwitchTo(oldctx);
			
		}
		PG_CATCH();
		{
			rows_processed = -1;
			PG_RE_THROW();
		}
		PG_END_TRY();

		if (queryDesc)
			sql_close_plan(queryDesc, receiver);
	}
}
/*
 * Initialize MatTableColumnInfo.
 */
void
mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *grouplist)
{
	matcolinfo->matcollist = NIL;
	matcolinfo->partial_seltlist = NIL;
	matcolinfo->partial_grouplist = grouplist;
	matcolinfo->mat_groupcolname_list = NIL;
	matcolinfo->matpartcolno = -1;
	matcolinfo->matpartcolname = NULL;
}
