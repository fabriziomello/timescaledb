/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

#include "invalidation.h"
#include "materialize.h"

extern Datum continuous_agg_refresh(PG_FUNCTION_ARGS);
extern void continuous_agg_calculate_merged_refresh_window(
	const ContinuousAgg *cagg, const InternalTimeRange *refresh_window,
	const InvalidationStore *invalidations, const ContinuousAggsBucketFunction *bucket_function,
	InternalTimeRange *merged_refresh_window, const CaggRefreshContext context);
extern void
continuous_agg_refresh_internal(const ContinuousAgg *cagg, const InternalTimeRange *refresh_window,
								const CaggRefreshContext context, const bool start_isnull,
								const bool end_isnull, bool bucketing_refresh_window, bool force,
								bool process_hypertable_invalidations, bool extend_last_bucket);
extern List *continuous_agg_split_refresh_window(ContinuousAgg *cagg,
												 InternalTimeRange *original_refresh_window,
												 int32 buckets_per_batch,
												 bool refresh_newest_first);
