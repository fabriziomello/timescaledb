/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "activity/ts_backend_progress.h"
#include "activity/ts_backend_status.h"

/*
 * ts_stat_progress_start_command()
 */
void
ts_stat_progress_start_command(TsProgressCommandType cmdtype, Oid relid)
{
	volatile TsBackendStatus *tsbeentry = TsMyBEEntry;

	if (!tsbeentry /*|| !pgstat_track_activities*/)
		return;

	TS_STAT_BEGIN_WRITE_ACTIVITY(tsbeentry);
	tsbeentry->ts_progress_type = cmdtype;
	tsbeentry->relid = relid;
	MemSet(&tsbeentry->params, 0, sizeof(tsbeentry->params));
	TS_STAT_END_WRITE_ACTIVITY(tsbeentry);
}

/*
 * ts_stat_progress_update_param()
 */
void
ts_stat_progress_update_param(int index, int64 val)
{
	volatile TsBackendStatus *tsbeentry = TsMyBEEntry;

	Assert(index >= 0 && index < PGSTAT_NUM_PROGRESS_PARAM);

	if (!tsbeentry /*|| !pgstat_track_activities*/)
		return;

	TS_STAT_BEGIN_WRITE_ACTIVITY(tsbeentry);
	tsbeentry->params[index] = val;
	TS_STAT_END_WRITE_ACTIVITY(tsbeentry);
}

/*
 * ts_stat_progress_incr_param()
 */
void
ts_stat_progress_incr_param(int index, int64 incr)
{
	volatile TsBackendStatus *tsbeentry = TsMyBEEntry;

	Assert(index >= 0 && index < PGSTAT_NUM_PROGRESS_PARAM);

	if (!tsbeentry /*|| !pgstat_track_activities*/)
		return;

	TS_STAT_BEGIN_WRITE_ACTIVITY(tsbeentry);
	tsbeentry->params[index] += incr;
	TS_STAT_END_WRITE_ACTIVITY(tsbeentry);
}

/*
 * ts_stat_progress_update_multi_param()
 */
void
ts_stat_progress_update_multi_param(int nparam, const int *index, const int64 *val)
{
	volatile TsBackendStatus *tsbeentry = TsMyBEEntry;
	int i;

	if (!tsbeentry /*|| !pgstat_track_activities*/ || nparam == 0)
		return;

	TS_STAT_BEGIN_WRITE_ACTIVITY(tsbeentry);

	for (i = 0; i < nparam; ++i)
	{
		Assert(index[i] >= 0 && index[i] < PGSTAT_NUM_PROGRESS_PARAM);

		tsbeentry->params[index[i]] = val[i];
	}

	TS_STAT_END_WRITE_ACTIVITY(tsbeentry);
}

/*
 * pgstat_progress_end_command()
 */
void
ts_stat_progress_end_command(void)
{
	volatile TsBackendStatus *tsbeentry = TsMyBEEntry;

	if (!tsbeentry /*|| !pgstat_track_activities*/)
		return;

	if (tsbeentry->ts_progress_type == PROGRESS_COMMAND_INVALID)
		return;

	TS_STAT_BEGIN_WRITE_ACTIVITY(tsbeentry);
	tsbeentry->ts_progress_type = PROGRESS_COMMAND_INVALID;
	tsbeentry->relid = InvalidOid;
	TS_STAT_END_WRITE_ACTIVITY(tsbeentry);
}
