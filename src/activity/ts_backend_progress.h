/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

typedef enum TsProgressCommandType
{
	TS_PROGRESS_INVALID,
	TS_PROGRESS_CAGG_REFRESH,
} TsProgressCommandType;

#define TS_PROGRESS_CAGG_REFRESH_PHASE 0
#define TS_PROGRESS_CAGG_REFRESH_JOB 1
#define TS_PROGRESS_CAGG_REFRESH_BATCH 2
#define TS_PROGRESS_CAGG_REFRESH_TOTAL 3

void ts_stat_progress_start_command(TsProgressCommandType cmdtype, Oid relid);
void ts_stat_progress_update_param(int index, int64 val);
void ts_stat_progress_incr_param(int index, int64 incr);
void ts_stat_progress_end_command(void);
