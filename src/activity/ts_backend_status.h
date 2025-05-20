/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <utils/backend_progress.h>
#include <utils/backend_status.h>

#include "activity/ts_backend_progress.h"

typedef struct TsBackendStatus
{
	/*
	 * To avoid locking overhead, we use the following protocol: a backend
	 * increments st_changecount before modifying its entry, and again after
	 * finishing a modification.  A would-be reader should note the value of
	 * st_changecount, copy the entry into private memory, then check
	 * st_changecount again.  If the value hasn't changed, and if it's even,
	 * the copy is valid; otherwise start over.  This makes updates cheap
	 * while reads are potentially expensive, but that's the tradeoff we want.
	 *
	 * The above protocol needs memory barriers to ensure that the apparent
	 * order of execution is as it desires.  Otherwise, for example, the CPU
	 * might rearrange the code so that st_changecount is incremented twice
	 * before the modification on a machine with weak memory ordering.  Hence,
	 * use the macros defined below for manipulating st_changecount, rather
	 * than touching it directly.
	 */
	int changecount;

	TsProgressCommandType ts_progress_type;
	Oid relid;
	int64 params[PGSTAT_NUM_PROGRESS_PARAM];

	/* Postgres backend status entry */
	PgBackendStatus *pgbeentry;
} TsBackendStatus;

#define TS_STAT_BEGIN_WRITE_ACTIVITY(tsbeentry)                                                    \
	do                                                                                             \
	{                                                                                              \
		START_CRIT_SECTION();                                                                      \
		(tsbeentry)->changecount++;                                                                \
		pg_write_barrier();                                                                        \
	} while (0)

#define TS_STAT_END_WRITE_ACTIVITY(tsbeentry)                                                      \
	do                                                                                             \
	{                                                                                              \
		pg_write_barrier();                                                                        \
		(tsbeentry)->changecount++;                                                                \
		Assert(((tsbeentry)->changecount & 1) == 0);                                               \
		END_CRIT_SECTION();                                                                        \
	} while (0)

PGDLLEXPORT TsBackendStatus *TsBackendStatusArray = NULL;
PGDLLEXPORT TsBackendStatus *TsMyBEEntry;

void _backend_status_init(void);
