/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include <storage/lwlock.h>
#include <storage/shmem.h>

#include "activity/ts_backend_status.h"
#include "loader/ts_backend_status.h"

#define TS_BACKEND_STATUS_NAME "ts_backend_status_array"
#define TS_BACKEND_STATUS_TRANCHE_NAME "ts_backend_status_array_tranche"

/*
 * Initialize the shared status array
 */
void
ts_backend_status_shmem_alloc(void)
{
	/* Create or attach to the shared array */
	Size size = mul_size(sizeof(TsBackendStatus), MaxBackends);

	RequestAddinShmemSpace(size);
	RequestNamedLWLockTranche(TS_BACKEND_STATUS_TRANCHE_NAME, 1);
}

void
ts_backend_status_shmem_startup(void)
{
	bool found;
	Size size = mul_size(sizeof(TsBackendStatus), MaxBackends);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	TsBackendStatusArray =
		(TsBackendStatus *) ShmemInitStruct(TS_BACKEND_STATUS_NAME, size, &found);

	if (!found)
	{
		MemSet(TsBackendStatusArray, 0, size);
	}
	LWLockRelease(AddinShmemInitLock);
}
