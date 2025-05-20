/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include "activity/ts_backend_status.h"

void
_backend_status_init(void)
{
	TsMyBEEntry = &TsBackendStatusArray[MyProcNumber];
	TsMyBEEntry->pgbeentry = MyBEEntry;
}