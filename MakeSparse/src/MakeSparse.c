/* Standard BSD license disclaimer.

Copyright(c) 2016-2021, Lance D. Stringham
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include "targetver.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
// Necessary due to WIN32_LEAN_AND_MEAN
#include <winioctl.h>

#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>

#include <assert.h>

#include <SparseFileLib.h>

#define DEFAULT_EXE_NAME        L"MakeSparse.exe"

/* TODO probably never: Handle file analysis and deallocation for files roughly
 * 60 TiB or more (assuming 4k clusters) for 32-bit builds since we may start
 * running into memory issues with only 2 GiB of usable memory normally
 * available to the process. */

/* This value will be used if the cluster size of the filesystem cannot be
 * determined automatically. */
#define DEFAULT_FS_CLUSTER_SIZE     4096

/* 10 seconds in milliseconds */
#define STATS_TIMER_INTERVAL_MS  (10 * 1000)


static DWORD
SetSparseRange(
	_In_    HANDLE      FileHandle,
	_In_    LONGLONG    FileOffset,
	_In_    LONGLONG    BeyondFinalZero
	)
{
	DWORD   errRet;

	FILE_ZERO_DATA_INFORMATION fzdi;

	fzdi.FileOffset.QuadPart      = FileOffset;
	fzdi.BeyondFinalZero.QuadPart = BeyondFinalZero;

	if (!DeviceIoControl(FileHandle,
	                     FSCTL_SET_ZERO_DATA,
	                     &fzdi,
	                     sizeof(fzdi),
	                     NULL,
	                     0,
	                     NULL,
	                     NULL))
	{
		errRet = GetLastError();
	} else {
		errRet = ERROR_SUCCESS;
	}
	return errRet;
}

// TODO: come back and completely rewrite this logic!
/* TODO: Definitively determine if this should send a zero ioctl for every
 * empty cluster or only for larger cluster groups. Also need to see if cluster
 * groups should be aligned. */
static DWORD
SetSparseRanges(
	_Inout_ HANDLE          FileHandle,
	_In_    UINT64          FileSize,
	_In_    SIZE_T          ClusterSize,
	_In_    DWORD           MinClusterGroup,
	_In_    PCLUSTER_MAP    ZeroClusterMap
	)
{
	UINT64  i, numClusters, runtBytes;
	INT64   firstClusterInSequence;
	DWORD   errRet;

	errRet = ERROR_SUCCESS;

	numClusters = FileSize / ClusterSize;
	firstClusterInSequence = -1;

	for (i = 0; i < numClusters; ++i) {
		if (ClusterMapIsMarkedZero(ZeroClusterMap, i)) {
			if (firstClusterInSequence < 0)
				firstClusterInSequence = (INT64)i;
		} else {
			if ((firstClusterInSequence >= 0)
				&& (MinClusterGroup <= (i - firstClusterInSequence))) {
				errRet = SetSparseRange(FileHandle,
				                        firstClusterInSequence * ClusterSize,
				                        i * ClusterSize);
				if (errRet != ERROR_SUCCESS) {
					LogError(L"Error %#llx returned from SetSparseRange call.\n",
					         (long long)errRet);
					goto func_return;
				}
			}
			firstClusterInSequence = -1;
		}
		// TODO: replace this.
		//LogProgressOnInterval(IoCbShared);
	}

	runtBytes = FileSize % ClusterSize;
	if (runtBytes) {
		if (!ClusterMapIsMarkedZero(ZeroClusterMap, i)) {
			runtBytes = 0;
		}
		// Don't bother zeroing a runt by itself.
	}
	if ((firstClusterInSequence >= 0)
		&& (MinClusterGroup <= (i - firstClusterInSequence))) {
		errRet = SetSparseRange(FileHandle,
		                        firstClusterInSequence * ClusterSize,
		                        (i * ClusterSize) + runtBytes);
		if (errRet != ERROR_SUCCESS) {
			LogError(L"Error %#llx returned from SetSparseRange call.\n",
			         (long long)errRet);
			goto func_return;
		}
	}

func_return:
	return errRet;
}


static DWORD
SetSparseAttribute(
	_In_    HANDLE  FileHandle
	)
{
	DWORD                  errRet, tmp;
	FILE_SET_SPARSE_BUFFER fssb;

	fssb.SetSparse = TRUE;
	if (!DeviceIoControl(FileHandle,
		                 FSCTL_SET_SPARSE,
		                 &fssb,
		                 sizeof(fssb),
		                 NULL,
		                 0,
		                 &tmp,
		                 NULL))
	{
		errRet = GetLastError();
	} else {
		errRet = ERROR_SUCCESS;
	}
	return errRet;
}


static VOID
PrintUsageInfo(
	_In_    LPWSTR      ExeName
	)
{
	// TODO: Make this better.
	LogInfo(L"%s [-p] [-m] Path\\To\\FileToMakeSparse.ext\n"
	        L"\tSpecify -p to preserve file timestamps.\n"
	        L"\tSpecify -m to print map of zero clusters.\n",
	        ExeName);
}


// This is exceedingly lightweight and begging to be replaced. I don't really
// want to write an entire command line parsing library and I wasn't seeing
// anything online with a license that wasn't GPL or didn't contain a mandatory
// advertising clause. This code sucks. But since we're only dealing with a
// tiny set of args, it's good enough for now...
/* Returns 0 for success, non-zero otherwise. */
_Success_(return == 0)
static int
ParseCommandLine(
	_In_        int         argc,
	_In_        WCHAR       **argv,
	_Out_ _Post_valid_
	            LPWSTR      *InvocationName,
	_Out_       BOOL        *PreserveFileTimes,
	_Out_       BOOL        *PrintSparseMap,
	_Out_       LPWSTR      *FileName
	)
{
	int     ret, i;
	BOOL    flTimes, sMap;

	ret = -1;
	flTimes = 0;
	sMap = 0;

	/* Check for funny business with the invocation method */
	if (argc) {
		*InvocationName = argv[0];
	} else {
		*InvocationName = DEFAULT_EXE_NAME;
		goto func_return;
	}

	/* Validate we have expected number of arguments. */
	if (argc < 2 || argc > 4 ) {
		goto func_return;
	}

	for (i = 1; i < (argc - 1); ++i) {
		if (!wcscmp(argv[i], L"-p")) {
			flTimes = 1;
		} else if (!wcscmp(argv[i], L"-m")) {
			sMap = 1;
		} else {
			goto func_return;
		}
	}

	*PreserveFileTimes = flTimes;
	*PrintSparseMap = sMap;
	*FileName = argv[i];
	ret = 0;

func_return:
	return (ret);
}


int
wmain(
	int         argc,
	WCHAR       **argv
	)
{
	SIZE_T          fsClusterSize;
	HANDLE          fl;
	BOOL            prsvTm, printSparseMap;
	LPWSTR          invocationName, flName;
	FILETIME        tmCrt;
	FILETIME        tmAcc;
	FILETIME        tmWrt;
	LARGE_INTEGER   flSz;
	UINT64          startQPCVal, hours, minutes, seconds;
	DWORD           errRet;
	PCLUSTER_MAP    zeroClusterMap;
	int             retVal;

	fl = NULL;
	zeroClusterMap = NULL;

	SparseFileLibInit();

	startQPCVal = GetQPCVal();

	if (ParseCommandLine(argc, argv, &invocationName,
	                     &prsvTm, &printSparseMap, &flName)) {
		PrintUsageInfo(invocationName);
		return EXIT_FAILURE;
	}

	LogInfo(L"Opening file %s\n", flName);

	fl = OpenFileExclusive(flName,
	                       FILE_FLAG_SEQUENTIAL_SCAN,
	                       &flSz,
	                       &fsClusterSize,
	                       &tmCrt,
	                       &tmAcc,
	                       &tmWrt);
	if (NULL == fl) {
		LogError(L"Failed to open file %s with error %#llx\n",
		         flName, (long long)GetLastError());
		goto error_return;
	}

	if (fsClusterSize == 0) {
		fsClusterSize = DEFAULT_FS_CLUSTER_SIZE;
		LogInfo(L"Unable to determine cluster size of file system. "
		        L"Using default cluster size: %ld\n",
		        (LONG)fsClusterSize);
	} else {
		LogInfo(L"Cluster size: %ld\n", (LONG)fsClusterSize);
	}

	LogInfo(L"Starting file analysis.\n");
	if (!BuildSparseMap(fl, stdout, STATS_TIMER_INTERVAL_MS, &fsClusterSize,
	                    &zeroClusterMap)) {
		LogError(L"Failed BuildSparseMap with error %#llx\n",
		         (long long)GetLastError());
		goto error_return;
	}

	LogInfo(L"Completed file analysis. Starting to dispatch zero ranges to file system.\n");

	// TODO: Don't blindly set this if no zero clusters detected.
	errRet = SetSparseAttribute(fl);
	if (ERROR_SUCCESS != errRet) {
		LogError(L"Error %#llx from SetSparseAttribute call.\n",
		         (long long)errRet);
		goto error_return;
	}

	errRet = SetSparseRanges(fl,
	                         (UINT64)flSz.QuadPart,
	                         fsClusterSize,
	                         1,
	                         zeroClusterMap);
	if (ERROR_SUCCESS != errRet) {
		LogError(L"Error %#llx from SetSparseRanges call.\n",
		          (long long)errRet);
		goto error_return;
	}

	LogInfo(L"Marking zero ranges complete.\n");

	/* Reset modified and access timestamps if preserve filetimes specified */
	if (prsvTm) {
		if (0 == SetFileTime(fl, NULL, &tmAcc, &tmWrt)) {
			LogError(L"WARNING: Failed to preserve file times on file.\n");
		}
	}

	/* Flush buffers on file */
	if (!FlushFileBuffers(fl)) {
		LogError(L"WARNING: Failed FlushFileBuffers on target file with lastErr %lu.\n", GetLastError());
	}

	// What would we do if this failed anyways?
	(void)CloseHandle(fl);
	fl = NULL;

	seconds = ElapsedQPCInSeconds(startQPCVal, GetQPCVal());
	hours = seconds / (60 * 60);
	seconds = seconds % (60 * 60);
	minutes = seconds / 60;
	seconds = seconds % 60;
	LogInfo(L"Completed processing in: %llu hours, %llu minutes, %llu seconds\n",
	        hours, minutes, seconds);

	if (printSparseMap) {
		LogInfo(L"Printing sparse cluster map\n");
		ClusterMapPrint(zeroClusterMap, stdout);
	}

	ClusterMapFree(zeroClusterMap);
	zeroClusterMap = NULL;

	retVal = EXIT_SUCCESS;
	goto func_return;

error_return:
	retVal = EXIT_FAILURE;

func_return:
	if (fl)
		(void)CloseHandle(fl);
	if (zeroClusterMap)
		ClusterMapFree(zeroClusterMap);

	return retVal;
}

