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
#include <Windows.h>

#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdarg.h>

#include <assert.h>

#include "SparseFileLib.h"

// This gets initialized by SparseFileLibInit
static UINT64 QPCFrequency;


// internal type declarations that are hidden from consumers
struct CLUSTER_MAP {
	const UINT64        FileSize;
	const DWORD         ClusterShift;
	volatile LONG       ClusterMap[ANYSIZE_ARRAY];
};
typedef struct CLUSTER_MAP CLUSTER_MAP;


_Use_decl_annotations_
void __cdecl
LogError(
	LPCWSTR         FormatString,
	...
	)
{
	va_list varArgs;
	va_start(varArgs, FormatString);

	// TODO: append a newline to the format string.

	(void)vfwprintf(stderr, FormatString, varArgs);
}


_Use_decl_annotations_
void __cdecl
LogInfo(
	LPCWSTR         FormatString,
	...
	)
{
	va_list varArgs;
	va_start(varArgs, FormatString);

	// TODO: append a newline to the format string.

	(void)vfwprintf(stdout, FormatString, varArgs);
}


// TODO: Figure out the most efficient method of checking for zeros.
_Use_decl_annotations_
BOOL __stdcall
IsZeroBuf(
	LPVOID          Buf,
	DWORD           BufSz
	)
{
	char *p, *e;

	p = Buf;
	e = p + BufSz;

	while (*p == 0 && ++p < e);

	return (p >= e);
}


_Use_decl_annotations_
PCLUSTER_MAP __stdcall
ClusterMapAllocate(
	DWORD           ClusterSize,
	UINT64          FileSize
	)
{
	PCLUSTER_MAP    clusterMap;
	UINT64          allocSize;
	DWORD           clusterShift;

	clusterMap = NULL;

	if (!BitScanReverse(&clusterShift, ClusterSize)) {
		SetLastError(ERROR_INVALID_PARAMETER);
		goto func_return;
	}

	// offsetof to calculate the base struct data size.
	// FileSize / ClusterSize for floored number of clusters.
	// / 32 since thats the number of bits in a LONG
	// + 1 since the number of clusters may not be evenly divisiable by 32.
	// * sizeof(LONG) to actually figure out the number of bytes we need for the map.
	allocSize = offsetof(CLUSTER_MAP, ClusterMap)
	          + ((((FileSize >> clusterShift) / 32) + 1) * sizeof(LONG));


#ifdef _M_IX86
	if (allocSize >= INT32_MAX) {
		LogError(L"Insufficient addressable address space for file map. Use 64-bit build.");
		SetLastError(ERROR_INSUFFICIENT_BUFFER);
		goto func_return;
	}
#endif

	clusterMap = calloc(1, (SIZE_T)allocSize);
	if (!clusterMap) {
		SetLastError(ERROR_OUTOFMEMORY);
		goto func_return;
	}

	// We need to cast away the const values to make the assignments.
	*(UINT64 *)&clusterMap->FileSize     = FileSize;
	*(DWORD  *)&clusterMap->ClusterShift = clusterShift;

func_return:
	return clusterMap;
}


_Use_decl_annotations_
void __stdcall
ClusterMapFree(
	PCLUSTER_MAP    ClusterMap
	)
{
	free(ClusterMap);
}


_Use_decl_annotations_
void __stdcall
ClusterMapMarkZero(
	PCLUSTER_MAP    ClusterMap,
	UINT64          StartingByteOffset
	)
{
	UINT64 mapBit;

	assert(!(StartingByteOffset & (ClusterMap->ClusterShift - 1)));
	assert(  StartingByteOffset <  ClusterMap->FileSize);

	mapBit = StartingByteOffset >> ClusterMap->ClusterShift;
	InterlockedBitTestAndSet(ClusterMap->ClusterMap + (mapBit / 32), mapBit & 31);
}


_Use_decl_annotations_
BOOL __stdcall
ClusterMapIsMarkedZero(
	PCLUSTER_MAP    ClusterMap,
	UINT64          Cluster
	)
{
	/* (Cluster / 32) finds the dword in the map where the bit is stored.
	 * (1 << (Cluster & 31)) creates the bitmask to extract the bit we want.
	 * & extracts just the bit we care about.
	 * !! forces exactly 0 or 1 to be returned without using a conditional.
	**/
	return !!(*(ClusterMap->ClusterMap + (Cluster / 32)) & (1 << (Cluster & 31)));
}


_Use_decl_annotations_
void __stdcall
ClusterMapPrint(
	PCLUSTER_MAP    ClusterMap,
	FILE            *FileStream
	)
{
	UINT64 numClusters, i, displayGroups;

	numClusters = ClusterMap->FileSize >> ClusterMap->ClusterShift;
	if (ClusterMap->FileSize & (((UINT64)1 << ClusterMap->ClusterShift) - 1))
		++numClusters;

	fwprintf(FileStream, L"%-18s Cluster size = %d, 0 = empty cluster, 1 = data cluster",
	                     L"File Offset", (1 << ClusterMap->ClusterShift));

	for (i = 0, displayGroups = 0; i < numClusters; ++i) {
		if (!(displayGroups % 16) && !(i % 4))
			fwprintf(FileStream, L"\n0x%016llX", i << ClusterMap->ClusterShift);
		if (!(i % 4)) {
			++displayGroups;
			fwprintf(FileStream, L" ");
		}
		fwprintf(FileStream, ClusterMapIsMarkedZero(ClusterMap, i) ? L"0" : L"1");
	}
	fwprintf(FileStream, L"\n");
}


/* TODO: For windows 8 / server 2012 use GetFileInformationByHandleEx function
 * to query OS about the file sector size and alignment rather then the current
 * method of parsing the file handle path to open a handle to the drive and
 * using GetDiskFreeSpace to calculate the cluster size and assuming zero based
 * alignment. */
_Use_decl_annotations_
SIZE_T __stdcall
GetVolumeClusterSizeFromFileHandle(
	HANDLE      FileHandle
	)
{
	DWORD   sectorsPerCluster;
	DWORD   bytesPerSector;
	DWORD   numberOfFreeClusters;
	DWORD   totalNumberOfClusters;
	DWORD   flNameLen;
	DWORD   tmp;
	SIZE_T  clusterSize;
	int     delimCnt;
	WCHAR   *ofst;
	WCHAR   *flName;

	clusterSize = 0;

	/* Determine buffer size to allocate. */
	flNameLen = GetFinalPathNameByHandleW(FileHandle, NULL, 0, VOLUME_NAME_GUID);
	if (0 == flNameLen)
		goto func_return;

	flName = calloc(1, sizeof(WCHAR) * ((SIZE_T)flNameLen + 1));
	if (NULL == flName) {
		SetLastError(ERROR_NOT_ENOUGH_MEMORY);
		goto func_return;
	}

	tmp = GetFinalPathNameByHandleW(FileHandle, flName, flNameLen + 1, VOLUME_NAME_GUID);
	if (0 == tmp || flNameLen < tmp)
		goto cleanup_return;

	// DEBUG
	//LogInfo(L"Filename: %s\n", flName);

	ofst = flName;
	delimCnt = 0;
	while (delimCnt < 4 && *ofst != L'\0') {
		if (*ofst == L'\\')
			delimCnt++;
		ofst = CharNextW(ofst);
	}

	if (delimCnt != 4) {
		SetLastError(ERROR_DEVICE_FEATURE_NOT_SUPPORTED);
		goto cleanup_return;
	}

	if ((flName + flNameLen) - ofst <= 0) {
		SetLastError(ERROR_DEVICE_FEATURE_NOT_SUPPORTED);
		goto cleanup_return;
	}

	*ofst = L'\0'; /* Terminate the string after last delimiter. */
	//LogInfo(L"Filename: %s\n", flName);

	if (FALSE == GetDiskFreeSpaceW(flName,
	                               &sectorsPerCluster,
	                               &bytesPerSector,
	                               &numberOfFreeClusters,
	                               &totalNumberOfClusters))
		goto cleanup_return;

	clusterSize = (SIZE_T)bytesPerSector * (SIZE_T)sectorsPerCluster;

cleanup_return:
	free(flName);

func_return:
	return (clusterSize);
}


_Use_decl_annotations_
HANDLE __stdcall
OpenFileExclusive(
	LPCWSTR         Filename,
	DWORD           FileFlagsAttributes,
	PLARGE_INTEGER  FileSize,
	PSIZE_T         FsClusterSize,
	LPFILETIME      CreationTime,
	LPFILETIME      LastAccessTime,
	LPFILETIME      LastWriteTime
	)
{
	HANDLE      fl;
	SIZE_T      fsClusterSize;
	DWORD       err;
	DWORD       nonFatalErr;

	nonFatalErr = ERROR_SUCCESS;

	fl = CreateFileW(Filename,                      // user supplied filename
	                 GENERIC_READ | GENERIC_WRITE,  // read/write
	                 0,                             // do not share
	                 NULL,                          // default security
	                 OPEN_EXISTING,                 // creation disp
	                 FileFlagsAttributes,           // user specified
	                 NULL);                         // No template

	if (INVALID_HANDLE_VALUE == fl)
		goto error_return;

	if (NULL != FsClusterSize) {
		*FsClusterSize = 0;
		if ((fsClusterSize = GetVolumeClusterSizeFromFileHandle(fl)) < 1) {
			nonFatalErr = GetLastError();
		} else {
			*FsClusterSize = fsClusterSize;
		}
	}

	if (CreationTime || LastAccessTime || LastWriteTime)
		if (FALSE == GetFileTime(fl, CreationTime, LastAccessTime, LastWriteTime))
			goto error_return;

	if (FALSE == GetFileSizeEx(fl, FileSize))
		goto error_return;

	if (ERROR_SUCCESS != nonFatalErr)
		SetLastError(nonFatalErr);

	goto func_return;

error_return:
	if (INVALID_HANDLE_VALUE != fl) {
		err = GetLastError();
		CloseHandle(fl);
		SetLastError(err);
	}
	fl = NULL;

func_return:
	return fl;
}


_Use_decl_annotations_
DWORD __stdcall
SetFileSize(
	HANDLE          File,
	LARGE_INTEGER   NewFileSize
	)
{
	DWORD lastErr;

	lastErr = ERROR_SUCCESS;

	if (!SetFilePointerEx(File, NewFileSize, NULL, FILE_BEGIN)) {
		lastErr = GetLastError();
		goto func_return;
	}
	if (0 == SetEndOfFile(File)) {
		lastErr = GetLastError();
		goto func_return;
	}

func_return:
	return lastErr;
}


#define MAX_FILE_VIEW_SIZE (512 * 1024 * 1024)


/* TODO: Query existing sparse ranges and don't re-analyze them. */
_Success_(return == TRUE)
BOOL __stdcall
BuildSparseMap(
	_In_ HANDLE File,
	_In_opt_ FILE * const StatsStream,
	_In_opt_ UINT64 StatsFrequencyMillisec,
	_Inout_opt_ SIZE_T *ClusterSize,
	_Out_ PCLUSTER_MAP *ClusterMap
	)
{
	SIZE_T fsClusterSize;
	BOOL retVal;
	UINT64 bytesProcessed, numSparseClusters, startQPC, lastStatQPC;
	LARGE_INTEGER tmpLI;
	UINT64 flSize, hours, minutes, seconds;
	HANDLE flMap;
	SIZE_T currentViewSize, currentViewAlignedDownSize, i, startClusterOfst,
	       sequentialZeros;
	char *currentViewBase;
	PCLUSTER_MAP clusterMap;
	DWORD lastErr;
	double flSizeMiB;

	lastErr = 0;
	fsClusterSize = 0;
	retVal = FALSE;
	bytesProcessed = 0;
	flMap = NULL;
	currentViewBase = NULL;
	clusterMap = NULL;
	numSparseClusters = 0;

	startQPC = GetQPCVal();
	lastStatQPC = startQPC;

	if (NULL == ClusterSize || 0 == *ClusterSize) {
		fsClusterSize = (DWORD)GetVolumeClusterSizeFromFileHandle(File);
		if (!fsClusterSize) {
			lastErr = GetLastError();
			goto error_return;
		}
		if (ClusterSize)
			*ClusterSize = fsClusterSize;
	} else {
		fsClusterSize = *ClusterSize;
	}

	// Ensure the cluster size is a power of two and a reasonable size.
	if ((512 > fsClusterSize) || (fsClusterSize & (fsClusterSize - 1))) {
		lastErr = ERROR_INVALID_PARAMETER;
		goto error_return;
	}

	if (FALSE == GetFileSizeEx(File, &tmpLI))
		goto error_return;

	flSize = (UINT64)tmpLI.QuadPart;
	if (!flSize) {
		lastErr = ERROR_FILE_INVALID;
		goto error_return;
	}
	flSizeMiB = (double)flSize / (double)(1024 * 1024);

	clusterMap = ClusterMapAllocate((DWORD)fsClusterSize, flSize);
	if (!clusterMap) {
		lastErr = GetLastError();
		goto error_return;
	}

	flMap = CreateFileMappingW(File,
	                           NULL,
	                           PAGE_READONLY,
	                           0,
	                           0,
	                           NULL);
	if (!flMap) {
		lastErr = GetLastError();
		//LogError(L"Failed CreateFileMappingW with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}

	while (bytesProcessed < flSize) {
		currentViewSize = (SIZE_T)MIN(MAX_FILE_VIEW_SIZE, flSize - bytesProcessed);
		currentViewAlignedDownSize = ALIGN_DOWN_BY(currentViewSize, sizeof(ULONG_PTR));
		currentViewBase = MapViewOfFile(flMap,
		                                FILE_MAP_READ,
		                                (DWORD)(bytesProcessed >> 32),
		                                (DWORD)bytesProcessed,
		                                currentViewSize);
		if (!currentViewBase) {
			lastErr = GetLastError();
			//LogError(L"Failed MapViewOfFile with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}

		__try {
			i = 0;
			startClusterOfst = 0;
			while (i < currentViewAlignedDownSize) {
				if (*(ULONG_PTR *)(currentViewBase + i)) {
					sequentialZeros = (i - startClusterOfst);
					// Mark any detected sparse ranges.
					while (sequentialZeros >= fsClusterSize) {
						ClusterMapMarkZero(clusterMap, bytesProcessed + startClusterOfst);
						numSparseClusters++;
						sequentialZeros -= fsClusterSize;
						startClusterOfst += fsClusterSize;
					}
					i = ALIGN_DOWN_BY((i + fsClusterSize), fsClusterSize);
					startClusterOfst = i;
				} else {
					i += sizeof(ULONG_PTR);
				}
			}

			/* Take care of any remaining data at the end of a view that is
			 * smaller than a ULONG_PTR */
			while (i < currentViewSize) {
				if (*(currentViewBase + i)) {
					sequentialZeros = (i - startClusterOfst);
					// Mark any detected sparse ranges.
					while (sequentialZeros >= fsClusterSize) {
						ClusterMapMarkZero(clusterMap, bytesProcessed + startClusterOfst);
						numSparseClusters++;
						sequentialZeros -= fsClusterSize;
						startClusterOfst += fsClusterSize;
					}
					i = currentViewSize;
				} else {
					++i;
				}
			}

			/* Handle zero ranges up to the the end of the file view. */
			if (i == currentViewSize && (startClusterOfst < i) ) {
				sequentialZeros = (i - startClusterOfst);
				// Mark any detected sparse ranges including a final partial
				// cluster.
				do {
					ClusterMapMarkZero(clusterMap, bytesProcessed + startClusterOfst);
					numSparseClusters++;
					startClusterOfst += fsClusterSize;
					sequentialZeros -= MIN(fsClusterSize, sequentialZeros);
				} while (sequentialZeros);
			}

		} __except (GetExceptionCode() == EXCEPTION_IN_PAGE_ERROR
		                               ?  EXCEPTION_EXECUTE_HANDLER
		                               :  EXCEPTION_CONTINUE_SEARCH) {
			//LogError(L"Failed to read or write to files at offset: %llu", bytesProcessed);
			lastErr = ERROR_FILE_INVALID;
			goto error_return;
		}

		bytesProcessed += currentViewSize;

		if (StatsStream) {
			if (ElapsedQPCInMillisec(lastStatQPC, GetQPCVal()) >= StatsFrequencyMillisec) {
				fwprintf(StatsStream,
				         L"Analyzed: %8.2f MiB of %8.2f MiB. %8.2f MiB of sparse ranges found.\n",
				         (double)bytesProcessed / 1048576.0,
				         flSizeMiB,
				         (double)(numSparseClusters * fsClusterSize) / 1048576.0);
				lastStatQPC = GetQPCVal();
			}
		}

		if (!UnmapViewOfFile(currentViewBase)) {
			lastErr = GetLastError();
			//LogError(L"Failed UnmapViewOfFile with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}
		currentViewBase = NULL;
	}

	if (!CloseHandle(flMap)) {
		lastErr = GetLastError();
		goto error_return;
	}

	*ClusterMap = clusterMap;

	if (StatsStream) {
		seconds = ElapsedQPCInSeconds(startQPC, GetQPCVal());
		hours = seconds / (60 * 60);
		seconds = seconds % (60 * 60);
		minutes = seconds / 60;
		seconds = seconds % 60;

		fwprintf(StatsStream,
		        L"Analyzed: %8.2f MiB of %8.2f MiB. %8.2f MiB of zero ranges found.\n"
		        L"Elapsed time: %llu hours, %llu minutes, %llu seconds\n",
		        (double)bytesProcessed / 1048576.0,
		        flSizeMiB,
		        (double)(numSparseClusters * fsClusterSize) / 1048576.0,
		        hours, minutes, seconds);
	}

	retVal = TRUE;
	goto func_return;

error_return:
	if (currentViewBase)
		(void)UnmapViewOfFile(currentViewBase);
	if (flMap)
		(void)CloseHandle(flMap);
	if (clusterMap)
		ClusterMapFree(clusterMap);

	retVal = FALSE;
	SetLastError(lastErr);

func_return:
	return retVal;
}


UINT64 __stdcall
GetQPCVal(
	void
	)
{
	LARGE_INTEGER tmp;
	// Per MS docs this will always succeed on XP or later.
	(void)QueryPerformanceCounter(&tmp);
	return (UINT64)tmp.QuadPart;
}


_Use_decl_annotations_
UINT64 __stdcall
ElapsedQPCInHours(
	UINT64 StartVal,
	UINT64 EndVal
	)
{
	return ((EndVal - StartVal) / (60 * 60)) / QPCFrequency;
}


_Use_decl_annotations_
UINT64 __stdcall
ElapsedQPCInMinutes(
	UINT64 StartVal,
	UINT64 EndVal
	)
{
	return ((EndVal - StartVal) / 60) / QPCFrequency;
}


_Use_decl_annotations_
UINT64 __stdcall
ElapsedQPCInSeconds(
	UINT64 StartVal,
	UINT64 EndVal
	)
{
	return (EndVal - StartVal) / QPCFrequency;
}


_Use_decl_annotations_
UINT64 __stdcall
ElapsedQPCInMillisec(
	UINT64 StartVal,
	UINT64 EndVal
	)
{
	return ((EndVal - StartVal) * 1000) / QPCFrequency;
}


_Use_decl_annotations_
UINT64 __stdcall
ElapsedQPCInMicrosec(
	UINT64 StartVal,
	UINT64 EndVal
	)
{
	return ((EndVal - StartVal) * 1000000) / QPCFrequency;
}

_Use_decl_annotations_
UINT64 __stdcall
ElapsedQPCInNanosec(
	UINT64 StartVal,
	UINT64 EndVal
	)
{
	return ((EndVal - StartVal) * 1000000000) / QPCFrequency;
}


/* This will get called before the the user's main func */
void __stdcall
SparseFileLibInit(
	void
	)
{
	LARGE_INTEGER tmp;
	// Per MS docs this will always succeed on XP or later.
	(void)QueryPerformanceFrequency(&tmp);
	QPCFrequency = (UINT64)tmp.QuadPart;
}

