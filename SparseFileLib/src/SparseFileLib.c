/* Standard BSD license disclaimer.

Copyright(c) 2016-2020, Lance D. Stringham
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
	// Cluster / 32 finds the index in the map and then we read the value there.
	// 1 << (Cluster & 31) creates the bitmask to use to check if the bit is set in the value read.
	// & checks if just that bit is set
	// the !! forces exactly 0 or 1 to be returned rather than using a conditional to do it.
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
		if (*ofst == L'\\') delimCnt++;
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

