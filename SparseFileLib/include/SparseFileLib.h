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

#pragma once

#ifndef SPARSEFILELIB_H
#define SPARSEFILELIB_H

#include <windows.h>

// This function **must** be called before using any of these functions.
void __stdcall
SparseFileLibInit(
	void
	);

void __cdecl
LogError(
	_In_        LPCWSTR         FormatString,
	...
	);

void __cdecl
LogInfo(
	_In_        LPCWSTR         FormatString,
	...
	);

_Success_(return == TRUE)
BOOL __stdcall
IsZeroBuf(
	_In_        LPVOID          Buf,
	_In_        DWORD           BufSz
	);


typedef struct CLUSTER_MAP *PCLUSTER_MAP;

// On 32-bit enviornments this will fail with cluster maps using > 2GB memory.
// check errno for failure
_Success_(return != NULL)
PCLUSTER_MAP __stdcall
ClusterMapAllocate(
	_In_        DWORD           ClusterSize,
	_In_        UINT64          FileSize
	);

void __stdcall
ClusterMapFree(
	_In_ _Post_invalid_
	            PCLUSTER_MAP    ClusterMap
	);

void __stdcall
ClusterMapMarkZero(
	_Inout_     PCLUSTER_MAP    ClusterMap,
	_In_        UINT64          StartingByteOffset
	);

BOOL __stdcall
ClusterMapIsMarkedZero(
	_Inout_     PCLUSTER_MAP    ClusterMap,
	_In_        UINT64          Cluster
	);

void __stdcall
ClusterMapPrint(
	_In_        PCLUSTER_MAP    ClusterMap,
	_In_        FILE            *FileStream
	);

/* If NULL is returned caller may use GetLastError to find out what happened.
 * If FS cluster size is not able to be determined then the parameter is set
 * to zero and GetLastError will return the underlying error. */
_Success_(return != NULL)
HANDLE __stdcall
OpenFileExclusive(
	_In_        LPCWSTR         Filename,
	_In_        DWORD           FileFlagsAttributes,
	_Out_       PLARGE_INTEGER  FileSize,
	_Out_opt_   PSIZE_T         FsClusterSize,
	_Out_opt_   LPFILETIME      CreationTime,
	_Out_opt_   LPFILETIME      LastAccessTime,
	_Out_opt_   LPFILETIME      LastWriteTime
	);

/* Determine the cluster size of the file system that the handle resides on.
 * Returns -1 on failure. */
_Must_inspect_result_
_Success_(return != 0)
SIZE_T __stdcall
GetVolumeClusterSizeFromFileHandle(
	_In_        HANDLE          FileHandle
	);

/* Set file size for the file handle and move the file pointer to the new end
 * of the file. If the function fails the return value will be the result of a
 * GetLastError() call.
 *
 * WARNING: This function is not thread safe if any other thread may manipulate
 * the file pointer on the handle. This is a especially the case if not using
 * overlapped IO. If using multiple threads you must syncronize manipulation of
 * the file pointer and serialize any calls to this function.
 */
_Must_inspect_result_
_Success_(return == ERROR_SUCCESS)
DWORD __stdcall
SetFileSize(
	_In_        HANDLE          File,
	_In_        LARGE_INTEGER   NewFileSize
	);

UINT64 __stdcall
GetQPCVal(
	void
	);

UINT64 __stdcall
ElapsedQPCInHours(
	_In_ UINT64 StartVal,
	_In_ UINT64 EndVal
	);

UINT64 __stdcall
ElapsedQPCInMinutes(
	_In_ UINT64 StartVal,
	_In_ UINT64 EndVal
	);

UINT64 __stdcall
ElapsedQPCInSeconds(
	_In_ UINT64 StartVal,
	_In_ UINT64 EndVal
	);

UINT64 __stdcall
ElapsedQPCInMillisec(
	_In_ UINT64 StartVal,
	_In_ UINT64 EndVal
	);

UINT64 __stdcall
ElapsedQPCInMicrosec(
	_In_ UINT64 StartVal,
	_In_ UINT64 EndVal
	);

UINT64 __stdcall
ElapsedQPCInNanosec(
	_In_ UINT64 StartVal,
	_In_ UINT64 EndVal
	);

/* Utility macros. For some reason these macros aren't defined in windows.h
 * and instead are only available in the WDK headers so we need to define
 * them here if they are not already defined. */
#ifndef SPARSEFILELIB_DONT_DEFINE_UTIL_MACROS
#ifndef ALIGN_DOWN_POINTER_BY
#define ALIGN_DOWN_POINTER_BY(address, alignment) \
    ((PVOID)((ULONG_PTR)(address) & ~((ULONG_PTR)(alignment) - 1)))
#endif
#ifndef ALIGN_UP_POINTER_BY
#define ALIGN_UP_POINTER_BY(address, alignment) \
    (ALIGN_DOWN_POINTER_BY(((ULONG_PTR)(address) + (alignment) - 1), alignment))
#endif
#ifndef ALIGN_DOWN_POINTER
#define ALIGN_DOWN_POINTER(address, type) \
    ALIGN_DOWN_POINTER_BY(address, sizeof(type))
#endif
#ifndef ALIGN_UP_POINTER
#define ALIGN_UP_POINTER(address, type) \
    ALIGN_UP_POINTER_BY(address, sizeof(type))
#endif
#ifndef ALIGN_DOWN_BY
#define ALIGN_DOWN_BY(size, align)   ((ULONG_PTR)(size) & ~((ULONG_PTR)(align) - 1))
#endif
#ifndef MIN
#define MIN(x, y) ((x) < (y) ? (x) : (y))
#endif
#ifndef MAX
#define MAX(x, y) ((x) > (y) ? (x) : (y))
#endif
#endif

#endif // SPARSEFILELIB_H

