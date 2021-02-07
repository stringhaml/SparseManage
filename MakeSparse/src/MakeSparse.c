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

/* Disable warning for "nonstandard extension used: nameless struct/union"
 * since it actually is standard in C11. Now if only Visual Studio 2015 could
 * get to C99... Here's to hoping for better C standard support in the next
 * version... */
#pragma warning(disable:4201)

/* TODO: For windows 8 / server 2012 use GetFileInformationByHandleEx function
 * to query OS about the file sector size and alignment rather then the current
 * method of parsing the file handle path to open a handle to the drive and
 * using GetDiskFreeSpace to calculate the cluster size and assuming zero based
 * alignment. */

/* TODO: Chunk file analysis and deallocation for files roughly 60 TiB or more
 * (assuming 4k clusters) since we may start running into memory issues with
 * only 2 GiB of usable memory in 32-bit environments. */

/* TODO: Query existing sparse ranges and don't re-analyze them. */

/* TODO: Make this dynamic */
#define MAX_PENDING_IO              32

/* This value will be used if the cluster size of the filesystem cannot be
 * determined automatically. */
#define DEFAULT_FS_CLUSTER_SIZE     4096

/* Relative 10 seconds in 100nsec intervals */
#define STATS_TIMER_INTERVAL        (-100000000ll)

typedef struct IO_CB_SHARED {
	HANDLE          FileHandle;
	LARGE_INTEGER   FileSize;
	const SIZE_T    FsClusterSize;
	HANDLE          IoAvailSemaphore;
	HANDLE          StatsTimerHandle;
	LARGE_INTEGER   StatsTimerIntervalIn100Nsec;
	volatile LONG64 StatsIoReadBytes;
	volatile LONG64 StatsIoZeroedBytes;
	volatile LONG64 StatsBytesToZero;
	PCLUSTER_MAP    ZeroClusterMap;
} IO_CB_SHARED, *PIO_CB_SHARED;

enum IO_OP_TYPE {
	IO_READ = 1,
	IO_SET_SPARSE,
	IO_SET_ZERO_RANGE
};

typedef struct IO_OP {
	enum IO_OP_TYPE     OpType;
	union {
		// Aligned to PAGE_SIZE with _aligned_malloc
		char                        *ReadBuf;
		FILE_SET_SPARSE_BUFFER      SetSparseBuf;
		FILE_ZERO_DATA_INFORMATION  ZeroDataInfo;
	};
	OVERLAPPED          Ovlp;
} IO_OP, *PIO_OP;


static VOID
LogProgress(
	_In_    PIO_CB_SHARED   IoCbShared
	)
{
	double statsIoReadBytes;
	double fileSize;
	LONG64 statsIoZeroedBytes;
	LONG64 statsBytesToZero;

	fileSize = (double)IoCbShared->FileSize.QuadPart;
	statsIoReadBytes = (double)IoCbShared->StatsIoReadBytes;
	statsIoZeroedBytes = IoCbShared->StatsIoZeroedBytes;
	statsBytesToZero = IoCbShared->StatsBytesToZero;

	LogInfo(L"%.2f%% reads complete. (%.2f MiB of %.2f MiB)\n",
	        (statsIoReadBytes / fileSize) * 100.0,
	        statsIoReadBytes / 1048576.0,
	        fileSize / 1048576.0);
	LogInfo(L"%.2f MiB of %.2f MiB zeroed. (%lld B of %lld B)\n",
	        (double)statsIoZeroedBytes / 1048576.0,
	        (double)statsBytesToZero / 1048576.0,
	        (long long)(statsIoZeroedBytes),
	        (long long)(statsBytesToZero));
}


static VOID
LogProgressOnInterval(
	_In_    PIO_CB_SHARED   IoCbShared
	)
{
	DWORD waitRtn;

	if (   WAIT_OBJECT_0
		== (waitRtn = WaitForSingleObject(IoCbShared->StatsTimerHandle, 0)))
	{
		LogProgress(IoCbShared);
		SetWaitableTimer(IoCbShared->StatsTimerHandle,
		                 &IoCbShared->StatsTimerIntervalIn100Nsec,
		                 0,
		                 NULL,
		                 NULL,
		                 FALSE);
	} else if (waitRtn != WAIT_TIMEOUT) {
		LogError(L"Unexpected error 0x%08lX in wait call for StatsTimerHandle\n",
		         waitRtn);
		// TODO: Make this nicer.
		ExitProcess(EXIT_FAILURE);
	}
}


/* Note that if allocating an IO_READ the memory pointed to by ReadBuf is not
 * zeroed. */
_Success_(return != NULL)
static PIO_OP
AllocIoOp(
	_In_    enum IO_OP_TYPE     OpType,
	_In_    SIZE_T              ReadBufSize,
	_In_    SIZE_T              BufAlignment
	)
{
	PIO_OP o;

	o = calloc(1, sizeof(*o));
	if (NULL == o)
		return NULL;
	o->OpType = OpType;
	if (IO_READ == OpType) {
		o->ReadBuf = _aligned_malloc(ReadBufSize, BufAlignment);
		if (NULL == o->ReadBuf) {
			free(o);
			return NULL;
		}
	}
	return o;
}


static VOID
FreeIoOp(
	_In_    PIO_OP      Op
	)
{
	if (IO_READ == Op->OpType)
		_aligned_free(Op->ReadBuf);
	free(Op);
}


static void
ProcessIoReadCallback(
	_Inout_     PIO_CB_SHARED       IoCbShared,
	_In_range_(1, MAXULONG_PTR)
	            ULONG_PTR           NumBytesRead,
	_In_        PIO_OP              IoOp
	)
{
	SIZE_T          fsClusterSize, i;
	LPOVERLAPPED    ovrlp;
	ULARGE_INTEGER  startOffset;

	ovrlp = &IoOp->Ovlp;

	fsClusterSize = IoCbShared->FsClusterSize;
	InterlockedAdd64(&IoCbShared->StatsIoReadBytes, (LONG64)NumBytesRead);
	/* The loop isn't exactly obvious about the i += fsClusterSize. Basically
	 * we go through the read data in cluster size chunks. The only time we get
	 * a read size that isn't perfectly divisable by the cluster size is when
	 * we get to the end of the file. That case is covered by the else clause.
	 */
	for (i = 0; i < NumBytesRead; i += fsClusterSize) {
		if ((i + fsClusterSize) <= NumBytesRead) {
			if (IsZeroBuf(IoOp->ReadBuf + i, (DWORD)fsClusterSize)) {
				startOffset.LowPart  =  ovrlp->Offset;
				startOffset.HighPart =  ovrlp->OffsetHigh;
				startOffset.QuadPart += i;
				ClusterMapMarkZero(IoCbShared->ZeroClusterMap,
				                   startOffset.QuadPart);
				InterlockedAdd64(&IoCbShared->StatsBytesToZero,
				                 (LONG64)fsClusterSize);
			}
		} else {
			startOffset.LowPart  = ovrlp->Offset;
			startOffset.HighPart = ovrlp->OffsetHigh;
			// check if EOF runt
			if (   (LONGLONG)(IoCbShared->FileSize.QuadPart - startOffset.QuadPart)
			    == (LONGLONG)NumBytesRead)
			{
				if (IsZeroBuf(IoOp->ReadBuf + i, (DWORD)(NumBytesRead - i))) {
					ClusterMapMarkZero(IoCbShared->ZeroClusterMap,
					                   startOffset.QuadPart + i);
					InterlockedAdd64(&IoCbShared->StatsBytesToZero,
					                 (LONG64)(NumBytesRead - i));
				}
			} else {
				// TODO: determine if there is anything else to do here...
				LogError(L"Unexpected non-cluster size NumBytesRead: %zu",
				         NumBytesRead);
				assert(FALSE);
				ExitProcess(EXIT_FAILURE);
			}
		}
	}
}


static VOID CALLBACK
ProcessCompletedIoCallback(
	_Inout_     PTP_CALLBACK_INSTANCE Instance,
	_Inout_opt_ PVOID                 Context,
	_Inout_opt_ PVOID                 Overlapped,
	_In_        ULONG                 IoResult,
	_In_        ULONG_PTR             NumBytesTxd,
	_Inout_     PTP_IO                pTpIo
	)
{
	LPOVERLAPPED    pOvrlp;
	PIO_OP          ioOp;
	PIO_CB_SHARED   cbCtxShared;

	UNREFERENCED_PARAMETER(pTpIo);

	if (NULL == Context) {
		LogError(L"No context provided for io completion callback\n");
		ExitProcess(EXIT_FAILURE);
	}

	cbCtxShared = Context;

	/* Wakeup the dispatch thread, if necessary, when the callback returns. */
	ReleaseSemaphoreWhenCallbackReturns(Instance,
	                                    cbCtxShared->IoAvailSemaphore,
	                                    1);

	if (NULL == Overlapped) {
		LogError(L"Received unexpected NULL overlapped in io completion callback.\n");
		// TODO: Make this nicer.
		ExitProcess(EXIT_FAILURE);
	}
	pOvrlp = Overlapped;

	ioOp = CONTAINING_RECORD(pOvrlp, IO_OP, Ovlp);

	switch (ioOp->OpType) {
	case IO_READ:
		if (ERROR_SUCCESS == IoResult && NumBytesTxd) {
			ProcessIoReadCallback(cbCtxShared, NumBytesTxd, ioOp);
		} else {
			LogError(L"Error 0x%08lX reading from file at offset 0x%08llX%08llX\n",
			         IoResult,
			         (long long)pOvrlp->OffsetHigh,
			         (long long)pOvrlp->Offset);
			// TODO: Make this nicer.
			ExitProcess(EXIT_FAILURE);
		}
		break;

	case IO_SET_ZERO_RANGE:
		if (ERROR_SUCCESS == IoResult) {
			InterlockedAdd64(&cbCtxShared->StatsIoZeroedBytes,
			                (LONG64)(ioOp->ZeroDataInfo.BeyondFinalZero.QuadPart - ioOp->ZeroDataInfo.FileOffset.QuadPart));
		}
		/* FALL THROUGH */
	case IO_SET_SPARSE:
		if (ERROR_SUCCESS != IoResult) {
			LogError(L"Error 0x%08lX in %s ioctl call at offset 0x%08llX%08llX\n",
			         IoResult,
			         (IO_SET_ZERO_RANGE == ioOp->OpType)
			             ? L"set zero range"
			             : L"set sparse attribute",
			         (long long)pOvrlp->OffsetHigh, (long long)pOvrlp->Offset);
			// TODO: Make this nicer.
			ExitProcess(EXIT_FAILURE);
		}

		break;
	default:
		LogError(L"Received unexpected io op type %d in io completion callback\n",
		         ioOp->OpType);
		ExitProcess(EXIT_FAILURE);
	}
	FreeIoOp(ioOp);
}


/* Helper function to wait until the in-progress IOs have dropped below the the
 * threshold before dispatching more. */
static VOID
WaitAvailableIo(
	_Inout_     PIO_CB_SHARED   IoCbShared
	)
{
	DWORD   ret;

	do {
		ret = WaitForSingleObjectEx(IoCbShared->IoAvailSemaphore, INFINITE, TRUE);
	} while (WAIT_IO_COMPLETION == ret);
	if (WAIT_OBJECT_0 != ret) {
		LogError(L"Error %#llx in WaitForSingleObject while waiting for available IO op.\n",
		         (long long)GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
}


static DWORD
DispatchFileReads(
	_Inout_     PIO_CB_SHARED       IoCbShared,
	_In_        PTP_IO              IoTp
	)
{
	SYSTEM_INFO     sysInfo;
	SIZE_T          pageSize;
	LARGE_INTEGER   flOffset;
	UINT64          bytesToRead;
	PIO_OP          ioOp;
	BOOL            ret;
	DWORD           lastErr;

	GetSystemInfo(&sysInfo);
	pageSize = sysInfo.dwPageSize;

	flOffset.QuadPart = 0;
	while (flOffset.QuadPart < IoCbShared->FileSize.QuadPart) {
		WaitAvailableIo(IoCbShared);
		ioOp = AllocIoOp(IO_READ, IoCbShared->FsClusterSize, pageSize);
		if (NULL == ioOp) {
			LogError(L"Failed to allocate io op while dispatching file reads.\n");
			ExitProcess(EXIT_FAILURE);
		}

		ioOp->Ovlp.Offset     = flOffset.LowPart;
		ioOp->Ovlp.OffsetHigh = flOffset.HighPart;
		bytesToRead = IoCbShared->FileSize.QuadPart - flOffset.QuadPart;
		if (bytesToRead > IoCbShared->FsClusterSize)
			bytesToRead = IoCbShared->FsClusterSize;
		StartThreadpoolIo(IoTp);
		ret = ReadFile(IoCbShared->FileHandle, ioOp->ReadBuf, (DWORD)bytesToRead, NULL, &ioOp->Ovlp);
		if ((FALSE == ret) && (ERROR_IO_PENDING != (lastErr = GetLastError()))) {
			LogError(L"Error %#llx from ReadFile call.\n", (long long)lastErr);
			ExitProcess(EXIT_FAILURE);

		}
		flOffset.QuadPart += bytesToRead;

		LogProgressOnInterval(IoCbShared);
	}

	return ERROR_SUCCESS;
}


static DWORD
SetSparseRange(
	_Inout_     PIO_CB_SHARED       IoCbShared,
	_In_        PTP_IO              IoTp,
	_In_        LONGLONG            FileOffset,
	_In_        LONGLONG            BeyondFinalZero
	)
{
	DWORD   errRet;
	PIO_OP  ioOp;

	ioOp = AllocIoOp(IO_SET_ZERO_RANGE, 0, 0);
	if (NULL == ioOp)
		return ERROR_NOT_ENOUGH_MEMORY;
	ioOp->ZeroDataInfo.FileOffset.QuadPart      = FileOffset;
	ioOp->ZeroDataInfo.BeyondFinalZero.QuadPart = BeyondFinalZero;
	WaitAvailableIo(IoCbShared);
	StartThreadpoolIo(IoTp);
	if (!DeviceIoControl(IoCbShared->FileHandle,
	                     FSCTL_SET_ZERO_DATA,
	                     &ioOp->ZeroDataInfo,
	                     sizeof(ioOp->ZeroDataInfo),
	                     NULL,
	                     0,
	                     NULL,
	                     &ioOp->Ovlp))
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
	_Inout_     PIO_CB_SHARED       IoCbShared,
	_In_        PTP_IO              IoTp,
	_In_        DWORD               MinClusterGroup
	)
{
	SIZE_T  fsClusterSize;
	UINT64  i, numClusters, runtBytes;
	INT64   firstClusterInSequence;
	DWORD   errRet;

	fsClusterSize = IoCbShared->FsClusterSize;
	// numClusters may be off by one if filesize not a cluster size multiple.
	// This is handled after loop.
	numClusters = IoCbShared->FileSize.QuadPart / IoCbShared->FsClusterSize;
	firstClusterInSequence = -1;

	for (i = 0; i < numClusters; ++i) {
		if (ClusterMapIsMarkedZero(IoCbShared->ZeroClusterMap, i)) {
			if (firstClusterInSequence < 0)
				firstClusterInSequence = (INT64)i;
		} else {
			if ((firstClusterInSequence >= 0)
				&& (MinClusterGroup <= (i - firstClusterInSequence))) {
				errRet = SetSparseRange(IoCbShared, IoTp, firstClusterInSequence * fsClusterSize,
					i * fsClusterSize);
				if (   (errRet != ERROR_IO_PENDING)
				    && (errRet != ERROR_SUCCESS))
				{
					LogError(L"Error %#llx returned from SetSparseRange call.\n",
					         (long long)errRet);
					ExitProcess(EXIT_FAILURE);
				}
			}
			firstClusterInSequence = -1;
		}
		LogProgressOnInterval(IoCbShared);
	}

	runtBytes = IoCbShared->FileSize.QuadPart % IoCbShared->FsClusterSize;
	if (runtBytes) {
		if (!ClusterMapIsMarkedZero(IoCbShared->ZeroClusterMap, i)) {
			runtBytes = 0;
		}
		// Don't bother zeroing a runt by itself.
	}
	if ((firstClusterInSequence >= 0)
		&& (MinClusterGroup <= (i - firstClusterInSequence))) {
		errRet = SetSparseRange(IoCbShared, IoTp, firstClusterInSequence * fsClusterSize,
			(i * fsClusterSize) + runtBytes);
		if (   (errRet != ERROR_IO_PENDING)
			&& (errRet != ERROR_SUCCESS))
		{
			LogError(L"Error %#llx returned from SetSparseRange call.\n",
			         (long long)errRet);
			ExitProcess(EXIT_FAILURE);
		}
	}
	return ERROR_SUCCESS;
}


static DWORD
SetSparseAttribute(
	_In_        PIO_CB_SHARED       IoCbShared,
	_In_        PTP_IO              IoTp
	)
{
	DWORD   errRet, tmp;
	PIO_OP  ioOp;

	WaitAvailableIo(IoCbShared);

	ioOp = AllocIoOp(IO_SET_SPARSE, 0, 0);
	if (NULL == ioOp)
		return ERROR_NOT_ENOUGH_MEMORY;

	/* Set the sparse attribute for the file */
	ioOp->SetSparseBuf.SetSparse = TRUE;
	StartThreadpoolIo(IoTp);
	if (!DeviceIoControl(IoCbShared->FileHandle,
		                 FSCTL_SET_SPARSE,
		                 &ioOp->SetSparseBuf,
		                 sizeof(ioOp->SetSparseBuf),
		                 NULL,
		                 0,
		                 &tmp,
		                 &ioOp->Ovlp))
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
	LogInfo(L"%s [-p] [-m] Path\\To\\FileToMakeSparse.ext\nSpecify -p to preserve file timestamps.\nSpecify -m to print map of sparse clusters.\n",
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
	SIZE_T                      fsClusterSize;
	HANDLE                      fl;
	BOOL                        prsvTm, printSparseMap;
	LPWSTR                      invocationName, flName;
	FILETIME                    tmCrt;
	FILETIME                    tmAcc;
	FILETIME                    tmWrt;
	LARGE_INTEGER               flSz;
	PTP_IO                      pTpIo;
	IO_CB_SHARED                ioCbShared;
	DWORD                       errRet;
	PCLUSTER_MAP                zeroClusterMap;

	SparseFileLibInit();

	memset(&ioCbShared, 0, sizeof(ioCbShared));

	if (ParseCommandLine(argc, argv, &invocationName, &prsvTm, &printSparseMap, &flName)) {
		PrintUsageInfo(invocationName);
		return EXIT_FAILURE;
	}

	LogInfo(L"Opening file %s\n", flName);

	fl = OpenFileExclusive(flName,
	                       FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN,
	                       &flSz,
	                       &fsClusterSize,
	                       &tmCrt,
	                       &tmAcc,
	                       &tmWrt);
	if (NULL == fl) {
		LogError(L"Failed to open file %s with error %#llx\n",
		         flName, (long long)GetLastError());
		return (EXIT_FAILURE);
	}

	if (fsClusterSize == 0) {
		fsClusterSize = DEFAULT_FS_CLUSTER_SIZE;
		LogInfo(L"Unable to determine cluster size of file system. Using default cluster size: %ld\n", (LONG)fsClusterSize);
	} else {
		LogInfo(L"Cluster size: %ld\n", (LONG)fsClusterSize);
	}

	zeroClusterMap = ClusterMapAllocate((DWORD)fsClusterSize, flSz.QuadPart);
	if (NULL == zeroClusterMap) {
		// FIXME
		CloseHandle(fl);
		LogError(L"Failed to allocate memory for cluster map\n");
		ExitProcess(EXIT_FAILURE);
	}

	/* create and set waitable timer for stats output */
	ioCbShared.StatsTimerIntervalIn100Nsec.QuadPart = STATS_TIMER_INTERVAL;
	ioCbShared.StatsTimerHandle = CreateWaitableTimerW(NULL, TRUE, NULL);
	if (NULL == ioCbShared.StatsTimerHandle) {
		// FIXME
		CloseHandle(fl);
		LogError(L"Failed to allocate waitable timer for stats output with error %#llx\n",
		         (long long)GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	// FIXME: error check
	SetWaitableTimer(ioCbShared.StatsTimerHandle, &ioCbShared.StatsTimerIntervalIn100Nsec, 0, NULL, NULL, FALSE);

	// Have to cast away the const qualifier for initial assignment.
	*(SIZE_T *)&ioCbShared.FsClusterSize = fsClusterSize;
	ioCbShared.FileHandle      = fl;
	ioCbShared.FileSize        = flSz;
	ioCbShared.ZeroClusterMap  = zeroClusterMap;
	ioCbShared.IoAvailSemaphore = CreateSemaphoreW(NULL, MAX_PENDING_IO, MAX_PENDING_IO, NULL);
	if (NULL == ioCbShared.IoAvailSemaphore) {
		//FIXME
		LogError(L"Error %#llx from CreateSemaphoreW call.\n",
		         (long long)GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	pTpIo = CreateThreadpoolIo(fl, ProcessCompletedIoCallback, &ioCbShared, NULL);
	if (NULL == pTpIo) {
		// FIXME
		LogError(L"Error %#llx from CreateThreadpoolIo call.\n",
		         (long long)GetLastError());
		ExitProcess(EXIT_FAILURE);
	}

	LogInfo(L"Starting file analysis.\n");
	errRet = DispatchFileReads(&ioCbShared, pTpIo);
	if (ERROR_SUCCESS != errRet) {
		// FIXME
		LogError(L"Error %#llx from DispatchFileReads call.\n",
		         (long long)errRet);
		ExitProcess(EXIT_FAILURE);
	}
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);

	// For Debug
	//ClusterMapPrint(zeroClusterMap, stdout);

	LogInfo(L"Completed file analysis. Starting to dispatch zero ranges to file system.\n");
	LogProgress(&ioCbShared);

	// TODO: Don't blindly set this if no zero clusters detected.
	errRet = SetSparseAttribute(&ioCbShared, pTpIo);
	if (!((ERROR_SUCCESS == errRet) || (ERROR_IO_PENDING == errRet))) {
		// FIXME
		LogError(L"Error %#llx from SetSparseAttribute call.\n",
		         (long long)errRet);
		ExitProcess(EXIT_FAILURE);
	}
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);

	errRet = SetSparseRanges(&ioCbShared, pTpIo, 1);
	if (ERROR_SUCCESS != errRet) {
		LogError(L"Error %#llx from SetSparseRanges call.\n",
		          (long long)errRet);
		ExitProcess(EXIT_FAILURE);
	}
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);

	LogInfo(L"Zero ranges dispatches complete.\n");

	/* Reset modified and access timestamps if preserve filetimes specified */
	if (prsvTm) {
		if (0 == SetFileTime(fl, NULL, &tmAcc, &tmWrt)) {
			LogError(L"WARNING: Failed to preserve file times on file.\n");
		}
	}

	/* Clean up */
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);
	CloseThreadpoolIo(pTpIo);

	/* Flush buffers on file */
	if (!FlushFileBuffers(fl)) {
		LogError(L"WARNING: Failed FlushFileBuffers on target file with lastErr %lu.\n", GetLastError());
	}

	LogProgress(&ioCbShared);
	(void)CloseHandle(fl);
	(void)CloseHandle(ioCbShared.StatsTimerHandle);
	(void)CloseHandle(ioCbShared.IoAvailSemaphore);

	LogInfo(L"Completed processing file\n");

	if (printSparseMap) {
		LogInfo(L"Printing sparse cluster map\n");
		ClusterMapPrint(zeroClusterMap, stdout);
	}

	ClusterMapFree(zeroClusterMap);

	return EXIT_SUCCESS;
}

