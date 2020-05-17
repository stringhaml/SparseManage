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

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
// Necessary due to WIN32_LEAN_AND_MEAN
#include <winioctl.h>

#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>

#include <SparseFileLib.h>

#define MAX_PENDING_IOS     128
#define DEFAULT_EXE_NAME    L"CopySparse.exe"

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

/* Global variables */
PCLUSTER_MAP    g_ClusterMap;
HANDLE          g_TargetFile;
DWORD           g_NumPendingIOs;
UINT64          g_BytesRead;
UINT64          g_BytesWritten;

typedef struct IO_OP {
	PVOID       AlignedBuf;
	OVERLAPPED  Overlapped;
} IO_OP, *PIO_OP;


_Must_inspect_result_
_Success_(return != NULL)
static PIO_OP __fastcall
AllocIoOp(
	_In_range_(1, MAXDWORD) DWORD   BufSize
	)
{
	PIO_OP ioOp;

	ioOp = calloc(1, sizeof(*ioOp));
	if (!ioOp) {
		LogError(L"Failed calloc with errno %d", errno);
		goto error_return;
	}

	ioOp->AlignedBuf = _aligned_malloc(BufSize, PAGE_SIZE);
	if (!ioOp->AlignedBuf) {
		LogError(L"Failed _aligned_malloc with errno %d", errno);
		goto error_return;
	}

	/* Embed the original buffer size in the hEvent member since it's free for context use when
	Read/WriteFileEx variants are used. */
	ioOp->Overlapped.hEvent = (HANDLE)(ULONG_PTR)BufSize;

	goto func_return;

error_return:
	if (ioOp) {
		if (ioOp->AlignedBuf)
			_aligned_free(ioOp->AlignedBuf);
		free(ioOp);
		ioOp = NULL;
	}
	SetLastError(ERROR_OUTOFMEMORY);

func_return:
	return ioOp;
}


static void __fastcall
FreeIoOp(
	_In_ _Post_invalid_ PIO_OP IoOp
	)
{
	_aligned_free(IoOp->AlignedBuf);
	free(IoOp);
}


static void WINAPI
TargetWriteComplete(
	_In_    DWORD dwErrorCode,
	_In_    DWORD dwNumberOfBytesTransfered,
	_Inout_ LPOVERLAPPED lpOverlapped
	)
{
	PIO_OP  ioOp;
	UINT64  offset;
	DWORD   writeLength;

	ioOp        = CONTAINING_RECORD(lpOverlapped, IO_OP, Overlapped);
	writeLength = (DWORD)(ULONG_PTR)lpOverlapped->hEvent;
	offset      = lpOverlapped->Offset | ((UINT64)lpOverlapped->OffsetHigh << 32);

	if (ERROR_SUCCESS != dwErrorCode) {
		LogError(L"Failed write to target file at offset 0x%llx of length %lu with error code %lu", offset, writeLength, dwErrorCode);
		// TODO: make this better
		ExitProcess(EXIT_FAILURE);
	}

	if (dwNumberOfBytesTransfered != writeLength) {
		LogError(L"Failed to write expected length %lu to target file at offset 0x%llx", writeLength, offset);
		// TODO: make this better
		ExitProcess(EXIT_FAILURE);
	}

	FreeIoOp(ioOp);
	g_NumPendingIOs--;
	g_BytesWritten += dwNumberOfBytesTransfered;
}


static void WINAPI
SourceReadComplete(
	_In_    DWORD dwErrorCode,
	_In_    DWORD dwNumberOfBytesTransfered,
	_Inout_ LPOVERLAPPED lpOverlapped
	)
{
	PIO_OP  ioOp;
	UINT64  offset;
	DWORD   readLength, lastErr;

	ioOp        = CONTAINING_RECORD(lpOverlapped, IO_OP, Overlapped);
	readLength  = (DWORD)(ULONG_PTR)lpOverlapped->hEvent;
	offset      = lpOverlapped->Offset | ((UINT64)lpOverlapped->OffsetHigh << 32);

	if (ERROR_SUCCESS != dwErrorCode) {
		LogError(L"Failed read from source file at offset 0x%llx of length %lu with error code %lu", lpOverlapped->Pointer, readLength, dwErrorCode);
		// TODO: make this better
		ExitProcess(EXIT_FAILURE);
	}

	if (readLength != dwNumberOfBytesTransfered) {
		LogError(L"Failed to read expected number of bytes %lu from source file at offset 0x%llx of length %lu with error code %lu", readLength, lpOverlapped->Pointer, readLength, dwErrorCode);
		// TODO: make this better
		ExitProcess(EXIT_FAILURE);
	}

	/* If the buffer is zero we're done with it but if it's not we queue a write to the target file
	 * using the same overlapped and buffer. */
	if (IsZeroBuf(ioOp->AlignedBuf, dwNumberOfBytesTransfered)) {
		FreeIoOp(ioOp);
		g_NumPendingIOs--;
		if (g_ClusterMap)
			ClusterMapMarkZero(g_ClusterMap, offset);
	} else {
		/* Per docs MS won't modify overlapped offset values but we do need to clear the internal
		values for overlapped re-use. */
		lpOverlapped->Internal      = 0;
		lpOverlapped->InternalHigh  = 0;
		/* Per MS docs we always need to check GetLastError so we ignore the return value. */
		(void)WriteFileEx(g_TargetFile,
		                  ioOp->AlignedBuf,
		                  dwNumberOfBytesTransfered,
		                  lpOverlapped,
		                  TargetWriteComplete);
		lastErr = GetLastError();
		if (ERROR_SUCCESS != lastErr) {
			LogError(L"Failed WriteFileEx with lastErr %lu (0x%08lx)", lastErr, lastErr);
			// TODO: make this better
			ExitProcess(EXIT_FAILURE);
		}
	}

	g_BytesRead += dwNumberOfBytesTransfered;
}


static void __stdcall
PrintUsageInfo(
	LPWSTR exeName
	)
{
	LogInfo(L"Usage: %s [-h] [-m] INPUTFILE OUTPUTFILE\n\t-h Print this help message.\n\t-m Print sparse cluster map.\n", exeName);
}


_Must_inspect_result_
_Success_(return != 0)
static BOOL __stdcall
ParseArgs(
	_In_    int         argc,
	_In_    wchar_t     **argv,
	_Out_   BOOLEAN     *PrintClusterMap,
	_Out_   LPWSTR      *SourceFileName,
	_Out_   LPWSTR      *TargetFileName
	)
{
	BOOL    retVal;
	BOOLEAN pcm;
	int     i;

	retVal = FALSE;
	pcm = FALSE;

	if ((argc < 3) || (argc > 5)) {
		PrintUsageInfo((argc < 1) ? DEFAULT_EXE_NAME : argv[0]);
		goto func_return;
	}

	for (i = 1; i < (argc - 2); ++i) {
		if (!wcscmp(L"-h", argv[i])) {
			PrintUsageInfo(argv[0]);
			goto func_return;
		} else if (!wcscmp(L"-m", argv[i])) {
			/* catch if they specified -m twice. */
			if (pcm)
				goto func_return;
			pcm = TRUE;
		}
	}

	*PrintClusterMap = pcm;
	*SourceFileName = argv[i];
	*TargetFileName = argv[i + 1];
	retVal = TRUE;

func_return:
	return retVal;
}


int
wmain(
	int         argc,
	wchar_t     **argv
	)
{
	HANDLE                  sourceFile;
	HANDLE                  statsTimer;
	PIO_OP                  ioOp;
	BOOLEAN                 printSparseMap;
	LPWSTR                  sourceFileName, targetFileName;
	FILETIME                ftCreate, ftAccess, ftWrite;
	FILE_SET_SPARSE_BUFFER  sparseBuf;
	LARGE_INTEGER           sourceFileSize, statsFreq;
	ULARGE_INTEGER          currentOffset;
	SIZE_T                  sourceClusterSize;
	DWORD                   lastErr;
	OVERLAPPED              setSparseOverlapped;
	double                  sourceFileSizeMiB;
	int                     retVal;

	sourceFile = NULL;
	statsTimer = NULL;
	RtlZeroMemory(&setSparseOverlapped, sizeof(setSparseOverlapped));

	if (!ParseArgs(argc, argv, &printSparseMap, &sourceFileName, &targetFileName)) {
		goto error_return;
	}

	sourceFile = OpenFileExclusive(sourceFileName,
	                               FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN,
	                               &sourceFileSize,
	                               &sourceClusterSize,
	                               &ftCreate,
	                               &ftAccess,
	                               &ftWrite);
	if (!sourceFile) {
		lastErr = GetLastError();
		LogError(L"Failed to open file %s with lastErr %lu (0x%08lx)", sourceFileName, lastErr, lastErr);
		goto error_return;
	}

	if (!sourceClusterSize) {
		LogInfo(L"Unable to determine cluster size of source file volume. Defaulting to %d.\n", 4096);
		sourceClusterSize = 4096;
	}

	if (printSparseMap) {
		g_ClusterMap = ClusterMapAllocate((DWORD)sourceClusterSize, (UINT64)sourceFileSize.QuadPart);
		if (!g_ClusterMap) {
			lastErr = GetLastError();
			LogError(L"Failed ClusterMapAllocate with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}
	}

	g_TargetFile = CreateFileW(targetFileName,
	                           GENERIC_ALL,
	                           0,
	                           NULL,
	                           CREATE_NEW,
	                           FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
	                           sourceFile);
	if (INVALID_HANDLE_VALUE == g_TargetFile) {
		g_TargetFile = NULL;
		lastErr = GetLastError();
		LogError(L"Failed CreateFileW for filename %s with lastErr %lu (0x%08lx)", targetFileName, lastErr, lastErr);
		goto error_return;
	}

	setSparseOverlapped.hEvent = CreateEventW(NULL, TRUE, TRUE, NULL);
	if (!setSparseOverlapped.hEvent) {
		lastErr = GetLastError();
		LogError(L"Failed CreateEventW with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}

	/* Set the sparse attribute on the target file */
	sparseBuf.SetSparse = TRUE;
	if (!DeviceIoControl(g_TargetFile,
	                     FSCTL_SET_SPARSE,
	                     &sparseBuf,
	                     sizeof(sparseBuf),
	                     NULL,
	                     0,
	                     NULL,
	                     &setSparseOverlapped)) {
		lastErr = GetLastError();
		if (lastErr != ERROR_IO_PENDING) {
			LogError(L"Failed DeviceIoControl for FSCTL_SET_SPARSE with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}
	}

	/* GetOverlappedResult requires a param for bytes returned, but for our case, we don't care so
	 * lastErr is used as a tmp variable here. */
	if (!GetOverlappedResult(g_TargetFile, &setSparseOverlapped, &lastErr, TRUE)) {
		lastErr = GetLastError();
		LogError(L"Failed GetOverlappedResult for DeviceIoControl for FSCTL_SET_SPARSE with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}

	/* we're done with our sparsefile overlapped. */
	(void)CloseHandle(setSparseOverlapped.hEvent);
	setSparseOverlapped.hEvent = NULL;

	/* Set the target file size to match the source. */
	lastErr = SetFileSize(g_TargetFile, sourceFileSize);
	if (ERROR_SUCCESS != lastErr) {
		LogError(L"Failed SetFileSize with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}

	/* create and set waitable timer for stats output */
	statsFreq.QuadPart = -100000000ll; /* 10 seconds */
	statsTimer = CreateWaitableTimerW(NULL, TRUE, NULL);
	if (!statsTimer) {
		lastErr = GetLastError();
		LogError(L"Failed CreateWaitableTimerW with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}

	SetWaitableTimer(statsTimer, &statsFreq, 0, NULL, NULL, FALSE);

	sourceFileSizeMiB = (double)sourceFileSize.QuadPart / 1048576.0;
	currentOffset.QuadPart = 0;
	while (currentOffset.QuadPart < (ULONGLONG)sourceFileSize.QuadPart) {
		if (g_NumPendingIOs < MAX_PENDING_IOS) {
			ioOp = AllocIoOp((DWORD)min(sourceClusterSize,
			                             (((ULONGLONG)sourceFileSize.QuadPart
			                                        - currentOffset.QuadPart))));
			if (!ioOp) {
				lastErr = GetLastError();
				LogError(L"Failed AllocIoOp with lastErr %lu (0x%08lx)", lastErr, lastErr);
				goto error_return;
			}

			ioOp->Overlapped.Offset     = currentOffset.LowPart;
			ioOp->Overlapped.OffsetHigh = currentOffset.HighPart;
			++g_NumPendingIOs;
			/* Per MS docs we always need to check GetLastError so we ignore the return value. */
			(void)ReadFileEx(sourceFile,
			                 ioOp->AlignedBuf,
			                 (DWORD)sourceClusterSize,
			                 &ioOp->Overlapped,
			                 SourceReadComplete);
			lastErr = GetLastError();
			if (ERROR_SUCCESS != lastErr) {
				LogError(L"Failed ReadFileEx with lastErr %lu (0x%08lx)", lastErr, lastErr);
				// TODO: make this better
				goto error_return;
			}
			currentOffset.QuadPart += sourceClusterSize;
		} else {
			lastErr = WaitForSingleObjectEx(statsTimer, INFINITE, TRUE);
			if (WAIT_OBJECT_0 == lastErr) {
				LogInfo(L"Read: %8.2f MiB of %8.2f MiB; Written: %8.2f\n", (double)g_BytesRead / 1048576.0, sourceFileSizeMiB, (double)g_BytesWritten / 1048576.0);
				(void)SetWaitableTimer(statsTimer, &statsFreq, 0, NULL, NULL, FALSE);
			} else if (WAIT_IO_COMPLETION != lastErr) {
				lastErr = GetLastError();
				LogError(L"Unexpected WaitForSingleObjectEx return with lastErr %lu (0x%08lx)", lastErr, lastErr);
				goto error_return;
			}
		}
	}

	/* Wait for all pending IOs complete. */
	while (g_NumPendingIOs) {
		lastErr = WaitForSingleObjectEx(statsTimer, INFINITE, TRUE);
		if (WAIT_OBJECT_0 == lastErr) {
			LogInfo(L"Read: %8.2f MiB of %8.2f MiB; Written: %8.2f\n", (double)g_BytesRead / 1048576.0, sourceFileSizeMiB, (double)g_BytesWritten / 1048576.0);
			(void)SetWaitableTimer(statsTimer, &statsFreq, 0, NULL, NULL, FALSE);
		} else if (WAIT_IO_COMPLETION != lastErr) {
			lastErr = GetLastError();
			LogError(L"Unexpected WaitForSingleObjectEx return with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}
	}

	/* Set timestamps on target from source file. */
	if (!SetFileTime(g_TargetFile, &ftCreate, &ftAccess, &ftWrite)) {
		lastErr = GetLastError();
		LogError(L"Failed to write file time values to target file with lastErr %lu (0x%08lx)\n", lastErr, lastErr);
	}

	/* Start clean up */
	(void)CloseHandle(sourceFile);
	sourceFile = NULL;

	/* Flush buffers on file */
	if (!FlushFileBuffers(g_TargetFile)) {
		lastErr = GetLastError();
		LogError(L"WARNING: Failed FlushFileBuffers on target file with lastErr %lu.\n", lastErr);
	}

	(void)CloseHandle(g_TargetFile);
	g_TargetFile = NULL;

	LogInfo(L"Sparse file copy complete.\n%16llu bytes read\n%16.2f MiB read\n%16.2f GiB read\n%16llu bytes written\n%16.2f MiB written\n%16.2f GiB written\n",
	        g_BytesRead,    (double)g_BytesRead    / 1048576.0, (double)g_BytesRead    / 1073741824.0,
	        g_BytesWritten, (double)g_BytesWritten / 1048576.0, (double)g_BytesWritten / 1073741824.0);

	if (g_ClusterMap)
		ClusterMapPrint(g_ClusterMap, stdout);

	retVal = EXIT_SUCCESS;

	goto func_return;

error_return:
	retVal = EXIT_FAILURE;

func_return:
	if (sourceFile)
		(void)CloseHandle(sourceFile);
	if (g_TargetFile)
		(void)CloseHandle(g_TargetFile);
	if (statsTimer)
		(void)CloseHandle(statsTimer);
	if (g_ClusterMap)
		ClusterMapFree(g_ClusterMap);
	return retVal;
}
