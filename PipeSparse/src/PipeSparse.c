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
#include <windows.h>

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include <SparseFileLib.h>


#define LITERAL_TO_WSTR(x)      L#x
#define PP_EXPANSION_TO_WSTR(x) LITERAL_TO_WSTR(x)
#define STR_TO_WSTR2(s)         L##s
#define STR_TO_WSTR(s)          STR_TO_WSTR2(s)
#define LINE_WSTR               PP_EXPANSION_TO_WSTR(__LINE__)
#define FUNC_LINE_WSTR          STR_TO_WSTR(__FUNCTION__) L":" LINE_WSTR L": "
#define LogErrorFuncLine(_fmtStr, ...)   LogError(FUNC_LINE_WSTR _fmtStr L"\n", __VA_ARGS__)
#define LogInfoFuncLine(_fmtStr, ...)    LogInfo(FUNC_LINE_WSTR _fmtStr L"\n", __VA_ARGS__)

#define MAX_PENDING_WRITES      128

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

struct WriteOp {
	char        *Buf;
	size_t      BufSz;
	OVERLAPPED  Ovrlp;
};

struct CleanupThreadParams {
	HANDLE     IoAvailSemaphore;
	HANDLE     IOCompletionPort;
};


_Success_(return != NULL)
__declspec(restrict)
static struct WriteOp *
AllocWriteOp(
	_In_        size_t      BufSz
	)
{
	struct WriteOp *op;

	if (NULL == (op = calloc(1, sizeof(*op)))) {
		// suppress unsafe usage warning for _wcserror since we're going to kill the process.
#pragma warning(suppress: 4996)
		LogErrorFuncLine(L"Memory allocation failure of size %zu with errno %d: %s", sizeof(*op), errno, _wcserror(errno));
		goto error_return;
	}

	if (NULL == (op->Buf = _aligned_malloc(BufSz, PAGE_SIZE))) {
		// suppress unsafe usage warning for _wcserror since we're going to kill the process.
#pragma warning(suppress: 4996)
		LogErrorFuncLine(L"Memory allocation failure of size %zu with errno %d: %s", BufSz, errno, _wcserror(errno));
		goto error_return;
	}
	op->BufSz = BufSz;

	if (NULL == (op->Ovrlp.hEvent = CreateEventW(NULL, TRUE, TRUE, NULL))) {
		LogErrorFuncLine(L"Failed CreateEventW with GetLastError(): %lu", GetLastError());
		goto error_return;
	}

	goto func_return;

error_return:
	if (op) {
		if (op->Buf)
			_aligned_free(op->Buf);
		if (op->Ovrlp.hEvent)
			(void)CloseHandle(op->Ovrlp.hEvent);
		free(op);
	}
	op = NULL;

func_return:

	return op;
}

static void
FreeWriteOp(
	_In_ _Post_invalid_
	        struct WriteOp  *Op
	)
{
	assert(NULL != Op);

	if (NULL != Op->Buf) {
		_aligned_free(Op->Buf);
	}
	if (NULL != Op->Ovrlp.hEvent) {
		(void)CloseHandle(Op->Ovrlp.hEvent);
	}

	free(Op);
}


static SSIZE_T
FillBuf(
	_In_        HANDLE      InputHndl,
	_When_(return != -1, _Out_writes_bytes_(return))
	            LPVOID      Buf,
	_In_        DWORD       BufSz
	)
{
	DWORD       rdByts;
	DWORD       err;
	DWORD       totalRd;
	BOOL        doLoop;

	totalRd = 0;
	doLoop = TRUE;
	do {
		if (0 == ReadFile(InputHndl, (char *)Buf + totalRd, BufSz - totalRd, &rdByts, NULL)) {
			if (ERROR_BROKEN_PIPE == (err = GetLastError())) {
				return totalRd;
			} else if (ERROR_HANDLE_EOF == err) {
				return totalRd;
			}
			return (-1);
		}
		if (BufSz == (totalRd += rdByts)) {
			doLoop = FALSE;
		}
	} while (doLoop);

	return totalRd;
}


static DWORD WINAPI
CleanupThread(
	LPVOID Params
	)
{
	BOOL                        shutdown;
	HANDLE                      iocpHndl;
	HANDLE                      ioAvailSemaphore;
	DWORD                       bytsWrtn;
	ULONG_PTR                   completionKey;
	LPOVERLAPPED                ovlpdPtr;
	struct WriteOp              *curWriteOp;
	struct CleanupThreadParams  *inParams;

	inParams = Params;
	iocpHndl = inParams->IOCompletionPort;
	ioAvailSemaphore = inParams->IoAvailSemaphore;

	shutdown = FALSE;
	while (!shutdown) {
		if (!GetQueuedCompletionStatus(iocpHndl,
		                               &bytsWrtn,
		                               &completionKey,
		                               &ovlpdPtr,
		                               INFINITE))
		{
			LogErrorFuncLine(L"Failed GetQueuedCompletionStatus with GetLastError: %lu", GetLastError());
			// TODO: Possible to handle this better?
			ExitProcess(EXIT_FAILURE);
		}
		if (NULL != (void *)completionKey) {
			curWriteOp = CONTAINING_RECORD(ovlpdPtr, struct WriteOp, Ovrlp);
			FreeWriteOp(curWriteOp);
			if (!ReleaseSemaphore(ioAvailSemaphore, 1, NULL)) {
				LogErrorFuncLine(L"Failed ReleaseSemaphore with GetLastError: %lu", GetLastError());
				// TODO: Possible to handle this better?
				ExitProcess(EXIT_FAILURE);
			}
		} else {
			shutdown = TRUE;
		}
	}

	return 0;
}


int wmain(
	int         argc,
	wchar_t     **argv
	)
{
	HANDLE                      stdInHndl;
	HANDLE                      outHndl;
	HANDLE                      cleanupThreadHndl;
	HANDLE                      iocpHndl;
	HANDLE                      ioAvailSemaphore;
	OVERLAPPED                  outOvrlp;
	SSIZE_T                     bytsRd;
	DWORD                       outByts;
	UINT64                      processedByts;
	BOOL                        doLoop;
	FILE_SET_SPARSE_BUFFER      setSparseBuf;
	SSIZE_T                     fsClusterSize;
	ULARGE_INTEGER              tmpOfst;
	LARGE_INTEGER               flSize;
	DWORD                       lastErr;
	struct CleanupThreadParams  *tParams;
	struct WriteOp              *curWriteOp;

	if (2 != argc) {
		LogErrorFuncLine(L"Invalid command line parameters");
		ExitProcess(EXIT_FAILURE);
	}

	stdInHndl = GetStdHandle(STD_INPUT_HANDLE);

	outHndl = CreateFileW(argv[1],
	                      GENERIC_ALL,
	                      0,
	                      NULL,
	                      CREATE_NEW,
	                      FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
	                      NULL);
	if (INVALID_HANDLE_VALUE == outHndl) {
		lastErr = GetLastError();
		LogErrorFuncLine(L"Failed to create file %s with lastErr %lu.", argv[2], lastErr);
		ExitProcess(EXIT_FAILURE);
	}

	fsClusterSize = GetVolumeClusterSizeFromFileHandle(outHndl);
	if (fsClusterSize < 1) {
		LogError(L"Failed to read cluster size of storage volume. Defaulting to 4096 bytes.");
		fsClusterSize = 4096;
	}

	memset(&outOvrlp, 0, sizeof(outOvrlp));
	if (NULL == (outOvrlp.hEvent = CreateEventW(NULL, TRUE, TRUE, NULL))) {
		lastErr = GetLastError();
		LogErrorFuncLine(L"Failed CreateEventW with lastErr: %lu", lastErr);
		ExitProcess(EXIT_FAILURE);
	}

	/* Set the sparse attribute for the file */
	setSparseBuf.SetSparse = TRUE;
	if (!DeviceIoControl(outHndl,
	                     FSCTL_SET_SPARSE,
	                     &setSparseBuf,
	                     sizeof(setSparseBuf),
	                     NULL,
	                     0,
	                     &outByts,
	                     &outOvrlp)) {
		lastErr = GetLastError();
		if (lastErr != ERROR_IO_PENDING) {
			LogErrorFuncLine(L"Failed DeviceIoControl(FSCTL_SET_SPARSE) with lastErr: %lu", lastErr);
			ExitProcess(EXIT_FAILURE);
		}
	}

	if (!GetOverlappedResult(outHndl, &outOvrlp, &outByts, TRUE)) {
		lastErr = GetLastError();
		LogErrorFuncLine(L"Failed DeviceIoControl(FSCTL_SET_SPARSE) with lastErr: %lu", lastErr);
		ExitProcess(EXIT_FAILURE);
	}

	iocpHndl = CreateIoCompletionPort(outHndl, NULL, (ULONG_PTR)outHndl, 0);
	if (NULL == iocpHndl) {
		lastErr = GetLastError();
		LogErrorFuncLine(L"Failed CreateIoCompletionPort with lastErr: %lu", lastErr);
		ExitProcess(EXIT_FAILURE);
	}

	tParams = calloc(1, sizeof(*tParams));
	if (NULL == tParams) {
		// suppress unsafe usage warning for _wcserror since we're going to kill the process.
#pragma warning(suppress: 4996)
		LogErrorFuncLine(L"Memory allocation failure of size %zu with errno %d: %s", sizeof(*tParams), errno, _wcserror(errno));
		ExitProcess(EXIT_FAILURE);
	}

	ioAvailSemaphore = CreateSemaphoreW(NULL, MAX_PENDING_WRITES, MAX_PENDING_WRITES, NULL);
	if (NULL == ioAvailSemaphore) {
		lastErr = GetLastError();
		LogErrorFuncLine(L"Failed CreateSemaphoreW with lastErr: %lu", lastErr);
		ExitProcess(EXIT_FAILURE);
	}

	tParams->IOCompletionPort = iocpHndl;
	tParams->IoAvailSemaphore = ioAvailSemaphore;
	cleanupThreadHndl = CreateThread(NULL, 0, CleanupThread, tParams, 0, NULL);
	if (NULL == cleanupThreadHndl) {
		lastErr = GetLastError();
		LogErrorFuncLine(L"Failed CreateThread with lastErr: %lu", lastErr);
		ExitProcess(EXIT_FAILURE);
	}

	processedByts = 0;
	doLoop        = TRUE;
	curWriteOp    = NULL;
	do {
		if (!curWriteOp) {
			curWriteOp = AllocWriteOp((size_t)fsClusterSize);
			if (!curWriteOp) {
				LogErrorFuncLine(L"Failed AllocWriteOp()");
				ExitProcess(EXIT_FAILURE);
			}
		}

		bytsRd = FillBuf(stdInHndl, curWriteOp->Buf, (DWORD)curWriteOp->BufSz);
		if (fsClusterSize != bytsRd) {
			if (0 > bytsRd) {
				lastErr = GetLastError();
				LogErrorFuncLine(L"stdin read failure with lastErr: %ld", lastErr);
				ExitProcess(EXIT_FAILURE);
			}
			doLoop = FALSE;
		}

		if (bytsRd && (!IsZeroBuf(curWriteOp->Buf, (DWORD)bytsRd))) {
			lastErr = WaitForSingleObject(ioAvailSemaphore, INFINITE);
			if (WAIT_OBJECT_0 != lastErr) {
				LogErrorFuncLine(L"Failed WaitForSingleObject waitRet %lu and lastErr: %lu", lastErr, GetLastError());
				ExitProcess(EXIT_FAILURE);
			}
			tmpOfst.QuadPart                = processedByts;
			curWriteOp->Ovrlp.Offset        = tmpOfst.LowPart;
			curWriteOp->Ovrlp.OffsetHigh    = tmpOfst.HighPart;
			if (!WriteFile(outHndl,
			               curWriteOp->Buf,
			               (DWORD)bytsRd,
			               NULL,
			               &curWriteOp->Ovrlp))
			{
				lastErr = GetLastError();
				if (ERROR_IO_PENDING != lastErr) {
					LogErrorFuncLine(L"Failed to write to file with lastErr: %lu", lastErr);
					ExitProcess(EXIT_FAILURE);
				}
			}
			curWriteOp = NULL;
		}

		processedByts += bytsRd;
	} while (doLoop);

	if (curWriteOp)
		FreeWriteOp(curWriteOp);

	// Inform cleanup thread to shut itself down.
	if (!PostQueuedCompletionStatus(iocpHndl, 0, (ULONG_PTR)NULL, NULL)) {
		LogErrorFuncLine(L"Failed PostQueuedCompletionStatus with GetLastError(): %lu", GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	if (WAIT_OBJECT_0 != (lastErr = WaitForSingleObject(cleanupThreadHndl, INFINITE))) {
		LogErrorFuncLine(L"Failed WaitForSingleObject on cleanup thread with ret %ld and error %ld\n", lastErr, GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	CloseHandle(cleanupThreadHndl);

	/* Check that the final filesize matches number of bytes processed. */
	if (0 == GetFileSizeEx(outHndl, &flSize)) {
		LogErrorFuncLine(L"failed to get output file size with error: %ld\n", GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	if (flSize.QuadPart != (LONGLONG)processedByts) {
		flSize.QuadPart = processedByts;
		lastErr = SetFileSize(outHndl, flSize);
		if (ERROR_SUCCESS != lastErr) {
			LogErrorFuncLine(L"Failed SetFileSize with lastErr: %lu", GetLastError());
			ExitProcess(EXIT_FAILURE);
		}
	}

	/* clean-up */
	CloseHandle(outOvrlp.hEvent);
	CloseHandle(iocpHndl);
	CloseHandle(outHndl);
	CloseHandle(stdInHndl);
	CloseHandle(ioAvailSemaphore);
	return EXIT_SUCCESS;
}
