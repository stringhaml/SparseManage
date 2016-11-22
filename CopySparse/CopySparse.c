/* Standard BSD license disclaimer.

Copyright(c) 2016, Lance D. Stringham
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


#include <windows.h>
#include <tchar.h>

#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>

#define DEFAULT_EXE_NAME    (TEXT("CopySparse.exe"))

#define CHUNK_SIZE    (4 * 1024)

/* Local helper functions */
static void PrintUsageInfo(_TCHAR *exeName);

int _tmain(int argc, _TCHAR **argv)
{
	HANDLE                  inFileHndl;
	HANDLE                  outFileHndl;
	HANDLE                  statsTmrHndl;
	OVERLAPPED              inOvrlp;
	OVERLAPPED              outOvrlp;
	DWORD                   inByts;
	DWORD                   outByts;
	DWORD                   waitRtrn;
	FILETIME                tmCrt;
	FILETIME                tmAcc;
	FILETIME                tmWrt;
	FILE_SET_SPARSE_BUFFER  sparseBuf;
	LARGE_INTEGER           inFileSize;
	LARGE_INTEGER           statsFreq;
	ULARGE_INTEGER          tmpOfst;
	uint64_t                curOfst;
	uint64_t                bytsWrtn;
	uint64_t                bytsRd;
	int64_t                 bytsRemaining;
	BOOL                    rslt;
	int                     i;
	char                    *prevBuf;
	char                    *curBuf;
	char                    *nextBuf;

	if (argc != 3) {
		PrintUsageInfo((argc < 1) ? DEFAULT_EXE_NAME : argv[0]);
		return (EXIT_FAILURE);
	}

	inFileHndl = CreateFile(argv[1], GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
	if (INVALID_HANDLE_VALUE == inFileHndl) {
		_tprintf(_T("failed to open file %s with error %d.\n"), argv[1], GetLastError());
		return (EXIT_FAILURE);
	}
	if (FALSE == GetFileTime(inFileHndl, &tmCrt, &tmAcc, &tmWrt)) {
		_tprintf(_T("Failed to read time values of reference file.\n"));
		CloseHandle(inFileHndl);
		return (EXIT_FAILURE);
	}

	outFileHndl = CreateFile(argv[2], GENERIC_ALL, 0, NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, inFileHndl);
	if (INVALID_HANDLE_VALUE == outFileHndl) {
		_tprintf(_T("failed to create file %s with error %d.\n"), argv[2], GetLastError());
		return (EXIT_FAILURE);
	}

	memset(&inOvrlp, 0, sizeof(inOvrlp));
	memset(&outOvrlp, 0, sizeof(outOvrlp));

	inOvrlp.hEvent = CreateEvent(NULL, TRUE, TRUE, NULL);
	outOvrlp.hEvent = CreateEvent(NULL, TRUE, TRUE, NULL);
	if (NULL == inOvrlp.hEvent || NULL == outOvrlp.hEvent) {
		_tprintf(_T("Failed to create events"));
		return(EXIT_FAILURE);
	}

	/* Set the sparse attribute for the new file */
	sparseBuf.SetSparse = TRUE;
	if (0 == DeviceIoControl(outFileHndl, FSCTL_SET_SPARSE, &sparseBuf, sizeof(sparseBuf), NULL, 0, &outByts, &outOvrlp) && GetLastError() != ERROR_IO_PENDING) {
		_tprintf(_T("Failed to set sparse attribute\n"));
		return (EXIT_FAILURE);
	}
	/* Wait for ioctl to complete */
	if (0 == GetOverlappedResult(outFileHndl, &outOvrlp, &outByts, TRUE)) {
		_tprintf(_T("Failed sparse ioctl with error %d\n"), GetLastError());
		return (EXIT_FAILURE);
	}

	/* create and set waitable timer for stats output */
	statsFreq.QuadPart = -100000000ll; /* 10 seconds */
	statsTmrHndl = CreateWaitableTimer(NULL, TRUE, NULL);
	SetWaitableTimer(statsTmrHndl, &statsFreq, 0, NULL, NULL, FALSE);

	/* Initialize loop and stats values */
	bytsWrtn = 0;
	bytsRd = 0;
	curOfst = 0;
	prevBuf = NULL;
	curBuf = NULL;

	/* Get files size */
	if (0 == GetFileSizeEx(inFileHndl, &inFileSize)) {
		abort();
	}

	/* Set bytes remaining for loop termination */
	bytsRemaining = inFileSize.QuadPart;

	/* Setup read buffer and start initial read */
	if (NULL == (nextBuf = calloc(1, CHUNK_SIZE))) {
		abort();
	}
	if (0 == ReadFile(inFileHndl, nextBuf, CHUNK_SIZE, NULL, &inOvrlp) && GetLastError() != ERROR_IO_PENDING) {
		abort();
	}

	/* Read, analyze, write loop */
	while (bytsRemaining > CHUNK_SIZE) {
		/* Wait for the pending read to complete */
		if (0 == GetOverlappedResult(inFileHndl, &inOvrlp, &inByts, TRUE)) {
			abort();
		}
		/* update read counter */
		bytsRd += inByts;

		/* Setup for next read */
		curBuf = nextBuf;
		if (NULL == (nextBuf = calloc(1, CHUNK_SIZE))) {
			abort();
		}
		/* Start next read */
		tmpOfst.QuadPart    = curOfst + CHUNK_SIZE;
		inOvrlp.Offset      = tmpOfst.LowPart;
		inOvrlp.OffsetHigh  = tmpOfst.HighPart;
		if (0 == ReadFile(inFileHndl, nextBuf, CHUNK_SIZE, NULL, &inOvrlp) && GetLastError() != ERROR_IO_PENDING) {
			abort();
		}

		/* Look at results of last read */
		for (i = 0; i < CHUNK_SIZE; i++) {
			if (*(curBuf + i) != '\0') break;
		}

		/* Determine if anything other than zeros was read */
		if (i != CHUNK_SIZE) {
			/* if so wait for the previous write to complete */
			if (0 == GetOverlappedResult(outFileHndl, &outOvrlp, &outByts, TRUE)) abort();
			/* Free old buffer */
			free(prevBuf);
			prevBuf = curBuf;
			/* Update write counter */
			bytsWrtn += outByts;
			tmpOfst.QuadPart    = curOfst;
			outOvrlp.Offset     = tmpOfst.LowPart;
			outOvrlp.OffsetHigh = tmpOfst.HighPart;
			/* Initiate write */
			if (0 == WriteFile(outFileHndl, prevBuf, CHUNK_SIZE, NULL, &outOvrlp) && GetLastError() != ERROR_IO_PENDING) {
				abort();
			}
		} else {
			free(curBuf);
		}

		/* Output current statistics on timed interval */
		if (WAIT_OBJECT_0 == (waitRtrn = WaitForSingleObject(statsTmrHndl, 0))) {
			_tprintf(_T("processed: %8llu MiB of %8lld MiB\n"), curOfst >> 20, inFileSize.QuadPart >> 20);
			SetWaitableTimer(statsTmrHndl, &statsFreq, 0, NULL, NULL, FALSE);
		} else if (waitRtrn != WAIT_TIMEOUT) {
			abort();
		}

		/* Update file and loop offsets */
		bytsRemaining -= CHUNK_SIZE;
		curOfst += CHUNK_SIZE;
	}

	/* Deal with leftovers and always write out last file section */
	/* Wait for last read to complete */
	rslt = GetOverlappedResult(inFileHndl, &inOvrlp, &inByts, TRUE);
	if (0 == rslt && GetLastError() != ERROR_HANDLE_EOF) {
		abort();
	}
	/* update read counter */
	bytsRd += inByts;

	/* Validate read size */
	if (inByts > CHUNK_SIZE) {
		abort();
	}

	if (inByts != bytsRemaining) {
		_tprintf(_T("Warning: final read not of expected size. Read %lu expected %lld\n"), inByts, bytsRemaining);
	}

	/* Wait for the previous write to complete */
	if (0 == GetOverlappedResult(outFileHndl, &outOvrlp, &outByts, TRUE)) abort();
	/* Free old buffer */
	free(prevBuf);
	curBuf = nextBuf;
	/* Update write counter */
	bytsWrtn += outByts;
	tmpOfst.QuadPart = curOfst;
	outOvrlp.Offset = tmpOfst.LowPart;
	outOvrlp.OffsetHigh = tmpOfst.HighPart;
	/* Initiate write */
	if (0 == WriteFile(outFileHndl, curBuf, inByts, NULL, &outOvrlp) && GetLastError() != ERROR_IO_PENDING) {
		abort();
	}

	/* Wait for write to complete */
	if (0 == GetOverlappedResult(outFileHndl, &outOvrlp, &outByts, TRUE)) abort();
	/* Update write counter */
	bytsWrtn += outByts;

	/* Copy timestamps from original file. */
	if (FALSE == SetFileTime(outFileHndl, &tmCrt, &tmAcc, &tmWrt)) {
		_tprintf(_T("Failed to write time values to target file.\n"));
	}

	_tprintf(_T("Sparse file copy complete.\n%16lld bytes read\n%16lld MiB read\n%16lld GiB read\n%16lld bytes written\n%16lld MiB written\n%16lld GiB written\n"),
	         bytsRd, bytsRd >> 20, bytsRd >> 30, bytsWrtn, bytsWrtn >> 20, bytsWrtn >> 30);

	/* Clean up */
	free(curBuf);
	FlushFileBuffers(outFileHndl);
	CloseHandle(inOvrlp.hEvent);
	CloseHandle(outOvrlp.hEvent);
	CloseHandle(inFileHndl);
	CloseHandle(outFileHndl);
	CloseHandle(statsTmrHndl);

	return (EXIT_SUCCESS);
}

static void PrintUsageInfo(_TCHAR *exeName)
{
	_tprintf(_T("Usage: %s INPUTFILE OUTPUTFILE\n"), exeName);

	return;
}

