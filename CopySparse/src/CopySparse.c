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

#define DEFAULT_EXE_NAME    L"CopySparse.exe"

/* I came up with 512 MiB so the contiguous VA space required for file mappings
 * would fit in both 32 and 64 bit processes. Since VA space is limited in
 * 32-bit processes we need to be careful not to pick a value that is too large
 * since Windows loads various libraries into the process memory and randomizes
 * the address layout throughout the address space. There may be much better
 * values that could be picked for 32-bit processes. I didn't spend any real
 * time trying to find a good one and just picked it based on the factors noted
 * above.
*/
#define MAX_FILE_VIEW_SIZE (512 * 1024 * 1024)


static void __stdcall
PrintUsageInfo(
	LPWSTR exeName
	)
{
	LogInfo(L"Usage: %s [-h] [-m] INPUTFILE OUTPUTFILE\n\t-h Print this help message.\n", exeName);
}


_Must_inspect_result_
_Success_(return != 0)
static BOOL __stdcall
ParseArgs(
	_In_    int         argc,
	_In_    wchar_t     **argv,
	_Out_   LPWSTR      *SourceFileName,
	_Out_   LPWSTR      *TargetFileName
	)
{
	BOOL    retVal;
	BOOLEAN pcm;
	int     i;

	retVal = FALSE;
	pcm = FALSE;

	if ((argc < 3) || (argc > 4)) {
		PrintUsageInfo((argc < 1) ? DEFAULT_EXE_NAME : argv[0]);
		goto func_return;
	}

	for (i = 1; i < (argc - 2); ++i) {
		if (!wcscmp(L"-h", argv[i])) {
			PrintUsageInfo(argv[0]);
			goto func_return;
		}
	}

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
	LPWSTR                  sourceFileName, targetFileName;
	HANDLE                  sourceFile, targetFile;
	HANDLE                  sourceFileMap, targetFileMap;
	char                    *sourceViewBase, *targetViewBase;
	SIZE_T                  currentMapSize, currentMapAlignedDownSize, i;
	UINT64                  bytesProcessed, startQPC;
	FILETIME                ftCreate, ftAccess, ftWrite;
	FILE_SET_SPARSE_BUFFER  sparseBuf;
	LARGE_INTEGER           sourceFileSize, statsFreq;
	UINT64                  hours, minutes, seconds;
	HANDLE                  statsTimer;
	DWORD                   lastErr;
	double                  sourceFileSizeMiB;
	int                     retVal;
	ULONG_PTR               tmpULP;
	char                    tmpChar;

	SparseFileLibInit();

	sourceFile      = NULL;
	targetFile      = NULL;
	sourceFileMap   = NULL;
	targetFileMap   = NULL;
	sourceViewBase  = NULL;
	targetViewBase  = NULL;
	statsTimer      = NULL;

	bytesProcessed  = 0;

	startQPC = GetQPCVal();

	if (!ParseArgs(argc, argv, &sourceFileName, &targetFileName)) {
		goto error_return;
	}

	sourceFile = OpenFileExclusive(sourceFileName,
	                               FILE_FLAG_SEQUENTIAL_SCAN,
	                               &sourceFileSize,
	                               NULL,
	                               &ftCreate,
	                               &ftAccess,
	                               &ftWrite);
	if (!sourceFile) {
		lastErr = GetLastError();
		LogError(L"Failed to open file %s with lastErr %lu (0x%08lx)", sourceFileName, lastErr, lastErr);
		goto error_return;
	}

	sourceFileSizeMiB = (double)sourceFileSize.QuadPart / 1048576.0;

	targetFile = CreateFileW(targetFileName,
	                         GENERIC_ALL,
	                         0,
	                         NULL,
	                         CREATE_NEW,
	                         FILE_ATTRIBUTE_NORMAL,
	                         sourceFile);
	if (INVALID_HANDLE_VALUE == targetFile) {
		targetFile = NULL;
		lastErr = GetLastError();
		LogError(L"Failed CreateFileW for filename %s with lastErr %lu (0x%08lx)", targetFileName, lastErr, lastErr);
		goto error_return;
	}

	/* Set the sparse attribute on the target file */
	sparseBuf.SetSparse = TRUE;
	if (!DeviceIoControl(targetFile,
	                     FSCTL_SET_SPARSE,
	                     &sparseBuf,
	                     sizeof(sparseBuf),
	                     NULL,
	                     0,
	                     NULL,
	                     NULL)) {
		lastErr = GetLastError();
		LogError(L"Failed DeviceIoControl for FSCTL_SET_SPARSE with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}

	/* Set the target file size to match the source. */
	lastErr = SetFileSize(targetFile, sourceFileSize);
	if (ERROR_SUCCESS != lastErr) {
		LogError(L"Failed SetFileSize with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}
	/* Check if the source file is zero bytes. If it is we're done. Requesting
	 * zero-byte mappings from CreateFileMap is an error. */
	if (!sourceFileSize.QuadPart)
		goto out_stats;

	sourceFileMap = CreateFileMappingW(sourceFile,
		NULL,
		PAGE_READONLY,
		0,
		0,
		NULL);
	if (!sourceFileMap) {
		lastErr = GetLastError();
		LogError(L"Failed CreateFileMappingW with lastErr %lu (0x%08lx)", lastErr, lastErr);
		goto error_return;
	}

	targetFileMap = CreateFileMappingW(targetFile,
		NULL,
		PAGE_READWRITE,
		0,
		0,
		NULL);
	if (!targetFileMap) {
		lastErr = GetLastError();
		LogError(L"Failed CreateFileMappingW with lastErr %lu (0x%08lx)", lastErr, lastErr);
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

	/* Read the source and write to the target using a sliding window over the
	 * files. This allows the OS to only allocate blocks for mapped segments we
	 * actually wrote data to. It's fast for reading because there are zero
	 * memory copies involved and Windows can simply DMA the data directly to a
	 * physical page and map it to our address space. It's super fast for
	 * writing because only pages we actually write to in the target VA window
	 * get backed with a physical page and blocks allocated in the file system.
	 * The kernel never needs to copy memory, or even map a system address, and
	 * can simply DMA the physical page to disk whenever it decides to flush
	 * it's dirty page cache. Of course if we're operating on a file over the
	 * network or there are filter drivers scanning all IO (i.e. virus scanner)
	 * then things aren't quite as efficient on the backend, but it's still way
	 * better than using ReadFiles/WriteFile. */
	while (bytesProcessed < (UINT64)sourceFileSize.QuadPart) {
		currentMapSize = (SIZE_T)MIN(MAX_FILE_VIEW_SIZE, (UINT64)sourceFileSize.QuadPart - bytesProcessed);
		currentMapAlignedDownSize = ALIGN_DOWN_BY(currentMapSize, sizeof(tmpULP));

		sourceViewBase = MapViewOfFile(sourceFileMap,
			FILE_MAP_READ,
			(DWORD)(bytesProcessed >> 32),
			(DWORD)bytesProcessed,
			currentMapSize);
		if (!sourceViewBase) {
			lastErr = GetLastError();
			LogError(L"Failed MapViewOfFile with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}

		targetViewBase = MapViewOfFile(targetFileMap,
			FILE_MAP_WRITE,
			(DWORD)(bytesProcessed >> 32),
			(DWORD)bytesProcessed,
			currentMapSize);
		if (!targetViewBase) {
			lastErr = GetLastError();
			LogError(L"Failed MapViewOfFile with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}

		/* Need to put i here or the compiler will complain since it thinks it
		 * could be used unitialized in the __except block. It won't be
		 * uninitialized, but sometimes it's better not to fight the compiler.
		 */
		i = 0;
		__try {
			for (; i < currentMapAlignedDownSize; i += sizeof(tmpULP)) {
				/* Assignment to local ensures only a single deref */
				tmpULP = *(ULONG_PTR *)(sourceViewBase + i);
				if (tmpULP)
					*(ULONG_PTR *)(targetViewBase + i) = tmpULP;
			}

			/* Take care of any remaining data at the end of a view that is
			 * smaller than a ULONG_PTR */
			for (; i < currentMapSize; ++i) {
				/* Assignment to local ensures only a single deref */
				tmpChar = *(sourceViewBase + i);
				if (tmpChar)
					*(targetViewBase + i) = tmpChar;
			}
		} __except (GetExceptionCode() == EXCEPTION_IN_PAGE_ERROR
		                               ?  EXCEPTION_EXECUTE_HANDLER
		                               :  EXCEPTION_CONTINUE_SEARCH) {
			bytesProcessed += i;
			LogError(L"Failed to read or write to files at offset: %llu", bytesProcessed);
			goto error_return;
		}

		bytesProcessed += currentMapSize;

		lastErr = WaitForSingleObject(statsTimer, 0);
		if (WAIT_OBJECT_0 == lastErr) {
			LogInfo(L"Copied: %8.2f MiB of %8.2f MiB\n", (double)bytesProcessed / 1048576.0, sourceFileSizeMiB);
			(void)SetWaitableTimer(statsTimer, &statsFreq, 0, NULL, NULL, FALSE);
		} else if (WAIT_TIMEOUT != lastErr) {
			LogError(L"Unexpected WaitForSingleObject return 0x%08lX GetLastError 0x%08lX in wait call for statsTimer\n",
				lastErr, GetLastError());
			goto error_return;
		}

		if (!UnmapViewOfFile(sourceViewBase)) {
			lastErr = GetLastError();
			LogError(L"Failed UnmapViewOfFile with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}
		sourceViewBase = NULL;
		if (!UnmapViewOfFile(targetViewBase)) {
			lastErr = GetLastError();
			LogError(L"Failed UnmapViewOfFile with lastErr %lu (0x%08lx)", lastErr, lastErr);
			goto error_return;
		}
		targetViewBase = NULL;
	}

	/* Finished copying file. Start clean up. */
	(void)CloseHandle(sourceFileMap);
	sourceFileMap = NULL;
	(void)CloseHandle(targetFileMap);
	targetFileMap = NULL;

	/* Set timestamps on target from source file. */
	if (!SetFileTime(targetFile, &ftCreate, &ftAccess, &ftWrite)) {
		lastErr = GetLastError();
		LogError(L"Failed to write file time values to target file with lastErr %lu (0x%08lx)\n", lastErr, lastErr);
	}

	(void)CloseHandle(sourceFile);
	sourceFile = NULL;

	/* Flush buffers on target file */
	if (!FlushFileBuffers(targetFile)) {
		lastErr = GetLastError();
		LogError(L"WARNING: Failed FlushFileBuffers on target file with lastErr %lu.\n", lastErr);
	}

	(void)CloseHandle(targetFile);
	targetFile = NULL;

out_stats:
	seconds = ElapsedQPCInSeconds(startQPC, GetQPCVal());
	hours = seconds / (60 * 60);
	seconds = seconds % (60 * 60);
	minutes = seconds / 60;
	seconds = seconds % 60;

	LogInfo(L"Sparse file copy complete.\n"
	        L"%llu hours, %llu minutes, %llu seconds.\n"
	        L"%16llu bytes read\n%16.2f MiB read\n%16.2f GiB read\n",
	        hours, minutes, seconds,
	        bytesProcessed, (double)bytesProcessed / 1048576.0, (double)bytesProcessed / 1073741824.0);

	retVal = EXIT_SUCCESS;

	goto func_return;

error_return:
	retVal = EXIT_FAILURE;

func_return:
	if (targetViewBase)
		(void)UnmapViewOfFile(targetViewBase);
	if (sourceViewBase)
		(void)UnmapViewOfFile(sourceViewBase);
	if (targetFileMap)
		(void)CloseHandle(targetFileMap);
	if (sourceFileMap)
		(void)CloseHandle(sourceFileMap);
	if (sourceFile)
		(void)CloseHandle(sourceFile);
	if (targetFile)
		(void)CloseHandle(targetFile);
	if (statsTimer)
		(void)CloseHandle(statsTimer);
	return retVal;
}

