/* PipeSparse.c - Accept piped input and output to a sparse file. */

#include <windows.h>
#include <tchar.h>

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <assert.h>


#define S(x)        #x
#define S_(x)       S(x)
#define S_LINE_     S_(__LINE__)

#define MAX_MEMORY_USAGE        (1024 * 1024 * 1024)
#define BUFFER_READ_SIZE        (1024 * 1024)

struct WriteOp {
	char        *Buf;
	size_t      BufSz;
	size_t      BufUsed;
	OVERLAPPED  Ovrlp;
};

struct CleanupThreadParams {
	volatile LONG       *WriteOpsPending;
	HANDLE              IOCompletionPort;
};


_Success_(return != NULL)
static struct WriteOp * AllocWriteOp(
	_In_        size_t      BufSz
	)
{
	struct WriteOp *op;

	if (NULL == (op = calloc(1, sizeof(*op)))) {
		_ftprintf(stderr, _T("Memory allocation failure at: ") _T(S_LINE_) _T("\n"));
		return NULL;
	}

	if (NULL == (op->Buf = malloc(BufSz))) {
		_ftprintf(stderr, _T("Memory allocation failure at: ") _T(S_LINE_) _T("\n"));
		free(op);
		return NULL;
	}
	op->BufSz = BufSz;

	if (NULL == (op->Ovrlp.hEvent = CreateEvent(NULL, TRUE, TRUE, NULL))) {
		_ftprintf(stderr, _T("Failed to create event at: ") _T(S_LINE_) _T("\n"));
		free(op->Buf);
		free(op);
		return NULL;
	}

	return op;
}

static void FreeWriteOp(
	_In_    struct WriteOp  *Op
	)
{
	assert(NULL != Op);

	if (NULL != Op->Buf) {
		free(Op->Buf);
	}
	if (NULL != Op->Ovrlp.hEvent) {
		CloseHandle(Op->Ovrlp.hEvent);
	}

	free(Op);
}

/* Determine the cluster size of the file system that the handle resides on.
 * Returns -1 on failure. */
static SSIZE_T GetDriveClusterSize(
	_In_    HANDLE      Fl
	)
{
	DWORD   sectorsPerCluster;
	DWORD   bytesPerSector;
	DWORD   numberOfFreeClusters;
	DWORD   totalNumberOfClusters;
	DWORD   flNameLen;
	DWORD   tmp;
	SSIZE_T clusterSize;
	int     delimCnt;
	TCHAR   *ofst;
	TCHAR   *flName;

	clusterSize = -1;

	/* Determine buffer size to allocate. */
	flNameLen = GetFinalPathNameByHandle(Fl, NULL, 0, VOLUME_NAME_GUID);
	if (0 == flNameLen)
		return clusterSize;

	flName = calloc(1, sizeof(TCHAR) * (flNameLen + 1));
	if (NULL == flName) {
		SetLastError(ERROR_NOT_ENOUGH_MEMORY);
		return clusterSize;
	}

	tmp = GetFinalPathNameByHandle(Fl, flName, flNameLen + 1, VOLUME_NAME_GUID);
	if (0 == tmp || flNameLen < tmp)
		goto free_name_mem;

	// DEBUG
	//_tprintf(_T("Filename: %s\n"), flName);

	ofst = flName;
	delimCnt = 0;
	while (delimCnt < 4 && *ofst != _T('\0')) {
		if (*ofst == _T('\\')) delimCnt++;
		ofst = CharNext(ofst);
	}

	if (delimCnt != 4) {
		SetLastError(ERROR_DEVICE_FEATURE_NOT_SUPPORTED);
		goto free_name_mem;
	}

	if ((flName + flNameLen) - ofst <= 0) {
		SetLastError(ERROR_DEVICE_FEATURE_NOT_SUPPORTED);
		goto free_name_mem;
	}

	*ofst = _T('\0'); /* Terminate the string after last delimiter. */
	//_tprintf(_T("Filename: %s\n"), flName);

	if (FALSE == GetDiskFreeSpace(flName, &sectorsPerCluster, &bytesPerSector, &numberOfFreeClusters, &totalNumberOfClusters))
		goto free_name_mem;

	clusterSize = bytesPerSector * sectorsPerCluster;

free_name_mem:
	free(flName);
	return (clusterSize);
}


static SSIZE_T FillBuf(
	_In_        HANDLE      InputHndl,
	_Out_       LPVOID      Buf,
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


// TODO: Determine fastest zero analysis.
static BOOL IsZeroBuf(
	_In_        LPVOID      Buf,
	_In_        DWORD       BufSz
	)
{
	char *p, *e;

	p = Buf;
	e = p + BufSz;

	while (*p == 0 && ++p < e);

	return (p >= e) ? TRUE : FALSE;
}

static DWORD WINAPI CleanupThread(LPVOID Params)
{
	BOOL                        shutdown;
	HANDLE                      iocpHndl;
	DWORD                       bytsWrtn;
	ULONG_PTR                   completionKey;
	LPOVERLAPPED                ovlpdPtr;
	struct WriteOp              *curWriteOp;
	struct CleanupThreadParams  *inParams;
	volatile LONG               *writeOpsPending;

	inParams = Params;
	iocpHndl = inParams->IOCompletionPort;
	writeOpsPending = inParams->WriteOpsPending;

	shutdown = FALSE;
	while (!shutdown || *writeOpsPending) {
		if (0 == GetQueuedCompletionStatus(iocpHndl, &bytsWrtn, &completionKey, &ovlpdPtr, INFINITE)) {
			_ftprintf(stderr, _T("Failed GetQueuedCompletionStatus with error: %ld\n"), GetLastError());
			ExitProcess(EXIT_FAILURE);
		}
		if (NULL != (void *)completionKey) {
			curWriteOp = CONTAINING_RECORD(ovlpdPtr, struct WriteOp, Ovrlp);
			FreeWriteOp(curWriteOp);
			InterlockedDecrement(writeOpsPending);
		} else {
			shutdown = TRUE;
		}
	}

	return 0;
}


int _tmain(int argc, _TCHAR **argv)
{
	HANDLE                      stdInHndl;
	HANDLE                      outHndl;
	HANDLE                      cleanupThreadHndl;
	HANDLE                      iocpHndl;
	OVERLAPPED                  outOvrlp;
	SSIZE_T                     bytsRd;
	DWORD                       outByts;
	SIZE_T                      processedByts;
	BOOL                        doLoop;
	FILE_SET_SPARSE_BUFFER      setSparseBuf;
	SSIZE_T                     fsClusterSize;
	ULARGE_INTEGER              tmpOfst;
	LARGE_INTEGER               flSize;
	DWORD                       err;
	struct CleanupThreadParams  *tParams;
	struct WriteOp              *curWriteOp;
	volatile LONG               writeOpsPending;

	if (2 != argc) {
		_ftprintf(stderr, _T("Invalid command line parameters\n"));
		ExitProcess(EXIT_FAILURE);
	}

	stdInHndl = GetStdHandle(STD_INPUT_HANDLE);

	outHndl = CreateFile(argv[1], GENERIC_ALL, 0, NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, NULL);
	if (INVALID_HANDLE_VALUE == outHndl) {
		_ftprintf(stderr, _T("failed to create file %s with error %d.\n"), argv[2], GetLastError());
		ExitProcess(EXIT_FAILURE);
	}

	fsClusterSize = GetDriveClusterSize(outHndl);
	if (fsClusterSize > MAX_MEMORY_USAGE) {
		_ftprintf(stderr, _T("Cluster size of storage exceeds maximum allowed memory usage: %d\n"), MAX_MEMORY_USAGE);
		ExitProcess(EXIT_FAILURE);
	}
	if (fsClusterSize < 1) {
		_ftprintf(stderr, _T("Failed to read cluster size of storage volume.\n"));
		ExitProcess(EXIT_FAILURE);
	}

	memset(&outOvrlp, 0, sizeof(outOvrlp));
	if (NULL == (outOvrlp.hEvent = CreateEvent(NULL, TRUE, TRUE, NULL))) {
		_ftprintf(stderr, _T("Failed to create event at: ") _T(S_LINE_) _T("\n"));
		ExitProcess(EXIT_FAILURE);
	}

	/* Set the sparse attribute for the file */
	setSparseBuf.SetSparse = TRUE;
	if (0 == DeviceIoControl(outHndl, FSCTL_SET_SPARSE, &setSparseBuf, sizeof(setSparseBuf), NULL, 0, &outByts, &outOvrlp)
		&& GetLastError() != ERROR_IO_PENDING) {
		_ftprintf(stderr, _T("Failed to set sparse attribute\n"));
		ExitProcess(EXIT_FAILURE);
	}

	if (0 == GetOverlappedResult(outHndl, &outOvrlp, &outByts, TRUE)) {
		_ftprintf(stderr, _T("Failed to set sparse attribute\n"));
		ExitProcess(EXIT_FAILURE);
	}

	if (NULL == (iocpHndl = CreateIoCompletionPort(outHndl, NULL, (ULONG_PTR)outHndl, 0))) {
		_ftprintf(stderr, _T("Failed to allocate io completion port: ") _T(S_LINE_) _T("\n"));
		ExitProcess(EXIT_FAILURE);
	}

	if (NULL == (tParams = calloc(1, sizeof(*tParams)))) {
		_ftprintf(stderr, _T("Memory allocation failure at: ") _T(S_LINE_) _T("\n"));
		ExitProcess(EXIT_FAILURE);
	}

	writeOpsPending = 0;
	tParams->IOCompletionPort = iocpHndl;
	tParams->WriteOpsPending = &writeOpsPending;
	if (NULL == (cleanupThreadHndl = CreateThread(NULL, 0, CleanupThread, tParams, 0, NULL))) {
		_ftprintf(stderr, _T("Failed to spawn cleanup thread: ") _T(S_LINE_) _T("\n"));
		ExitProcess(EXIT_FAILURE);
	}

	processedByts = 0;
	curWriteOp = NULL;
	doLoop = TRUE;
	while (doLoop) {
		if (NULL == curWriteOp && (NULL == (curWriteOp = AllocWriteOp(((size_t)fsClusterSize))))) {
			_ftprintf(stderr, _T("WriteOp allocation failure at: ") _T(S_LINE_) _T("\n"));
			ExitProcess(EXIT_FAILURE);
		}

		if (fsClusterSize != (bytsRd = FillBuf(stdInHndl, curWriteOp->Buf, (DWORD)curWriteOp->BufSz))) {
			if (0 > bytsRd) {
				_ftprintf(stderr, _T("stdin read failure with error: %ld\n"), GetLastError());
				ExitProcess(EXIT_FAILURE);
			}
			doLoop = FALSE;
		}

		curWriteOp->BufUsed = bytsRd;

		if (curWriteOp->BufUsed && (!IsZeroBuf(curWriteOp->Buf, (DWORD)curWriteOp->BufUsed))) {
			while ((writeOpsPending * fsClusterSize) >= MAX_MEMORY_USAGE) {
				Sleep(1); // Wait for buffers to be freed by cleanup thead.
			}
			InterlockedIncrement(&writeOpsPending);
			tmpOfst.QuadPart                = processedByts;
			curWriteOp->Ovrlp.Offset        = tmpOfst.LowPart;
			curWriteOp->Ovrlp.OffsetHigh    = tmpOfst.HighPart;
			if ((0 == WriteFile(outHndl, curWriteOp->Buf, (DWORD)curWriteOp->BufUsed, NULL, &curWriteOp->Ovrlp))
				&& (ERROR_IO_PENDING != (err = GetLastError()))) {
				_ftprintf(stderr, _T("Failed to write to file with error: %ld\n"), err);
				ExitProcess(EXIT_FAILURE);
			}
			curWriteOp = NULL;
		}
		processedByts += bytsRd;
	}

	// Inform cleanup thread to shut itself down.
	if (!PostQueuedCompletionStatus(iocpHndl, 0, (ULONG_PTR)NULL, NULL)) {
		_ftprintf(stderr, _T("failed to post to io completion port with error: %ld\n"), GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	if (WAIT_OBJECT_0 != (err = WaitForSingleObject(cleanupThreadHndl, INFINITE))) {
		_ftprintf(stderr, _T("failed to wait on cleanup thread with ret %ld and error %ld\n"), err, GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	CloseHandle(cleanupThreadHndl);

	/* Check that the final filesize matches number of bytes processed. */
	if (0 == GetFileSizeEx(outHndl, &flSize)) {
		_ftprintf(stderr, _T("failed to get output file size with error: %ld\n"), GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	if (flSize.QuadPart != (LONGLONG)processedByts) {
		flSize.QuadPart = processedByts;
		if (0 == SetFilePointerEx(outHndl, flSize, NULL, FILE_BEGIN)) {
			_ftprintf(stderr, _T("failed to move file pointer with error: %ld\n"), GetLastError());
			ExitProcess(EXIT_FAILURE);
		}
		if (0 == SetEndOfFile(outHndl)) {
			_ftprintf(stderr, _T("failed to set end of file with error: %ld\n"), GetLastError());
			ExitProcess(EXIT_FAILURE);
		}
	}

	/* clean-up */
	CloseHandle(outOvrlp.hEvent);
	CloseHandle(iocpHndl);
	CloseHandle(outHndl);
	CloseHandle(stdInHndl);
	return EXIT_SUCCESS;
}