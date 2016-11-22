// MakeSparse.c : Defines the entry point for the console application.
//

#include <windows.h>
#include <tchar.h>

#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>

#include <assert.h>

/* TODO: For windows 8 / server 2012 use GetFileInformationByHandleEx function
 * to query OS about the file sector size and alignment rather then the current
 * method of parsing the file handle path to open a handle to the drive and
 * using GetDiskFreeSpace to calculate the cluster size and assuming zero based
 * alignment. */

/* TODO: Chunk file analysis and deallocation for files roughly 60 TiB or more
 * (assuming 4k clusters) since way may start running into memory issues with
 * only 2 GiB of usable memory in 32-bit environments. */

/* TODO: Query existing sparse ranges and don't re-analyze them. */

/* TODO: Make this dynamic */
#define MAX_PENDING_IO      512


typedef struct IO_CP_CB_CTX {
	HANDLE          FileHandle;
	LARGE_INTEGER   FileSize;
	SIZE_T          FsClusterSize;
	HANDLE          IoAvailEvt;
	volatile LONG   PendingIo;
	volatile PLONG  ZeroClusterMap;
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


static VOID PrintUsageInfo()
{
	// TODO: Make this better.
	_tprintf(_T("MakeSparse.exe [-p] FileToMakeSparse\nSpecify -p to preserve file timestamps."));
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


/* TODO: Determine fastest portable zero analysis. */
_Success_(return == TRUE)
static BOOL IsZeroBuf(
	_In_    LPVOID      Buf,
	_In_    DWORD       BufSz
	)
{
	char *p, *e;

	p = Buf;
	e = p + BufSz;

	while (*p == 0 && ++p < e);

	return (p >= e) ? TRUE : FALSE;
}


/* Note that if allocating an IO_READ the memory pointed to by ReadBuf is not
 * zeroed. */
_Success_(return != NULL)
static PIO_OP AllocIoOp(
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


static VOID FreeIoOp(
	_In_    PIO_OP      Op
	)
{
	if (IO_READ == Op->OpType)
		_aligned_free(Op->ReadBuf);
	free(Op);
}


// On 32-bit enviornments this will fail with cluster maps using > 2GB memory.
static VOID MarkZeroCluster(
	_Inout_     volatile PLONG  ClusterMap,
	_In_        SIZE_T          ClusterSize,
	_In_        UINT64          StartingByteOffset
	)
{
	UINT64 mapBit;

	assert(!(StartingByteOffset % ClusterSize));

	mapBit = StartingByteOffset / ClusterSize;
	InterlockedBitTestAndSet(ClusterMap + (mapBit / 32), mapBit & 31);
}


static BOOLEAN IsMarkedZero(
	_In_        volatile PLONG  ClusterMap,
	_In_        UINT64          Cluster
	)
{
	return (*(ClusterMap + (Cluster / 32)) & (1 << (Cluster & 31))) ? TRUE : FALSE;
}


static VOID PrintClusterMap(
	_In_        volatile PLONG  ClusterMap,
	_In_        SIZE_T          ClusterSize,
	_In_        UINT64          FileSize
	)
{
	UINT64 numClusters, i, displayGroups;

	numClusters = FileSize / ClusterSize;
	if (FileSize % ClusterSize)
		++numClusters;

	for (i = 0, displayGroups = 0; i < numClusters; ++i) {
		if (!(displayGroups % 16) && !(i % 4))
			_tprintf(_T("\n%016llX"), i * ClusterSize);
		if (!(i % 4)) {
			++displayGroups;
			_tprintf(_T(" "));
		}
		_tprintf(IsMarkedZero(ClusterMap, i) ? _T("1") : _T("0"));
	}
	_tprintf(_T("\n"));
}

static VOID CALLBACK ProcessCompletedIoCallback(
	_Inout_     PTP_CALLBACK_INSTANCE Instance,
	_Inout_opt_ PVOID                 Context,
	_Inout_opt_ PVOID                 Overlapped,
	_In_        ULONG                 IoResult,
	_In_        ULONG_PTR             NumBytesTxd,
	_Inout_     PTP_IO                pTpIo
	)
{
	LPOVERLAPPED    pOvrlp;
	PIO_OP          pIoOp;
	PIO_CB_SHARED   cbCtx;
	LARGE_INTEGER   startOffset;

	UNREFERENCED_PARAMETER(Instance);
	UNREFERENCED_PARAMETER(pTpIo);

	if (NULL == Context) {
		_ftprintf(stderr,
			_T("No context provided for io completion callback\n"));
		ExitProcess(EXIT_FAILURE);
	}

	cbCtx = Context;

	InterlockedDecrement(&cbCtx->PendingIo);
	SetEvent(cbCtx->IoAvailEvt);

	if (NULL == Overlapped) {
		_ftprintf(stderr, _T("Received unexpected NULL overlapped in io completion callback.\n"));
		// TODO: Make this nicer.
		ExitProcess(EXIT_FAILURE);
	}
	pOvrlp = Overlapped;

	pIoOp = CONTAINING_RECORD(pOvrlp, IO_OP, Ovlp);

	switch (pIoOp->OpType) {
	case IO_READ:
		if (ERROR_SUCCESS == IoResult && NumBytesTxd) {
			for (SIZE_T i = 0; i < NumBytesTxd; i += cbCtx->FsClusterSize) {
				if ((i + cbCtx->FsClusterSize) <= NumBytesTxd) {
					if (IsZeroBuf(pIoOp->ReadBuf + i, (DWORD)cbCtx->FsClusterSize)) {
						startOffset.LowPart = pOvrlp->Offset;
						startOffset.HighPart = pOvrlp->OffsetHigh;
						startOffset.QuadPart += i;
						MarkZeroCluster(cbCtx->ZeroClusterMap, cbCtx->FsClusterSize, startOffset.QuadPart);
					}
				} else {
					startOffset.LowPart = pOvrlp->Offset;
					startOffset.HighPart = pOvrlp->OffsetHigh;
					// check if EOF runt
					if ((cbCtx->FileSize.QuadPart - startOffset.QuadPart) == (LONGLONG)NumBytesTxd) {
						if (IsZeroBuf(pIoOp->ReadBuf + i, (DWORD)(NumBytesTxd - i)))
							MarkZeroCluster(cbCtx->ZeroClusterMap, cbCtx->FsClusterSize, startOffset.QuadPart + i);
					}
				}
			}
		} else {
			_ftprintf(stderr,
				_T("Error reading from file at offset 0x%08llX%08llX\n"),
				(long long)pOvrlp->OffsetHigh, (long long)pOvrlp->Offset);
			// TODO: Make this nicer.
			ExitProcess(EXIT_FAILURE);
		}
		break;
	case IO_SET_SPARSE:
	case IO_SET_ZERO_RANGE:
		if (ERROR_SUCCESS != IoResult) {
			_ftprintf(stderr,
				_T("Error in %s ioctl call at offset 0x%08llX%08llX\n"),
				IO_SET_ZERO_RANGE == pIoOp->OpType ? _T("set zero range") : _T("set sparse attribute"),
				(long long)pOvrlp->OffsetHigh, (long long)pOvrlp->Offset);
			// TODO: Make this nicer.
			ExitProcess(EXIT_FAILURE);
		}

		break;
	default:
		_ftprintf(stderr,
			_T("Received unexpected io op type %d in io completion callback\n"),
			pIoOp->OpType);
		ExitProcess(EXIT_FAILURE);
	}
	FreeIoOp(pIoOp);
}


/* Helper function for single dispatch thread. Don't use with with multiple
 * dispatch threads. */
static VOID WaitAvailableIo(
	_Inout_     PIO_CB_SHARED   IoCbCtx
	)
{
	DWORD   ret;
	LONG    pendingIOs;

	ret = WaitForSingleObject(IoCbCtx->IoAvailEvt, INFINITE);
	if (WAIT_OBJECT_0 != ret) {
		_ftprintf(stderr,
			_T("Error %#llx in WaitForSingleObject while waiting for available IO op.\n"),
			(long long)GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	pendingIOs = InterlockedIncrement(&IoCbCtx->PendingIo);
	if (MAX_PENDING_IO <= pendingIOs)
		if (!ResetEvent(IoCbCtx->IoAvailEvt)) {
			_ftprintf(stderr, _T("Error %#llx resetting IoAvailEvt.\n"),
				(long long)GetLastError());
			ExitProcess(EXIT_FAILURE);
		}
}


static DWORD DispatchFileReads(
	_Inout_     PIO_CB_SHARED       IoCbCtx,
	_In_        PTP_IO              IoTp
	)
{
	SYSTEM_INFO     sysInfo;
	SIZE_T          pageSize;
	LARGE_INTEGER   flOffset;
	UINT64          bytesToRead;
	PIO_OP          pIoOp;
	BOOL            ret;
	DWORD           lastErr;

	GetSystemInfo(&sysInfo);
	pageSize = sysInfo.dwPageSize;

	flOffset.QuadPart = 0;
	while (flOffset.QuadPart < IoCbCtx->FileSize.QuadPart) {
		WaitAvailableIo(IoCbCtx);
		pIoOp = AllocIoOp(IO_READ, IoCbCtx->FsClusterSize, pageSize);
		if (NULL == pIoOp) {
			_ftprintf(stderr, _T("Failed to allocate io op while dispatching file reads.\n"));
			ExitProcess(EXIT_FAILURE);
		}

		pIoOp->Ovlp.Offset = flOffset.LowPart;
		pIoOp->Ovlp.OffsetHigh = flOffset.HighPart;
		bytesToRead = IoCbCtx->FileSize.QuadPart - flOffset.QuadPart;
		if (bytesToRead > IoCbCtx->FsClusterSize)
			bytesToRead = IoCbCtx->FsClusterSize;
		StartThreadpoolIo(IoTp);
		ret = ReadFile(IoCbCtx->FileHandle, pIoOp->ReadBuf, (DWORD)bytesToRead, NULL, &pIoOp->Ovlp);
		if ((FALSE == ret) && (ERROR_IO_PENDING != (lastErr = GetLastError()))) {
			_ftprintf(stderr, _T("Error %#llx from ReadFile call.\n"), (long long)GetLastError());
			ExitProcess(EXIT_FAILURE);

		}
		flOffset.QuadPart += bytesToRead;
	}
	return ERROR_SUCCESS;
}


static DWORD SetSparseRange(
	_Inout_     PIO_CB_SHARED       IoCbCtx,
	_In_        PTP_IO              IoTp,
	_In_        LONGLONG            FileOffset,
	_In_        LONGLONG            BeyondFinalZero
	)
{
	FILE_ZERO_DATA_INFORMATION  fzdi;
	DWORD                       errRet, tmp;
	PIO_OP                      pIoOp;

	pIoOp = AllocIoOp(IO_SET_ZERO_RANGE, 0, 0);
	if (NULL == pIoOp)
		return ERROR_NOT_ENOUGH_MEMORY;
	fzdi.FileOffset.QuadPart = FileOffset;
	fzdi.BeyondFinalZero.QuadPart = BeyondFinalZero;
	WaitAvailableIo(IoCbCtx);
	StartThreadpoolIo(IoTp);
	if (!DeviceIoControl(IoCbCtx->FileHandle, FSCTL_SET_ZERO_DATA, &fzdi,
			sizeof(fzdi), NULL, 0, &tmp, &pIoOp->Ovlp)) {
		errRet = GetLastError();
	} else {
		errRet = ERROR_SUCCESS;
	}
	return errRet;
}


/* TODO: Definitively determine if this should send a zero ioctl for every
 * empty cluster or only for larger cluster groups. Also need to see if cluster
 * groups should be aligned. */
static DWORD SetSparseRanges(
	_Inout_     PIO_CB_SHARED       IoCbCtx,
	_In_        PTP_IO              IoTp,
	_In_        DWORD               MinClusterGroup
	)
{
	SIZE_T                      fsClusterSize;
	UINT64                      i, numClusters, runtBytes;
	INT64                       firstClusterInSequence;
	DWORD                       errRet;

	fsClusterSize = IoCbCtx->FsClusterSize;
	// numClusters may be off by one if filesize not a cluster size multiple.
	// This is handled after loop.
	numClusters = IoCbCtx->FileSize.QuadPart / IoCbCtx->FsClusterSize;
	firstClusterInSequence = -1;

	for (i = 0; i < numClusters; ++i) {
		if (IsMarkedZero(IoCbCtx->ZeroClusterMap, i)) {
			if (firstClusterInSequence < 0)
				firstClusterInSequence = (INT64)i;
		} else {
			if ((firstClusterInSequence >= 0)
				&& (MinClusterGroup <= (i - firstClusterInSequence))) {
				errRet = SetSparseRange(IoCbCtx, IoTp, firstClusterInSequence * fsClusterSize,
					i * fsClusterSize);
				if (!((errRet == ERROR_IO_PENDING) || (errRet == ERROR_SUCCESS))) {
					_ftprintf(stderr, _T("Error %#llx returned from SetSparseRange call.\n"), (long long)errRet);
					ExitProcess(EXIT_FAILURE);

				}
			}
			firstClusterInSequence = -1;
		}
	}

	runtBytes = IoCbCtx->FileSize.QuadPart % IoCbCtx->FsClusterSize;
	if (runtBytes) {
		if (!IsMarkedZero(IoCbCtx->ZeroClusterMap, i)) {
			runtBytes = 0;
		}
		// Don't bother zeroing a runt by itself.
	}
	if ((firstClusterInSequence >= 0)
		&& (MinClusterGroup <= (i - firstClusterInSequence))) {
		errRet = SetSparseRange(IoCbCtx, IoTp, firstClusterInSequence * fsClusterSize,
			(i * fsClusterSize) + runtBytes);
		if (!((errRet == ERROR_IO_PENDING) || (errRet == ERROR_SUCCESS))) {
			_ftprintf(stderr, _T("Error %#llx returned from SetSparseRange call.\n"), (long long)errRet);
			ExitProcess(EXIT_FAILURE);
		}
	}
	return ERROR_SUCCESS;
}


static DWORD SetSparseAttribute(
	_In_        PIO_CB_SHARED       IoCbCtx,
	_In_        PTP_IO              IoTp
	)
{
	DWORD                   errRet, tmp;
	PIO_OP                  pIoOp;

	pIoOp = AllocIoOp(IO_SET_SPARSE, 0, 0);
	if (NULL == pIoOp)
		return ERROR_NOT_ENOUGH_MEMORY;

	/* Set the sparse attribute for the file */
	pIoOp->SetSparseBuf.SetSparse = TRUE;
	StartThreadpoolIo(IoTp);
	if (!DeviceIoControl(IoCbCtx->FileHandle,
			FSCTL_SET_SPARSE,
			&pIoOp->SetSparseBuf,
			sizeof(pIoOp->SetSparseBuf),
			NULL,
			0,
			&tmp,
			&pIoOp->Ovlp)) {
		errRet = GetLastError();
	} else {
		errRet = ERROR_SUCCESS;
	}
	return errRet;
}


// If NULL is returned caller may use GetLastError to find out what happened.
_Success_(return != NULL)
static HANDLE OpenFileOverlappedExclusive(
	_In_        LPCTSTR         FlName,
	_Out_       PLARGE_INTEGER  pFileSize,
	_Out_       PSIZE_T         pFsClusterSize,
	_Out_opt_   LPFILETIME      pCreationTime,
	_Out_opt_   LPFILETIME      pLastAccessTime,
	_Out_opt_   LPFILETIME      pLastWriteTime
	)
{
	HANDLE      fl;
	SSIZE_T     fsClusterSize;
	DWORD       err;

	fl = CreateFile(FlName,             // user supplied filename
		GENERIC_READ | GENERIC_WRITE,   // read/write
		0,                              // do not share
		NULL,                           // default security
		OPEN_EXISTING,                  // creation disp
		FILE_FLAG_OVERLAPPED,           // use async io
		NULL);                          // No template

	if (INVALID_HANDLE_VALUE == fl) {
		return (NULL);
	}

	if ((fsClusterSize = GetDriveClusterSize(fl)) < 1) {
		err = GetLastError();
		CloseHandle(fl);
		SetLastError(err);
		return NULL;
	}

	if (FALSE == GetFileTime(fl, pCreationTime, pLastAccessTime, pLastWriteTime)) {
		err = GetLastError();
		CloseHandle(fl);
		SetLastError(err);
		return NULL;
	}

	if (FALSE == GetFileSizeEx(fl, pFileSize)) {
		err = GetLastError();
		CloseHandle(fl);
		SetLastError(err);
		return NULL;
	}

	*pFsClusterSize = (SIZE_T)fsClusterSize;

	return fl;
}


int _tmain(
	int         argc,
	_TCHAR      **argv
	)
{
	int                         idx;
	SIZE_T                      fsClusterSize;
	HANDLE                      fl;
	BOOL                        prsvTm;
	FILETIME                    tmCrt;
	FILETIME                    tmAcc;
	FILETIME                    tmWrt;
	LARGE_INTEGER               flSz;
	PTP_IO                      pTpIo;
	IO_CB_SHARED                ioCbCtx;
	DWORD                       errRet;
	volatile PLONG              zeroClusterMap;

	idx = 1;
	prsvTm = FALSE;
	if (argc == 3 && !_tcscmp(argv[1], _T("-p"))) {
		idx = 2;
		prsvTm = TRUE;
	} else if (argc != 2) {
		PrintUsageInfo();
		return (EXIT_FAILURE);
	}

	fl = OpenFileOverlappedExclusive(argv[idx], &flSz, &fsClusterSize, &tmCrt, &tmAcc, &tmWrt);
	if (NULL == fl) {
		_tprintf(_T("Failed to open file %s with error %d\n"), argv[1], GetLastError());
		return (EXIT_FAILURE);
	}

	_tprintf(_T("Cluster size: %ld\n"), (LONG)fsClusterSize);

#ifdef _M_IX86
	if (((((flSz.QuadPart / fsClusterSize) / 32) + 1) * sizeof(LONG)) >= INT32_MAX) {
		_tprintf(_T("Insufficient addressable address space for file map. Use 64-bit build."));
		ExitProcess(EXIT_FAILURE);
	}
#endif
	zeroClusterMap = calloc(1, (SIZE_T)((((flSz.QuadPart / fsClusterSize) / 32) + 1) * sizeof(LONG)));
	if (NULL == zeroClusterMap) {
		CloseHandle(fl);
		_tprintf(_T("Failed to allocate memory for cluster map\n"));
		ExitProcess(EXIT_FAILURE);
	}

	ioCbCtx.FileHandle = fl;
	ioCbCtx.FileSize = flSz;
	ioCbCtx.FsClusterSize = fsClusterSize;
	ioCbCtx.PendingIo = 0;
	ioCbCtx.ZeroClusterMap = zeroClusterMap;
	ioCbCtx.IoAvailEvt = CreateEvent(NULL, TRUE, TRUE, NULL);
	if (NULL == ioCbCtx.IoAvailEvt) {
		_ftprintf(stderr, _T("Error %#llx from CreateEvent call.\n"),
			(long long)GetLastError());
		ExitProcess(EXIT_FAILURE);
	}
	pTpIo = CreateThreadpoolIo(fl, ProcessCompletedIoCallback, &ioCbCtx, NULL);
	if (NULL == pTpIo) {
		_ftprintf(stderr, _T("Error %#llx from CreateThreadpoolIo call.\n"),
			(long long)GetLastError());
		ExitProcess(EXIT_FAILURE);
	}

	_tprintf(_T("Starting file analysis."));
	errRet = DispatchFileReads(&ioCbCtx, pTpIo);
	if (ERROR_SUCCESS != errRet) {
		_ftprintf(stderr, _T("Error %#llx from DispatchFileReads call.\n"),
			(long long)errRet);
		ExitProcess(EXIT_FAILURE);
	}
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);

	// For Debug
	//PrintClusterMap(zeroClusterMap, fsClusterSize, flSz.QuadPart);

	_tprintf(_T("Completed file analysis. Starting to dispatch zero ranges to file system.\n"));

	// TODO: Don't blindly set this if no zero clusters detected.
	errRet = SetSparseAttribute(&ioCbCtx, pTpIo);
	if (ERROR_SUCCESS != errRet) {
		_ftprintf(stderr, _T("Error %#llx from SetSparseAttribute call.\n"),
			(long long)errRet);
		ExitProcess(EXIT_FAILURE);
	}
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);

	errRet = SetSparseRanges(&ioCbCtx, pTpIo, 1);
	if (ERROR_SUCCESS != errRet) {
		_ftprintf(stderr, _T("Error %#llx from SetSparseRanges call.\n"),
			(long long)errRet);
		ExitProcess(EXIT_FAILURE);
	}
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);

	/* Reset modified and access timestamps if preserve filetimes specified */
	if (TRUE == prsvTm) {
		if (0 == SetFileTime(fl, NULL, &tmAcc, &tmWrt)) {
			_ftprintf(stderr, _T("WARNING: Failed to preserve file times on file.\n"));
		}
	}

	/* Clean up */
	WaitForThreadpoolIoCallbacks(pTpIo, FALSE);
	CloseThreadpoolIo(pTpIo);

	/* Flush buffers on file */
	FlushFileBuffers(fl);

	CloseHandle(fl);

	_tprintf(_T("Completed processing file\n"));

	return EXIT_SUCCESS;
}
