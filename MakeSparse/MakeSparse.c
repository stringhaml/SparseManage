// MakeSparse.c : Defines the entry point for the console application.
//

#include <windows.h>
#include <tchar.h>

#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>

/* TODO: For windows 8 / server 2012 use GetFileInformationByHandleEx function
 * to query OS about the file sector size and alignment rather then assuming 4K
 * sectors that are zero aligned. (FILE_STORAGE_INFO struct)*/

#define BUFFER_READ_SIZE        (1024 * 1024)
//#define FS_ALLOC_SIZE           (4  * 1024)  // Will change to read from FS later
#define MAX_CONC_IOCTLS         (16)

#if MAX_CONC_IOCTLS > MAXIMUM_WAIT_OBJECTS
#error Bad size to punch ratio
#endif

static void PrintUsageInfo();
static int GetDriveClusterSize(HANDLE hFl);

int _tmain(int argc, _TCHAR **argv)
{
	int                         i;
	int                         idx;
	int                         doLoop;
	int                         fsClusterSize;
	HANDLE                      fl;
	HANDLE                      hTmr;
	BOOL                        prsvTm;
	FILETIME                    tmCrt;
	FILETIME                    tmAcc;
	FILETIME                    tmWrt;
	FILE_SET_SPARSE_BUFFER      setSparseBuf;
	FILE_ZERO_DATA_INFORMATION  zeroData[MAX_CONC_IOCTLS];
	LARGE_INTEGER               flSz;
	LARGE_INTEGER               zeroOfstStrt;
	LARGE_INTEGER               zeroOfstEnd;
	LARGE_INTEGER               statsFreq;
	ULARGE_INTEGER              tmpOfst;
	int64_t                     numZeroByts;
	uint64_t                    curOfst;
	DWORD                       bytsRd;
	uint64_t                    bytsRmn;
	DWORD                       ioctlBytsRd;
	DWORD                       waitRet;
	OVERLAPPED                  readOvrlp;
	OVERLAPPED                  ioctlOvrlps[MAX_CONC_IOCTLS];
	HANDLE                      ioctlOvrlpEvnts[MAX_CONC_IOCTLS];
	char                        *curReadBuf;
	char                        *nextReadBuf;
	char                        *preAllocBuf;

	idx = 1;
	prsvTm = FALSE;
	if (argc == 3 && !_tcscmp(argv[1], _T("-p"))) {
		idx = 2;
		prsvTm = TRUE;
	} else if (argc != 2) {
		PrintUsageInfo();
		return (EXIT_FAILURE);
	}

	fl = CreateFile(argv[idx],                    // user supplied filename
	                GENERIC_READ | GENERIC_WRITE, // read/write
	                0,                            // do not share
	                NULL,                         // default security
	                OPEN_EXISTING,                // creation disp
	                FILE_FLAG_OVERLAPPED,         // use async io
	                NULL);                        // No template

	if (INVALID_HANDLE_VALUE == fl) {
		_tprintf(_T("Failed to open file %s with error %d\n"), argv[1], GetLastError());
		return (EXIT_FAILURE);
	}

	/* Read current file time for later restore */
	if (0 == GetFileTime(fl, &tmCrt, &tmAcc, &tmWrt)) {
		_tprintf(_T("Failed to read file times.\n"));
		CloseHandle(fl);
		return (EXIT_FAILURE);
	}

	fsClusterSize = GetDriveClusterSize(fl);
	if (fsClusterSize <= 0) {
		//TODO: Handle nicely.
		abort();
	}

	_tprintf(_T("Cluster size: %d\n"), fsClusterSize);

	/* Initialize overlap structures and events */
	memset(&ioctlOvrlps, 0, sizeof(ioctlOvrlps));
	memset(&readOvrlp, 0, sizeof(readOvrlp));
	for (i = 0; i < MAX_CONC_IOCTLS; ++i) {
		ioctlOvrlpEvnts[i] = CreateEvent(NULL, TRUE, TRUE, NULL);
		if (NULL == ioctlOvrlpEvnts[i]) {
			_tprintf(_T("Failed to create events.\n"));
			return (EXIT_FAILURE);
		}
		ioctlOvrlps[i].hEvent = ioctlOvrlpEvnts[i];
	}
	readOvrlp.hEvent = CreateEvent(NULL, TRUE, TRUE, NULL);
	if (NULL == readOvrlp.hEvent) {
		_tprintf(_T("Failed to create events.\n"));
		return (EXIT_FAILURE);
	}

	/* Set the sparse attribute for the file */
	setSparseBuf.SetSparse = TRUE;
	if (0 == DeviceIoControl(fl,
	                         FSCTL_SET_SPARSE,
	                         &setSparseBuf,
	                         sizeof(setSparseBuf),
	                         NULL,
	                         0,
	                         &ioctlBytsRd,
	                         &ioctlOvrlps[0])
	    && GetLastError() != ERROR_IO_PENDING) {
		_tprintf(_T("Failed to set sparse attribute\n"));
		return (EXIT_FAILURE);
	}
	/* Wait for ioctl to complete */
	if (0 == GetOverlappedResult(fl, &ioctlOvrlps[0], &ioctlBytsRd, TRUE)) {
		_tprintf(_T("Failed sparse ioctl with error %d\n"), GetLastError());
		return (EXIT_FAILURE);
	}

	/* Get file size */
	if (0 == GetFileSizeEx(fl, &flSz)) {
		abort();
	}

	/* create and set waitable timer for stats output */
	statsFreq.QuadPart = -100000000ll; /* 10 seconds */
	hTmr = CreateWaitableTimer(NULL, TRUE, NULL);
	SetWaitableTimer(hTmr, &statsFreq, 0, NULL, NULL, FALSE);

	/* Initialize loop variables */
	numZeroByts = 0;
	zeroOfstStrt.QuadPart = 0;
	curOfst = 0;
	curReadBuf = NULL;
	bytsRmn = flSz.QuadPart;

	/* Setup read buffer and start initial read */
	if (NULL == (nextReadBuf = malloc(BUFFER_READ_SIZE))) {
		abort();
	}
	if (0 == ReadFile(fl, nextReadBuf, BUFFER_READ_SIZE, NULL, &readOvrlp)
	    && GetLastError() != ERROR_IO_PENDING) {
		abort();
	}

	doLoop = 1;
	while (doLoop) {
		/* Pre allocate next buffer while waiting for IO to complete */
		if (NULL == (preAllocBuf = malloc(BUFFER_READ_SIZE))) {
			//TODO: handle gracefully.
			abort();
		}

		/* Wait for pending read to complete */
		if (0 == GetOverlappedResult(fl, &readOvrlp, &bytsRd, TRUE)) {
			//TODO: handle gracefully.
			abort();
		}

		/* setup for next read */
		curReadBuf = nextReadBuf;
		if (bytsRd != bytsRmn) {
			nextReadBuf = preAllocBuf;
			/* TODO: Fix this to detect EOF! */
			/* start next read */
			tmpOfst.QuadPart = curOfst + bytsRd;
			readOvrlp.Offset = tmpOfst.LowPart;
			readOvrlp.OffsetHigh = tmpOfst.HighPart;
			if (0 == ReadFile(fl,
			                  nextReadBuf,
			                  BUFFER_READ_SIZE,
			                  NULL,
			                  &readOvrlp)
			    && GetLastError() != ERROR_IO_PENDING) {
				//TODO: handle gracefully
				abort();
			}
		} else {
			/* End of file reached, free pre-allocated buffer */
			free(preAllocBuf);
			doLoop = 0;
		}

		/* Check for any errors in last ioctl zero cycle */
		for (i = 0; i < MAX_CONC_IOCTLS; ++i) {
			if ((0 == GetOverlappedResult(fl,
			                              &ioctlOvrlps[i],
			                              &ioctlBytsRd,
			                              FALSE))
			    && (GetLastError() != ERROR_IO_PENDING)) {
				_tprintf(_T("zeroing ioctl failed with error %lu\n"),
				         GetLastError());
				// TODO: cleanup.
				return (EXIT_FAILURE);
			}
		}

		/* Analyze results of last read */
		for (i = 0; i < bytsRd; ++i) {
			if (*(curReadBuf + i) == '\0') {
				/* Reset zero start offset if num zero bytes reset */
				if (0 == numZeroByts) {
					zeroOfstStrt.QuadPart = curOfst + i;
				}
				numZeroByts++;

				/* Skip the rest of the loop unless we are at EOF */
				if ((i + 1) != bytsRmn) {
					continue;
				}
			} else if (numZeroByts < fsClusterSize) {
				/* Calculate next place in buffer to start analysis if
				 * numZeroByts less then the cluster size. -1 to account for
				 * loop increment. */
				i = (((i / fsClusterSize) + 1) * fsClusterSize) - 1;
				numZeroByts = 0;
				continue;
			}

			/* zero byte check failed or we are at EOF */
			/* Calculate aligned offset */
			zeroOfstEnd.QuadPart = zeroOfstStrt.QuadPart + numZeroByts;
			zeroOfstEnd.QuadPart -= zeroOfstEnd.QuadPart % fsClusterSize;
			zeroOfstStrt.QuadPart += (fsClusterSize - (zeroOfstStrt.QuadPart % fsClusterSize)) % fsClusterSize;

			/* see if we should de-allocate block */
			if ((zeroOfstEnd.QuadPart - zeroOfstStrt.QuadPart) >= fsClusterSize) {
				/* Wait for first available ioctl overlapped struct */
				waitRet = WaitForMultipleObjects(MAX_CONC_IOCTLS,
				                                 ioctlOvrlpEvnts,
				                                 FALSE,
				                                 INFINITE);
				if (waitRet < WAIT_OBJECT_0
				    || waitRet >= (WAIT_OBJECT_0 + MAX_CONC_IOCTLS)) {
					/* Wait failure of some sort */
					// TODO: Handle gracefully
					abort();
				}
				idx = waitRet - WAIT_OBJECT_0;
				/* Make sure the previous ioctl completed successfully */
				if (0 == GetOverlappedResult(fl,
				                             &ioctlOvrlps[idx],
				                             &ioctlBytsRd,
				                             FALSE)) {
					//TODO: handle gracefully
					_tprintf(_T("ioctl failed with error %d"), GetLastError());
					abort();
				}

				/* setup new ioctl */
				zeroData[idx].FileOffset.QuadPart = zeroOfstStrt.QuadPart;
				zeroData[idx].BeyondFinalZero.QuadPart = zeroOfstEnd.QuadPart;
				if (0 == DeviceIoControl(fl,
				                         FSCTL_SET_ZERO_DATA,
				                         &zeroData[idx],
				                         sizeof(zeroData[0]),
				                         NULL,
				                         0,
				                         &ioctlBytsRd,
				                         &ioctlOvrlps[idx])
				    && GetLastError() != ERROR_IO_PENDING) {
					//TODO: Handle gracefully.
					_tprintf(_T("Failed zero range ioctl\n"));
					abort();
				}
			}
			/* Reset zero byte counter */
			numZeroByts = 0;
		} /* end of zero analysis loop */

		/* Done with current buffer */
		free(curReadBuf);

		/* Update current offsets */
		curOfst += bytsRd;
		bytsRmn -= bytsRd;

		/* Output current statistics on timed interval */
		if (WAIT_OBJECT_0 == (waitRet = WaitForSingleObject(hTmr, 0))) {
			_tprintf(_T("processed: %8llu MiB of %lld MiB\n"),
			         curOfst >> 20, flSz.QuadPart >> 20);
			SetWaitableTimer(hTmr, &statsFreq, 0, NULL, NULL, FALSE);
		} else if (waitRet != WAIT_TIMEOUT) {
			//TODO: handle gracefully
			_tprintf(_T("stats timer wait fail\n"));
			//abort();
		}

	} /* end of while (doLoop) */

	/* Wait for pending ioctls to complete and check for errors */
	for (i = 0; i < MAX_CONC_IOCTLS; ++i) {
		if (0 == GetOverlappedResult(fl,
		                             &ioctlOvrlps[idx],
		                             &ioctlBytsRd,
		                             TRUE)) {
			_tprintf(_T("zeroing ioctl failed with error %lu\n"),
			         GetLastError());
			// TODO: cleanup.
			return (EXIT_FAILURE);
		}
	}

	/* Reset modified and access timestamps if preserve filetimes specified */
	if (TRUE == prsvTm) {
		if (0 == SetFileTime(fl, NULL, &tmAcc, &tmWrt)) {
			_tprintf(_T("WARNING: Failed to preserve file times on file."));
		}
	}

	/* Flush buffers on file */
	FlushFileBuffers(fl);

	/* Clean up */
	CloseHandle(fl);
	CloseHandle(hTmr);
	for (i = 0; i < MAX_CONC_IOCTLS; ++i) {
		CloseHandle(ioctlOvrlpEvnts[i]);
	}

	_tprintf(_T("Completed processing file\n")); 

	return 0;
}


static void PrintUsageInfo()
{
	_tprintf(_T("Someday I should get around to telling you how to use this program\n"));

	return;
}


/* Determine the cluster size of the file system that the handle resides on. */
static int GetDriveClusterSize(HANDLE hFl)
{
	DWORD   sectorsPerCluster;
	DWORD   bytesPerSector;
	DWORD   numberOfFreeClusters;
	DWORD   totalNumberOfClusters;
	DWORD   flNameLen;
	DWORD   tmp;
	int     clusterSize;
	int     delimCnt;
	TCHAR   *ofst;
	TCHAR   *flName;

	/* Determine buffer size to allocate. */
	flNameLen = GetFinalPathNameByHandle(hFl, NULL, 0, VOLUME_NAME_GUID);
	if (0 == flNameLen) {
		/* TODO: Make this nice */
		abort();
	}

	flName = calloc(1, sizeof(TCHAR) * (flNameLen + 1));

	tmp = GetFinalPathNameByHandle(hFl, flName, flNameLen + 1, VOLUME_NAME_GUID);
	if (0 == tmp || flNameLen < tmp) {
		abort();
	}

	// DEBUG
	//_tprintf(_T("Filename: %s\n"), flName);

	ofst = flName;
	delimCnt = 0;
	while (delimCnt < 4 && *ofst != _T('\0')) {
		if (*ofst == _T('\\')) delimCnt++;
		ofst = CharNext(ofst);
	}

	if (delimCnt != 4) {
		//TODO: Handle nicely.
		abort();
	}

	if ((flName + flNameLen) - ofst <= 0) {
		//TODO: Handle nicely.
		abort();
	}

	*ofst = _T('\0'); /* Terminate the string after last delimiter. */
	//_tprintf(_T("Filename: %s\n"), flName);

	if (0 == GetDiskFreeSpace(flName, &sectorsPerCluster, &bytesPerSector, &numberOfFreeClusters, &totalNumberOfClusters)) {
		//TODO: Handle nicely.
		abort();
	}

	free(flName);

	clusterSize = bytesPerSector * sectorsPerCluster;

	return (clusterSize);
}
