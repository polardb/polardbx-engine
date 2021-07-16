/*
 * Copyright (c) Przemyslaw Skibinski, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */

#if defined (__cplusplus)
extern "C" {
#endif


/*-****************************************
*  Dependencies
******************************************/
#include "util.h"       /* note : ensure that platform.h is included first ! */
#include <stdlib.h>     /* malloc, realloc, free */
#include <stdio.h>      /* fprintf */
#include <time.h>       /* clock_t, clock, CLOCKS_PER_SEC, nanosleep */
#include <errno.h>
#include <assert.h>

#if defined(_WIN32)
#  include <sys/utime.h>  /* utime */
#  include <io.h>         /* _chmod */
#else
#  include <unistd.h>     /* chown, stat */
#  if PLATFORM_POSIX_VERSION < 200809L || !defined(st_mtime)
#    include <utime.h>    /* utime */
#  else
#    include <fcntl.h>    /* AT_FDCWD */
#    include <sys/stat.h> /* utimensat */
#  endif
#endif

#if defined(_MSC_VER) || defined(__MINGW32__) || defined (__MSVCRT__)
#include <direct.h>     /* needed for _mkdir in windows */
#endif

#if defined(__linux__) || (PLATFORM_POSIX_VERSION >= 200112L)  /* opendir, readdir require POSIX.1-2001 */
#  include <dirent.h>       /* opendir, readdir */
#  include <string.h>       /* strerror, memcpy */
#endif /* #ifdef _WIN32 */

/*-****************************************
*  Internal Macros
******************************************/

/* CONTROL is almost like an assert(), but is never disabled.
 * It's designed for failures that may happen rarely,
 * but we don't want to maintain a specific error code path for them,
 * such as a malloc() returning NULL for example.
 * Since it's always active, this macro can trigger side effects.
 */
#define CONTROL(c)  {         \
    if (!(c)) {               \
        UTIL_DISPLAYLEVEL(1, "Error : %s, %i : %s",  \
                          __FILE__, __LINE__, #c);   \
        exit(1);              \
}   }

/* console log */
#define UTIL_DISPLAY(...)         fprintf(stderr, __VA_ARGS__)
#define UTIL_DISPLAYLEVEL(l, ...) { if (g_utilDisplayLevel>=l) { UTIL_DISPLAY(__VA_ARGS__); } }

/* A modified version of realloc().
 * If UTIL_realloc() fails the original block is freed.
 */
UTIL_STATIC void* UTIL_realloc(void *ptr, size_t size)
{
    void *newptr = realloc(ptr, size);
    if (newptr) return newptr;
    free(ptr);
    return NULL;
}

#if defined(_MSC_VER)
    #define chmod _chmod
#endif


/*-****************************************
*  Console log
******************************************/
int g_utilDisplayLevel;

int UTIL_requireUserConfirmation(const char* prompt, const char* abortMsg,
                                 const char* acceptableLetters, int hasStdinInput) {
    int ch, result;

    if (hasStdinInput) {
        UTIL_DISPLAY("stdin is an input - not proceeding.\n");
        return 1;
    }

    UTIL_DISPLAY("%s", prompt);
    ch = getchar();
    result = 0;
    if (strchr(acceptableLetters, ch) == NULL) {
        UTIL_DISPLAY("%s", abortMsg);
        result = 1;
    }
    /* flush the rest */
    while ((ch!=EOF) && (ch!='\n'))
        ch = getchar();
    return result;
}


/*-*************************************
*  Constants
***************************************/
#define LIST_SIZE_INCREASE   (8*1024)
#define MAX_FILE_OF_FILE_NAMES_SIZE (1<<20)*50


/*-*************************************
*  Functions
***************************************/

int UTIL_stat(const char* filename, stat_t* statbuf)
{
#if defined(_MSC_VER)
    return !_stat64(filename, statbuf);
#elif defined(__MINGW32__) && defined (__MSVCRT__)
    return !_stati64(filename, statbuf);
#else
    return !stat(filename, statbuf);
#endif
}

int UTIL_isRegularFile(const char* infilename)
{
    stat_t statbuf;
    return UTIL_stat(infilename, &statbuf) && UTIL_isRegularFileStat(&statbuf);
}

int UTIL_isRegularFileStat(const stat_t* statbuf)
{
#if defined(_MSC_VER)
    return (statbuf->st_mode & S_IFREG) != 0;
#else
    return S_ISREG(statbuf->st_mode) != 0;
#endif
}

/* like chmod, but avoid changing permission of /dev/null */
int UTIL_chmod(char const* filename, const stat_t* statbuf, mode_t permissions)
{
    stat_t localStatBuf;
    if (statbuf == NULL) {
        if (!UTIL_stat(filename, &localStatBuf)) return 0;
        statbuf = &localStatBuf;
    }
    if (!UTIL_isRegularFileStat(statbuf)) return 0; /* pretend success, but don't change anything */
    return chmod(filename, permissions);
}

int UTIL_setFileStat(const char *filename, const stat_t *statbuf)
{
    int res = 0;

    stat_t curStatBuf;
    if (!UTIL_stat(filename, &curStatBuf) || !UTIL_isRegularFileStat(&curStatBuf))
        return -1;

    /* set access and modification times */
    /* We check that st_mtime is a macro here in order to give us confidence
     * that struct stat has a struct timespec st_mtim member. We need this
     * check because there are some platforms that claim to be POSIX 2008
     * compliant but which do not have st_mtim... */
#if (PLATFORM_POSIX_VERSION >= 200809L) && defined(st_mtime)
    {
        /* (atime, mtime) */
        struct timespec timebuf[2] = { {0, UTIME_NOW} };
        timebuf[1] = statbuf->st_mtim;
        res += utimensat(AT_FDCWD, filename, timebuf, 0);
    }
#else
    {
        struct utimbuf timebuf;
        timebuf.actime = time(NULL);
        timebuf.modtime = statbuf->st_mtime;
        res += utime(filename, &timebuf);
    }
#endif

#if !defined(_WIN32)
    res += chown(filename, statbuf->st_uid, statbuf->st_gid);  /* Copy ownership */
#endif

    res += UTIL_chmod(filename, &curStatBuf, statbuf->st_mode & 07777);  /* Copy file permissions */

    errno = 0;
    return -res; /* number of errors is returned */
}

int UTIL_isDirectory(const char* infilename)
{
    stat_t statbuf;
    return UTIL_stat(infilename, &statbuf) && UTIL_isDirectoryStat(&statbuf);
}

int UTIL_isDirectoryStat(const stat_t* statbuf)
{
#if defined(_MSC_VER)
    return (statbuf->st_mode & _S_IFDIR) != 0;
#else
    return S_ISDIR(statbuf->st_mode) != 0;
#endif
}

int UTIL_compareStr(const void *p1, const void *p2) {
    return strcmp(* (char * const *) p1, * (char * const *) p2);
}

int UTIL_isSameFile(const char* fName1, const char* fName2)
{
    assert(fName1 != NULL); assert(fName2 != NULL);
#if defined(_MSC_VER) || defined(_WIN32)
    /* note : Visual does not support file identification by inode.
     *        inode does not work on Windows, even with a posix layer, like msys2.
     *        The following work-around is limited to detecting exact name repetition only,
     *        aka `filename` is considered different from `subdir/../filename` */
    return !strcmp(fName1, fName2);
#else
    {   stat_t file1Stat;
        stat_t file2Stat;
        return UTIL_stat(fName1, &file1Stat)
            && UTIL_stat(fName2, &file2Stat)
            && (file1Stat.st_dev == file2Stat.st_dev)
            && (file1Stat.st_ino == file2Stat.st_ino);
    }
#endif
}

/* UTIL_isFIFO : distinguish named pipes */
int UTIL_isFIFO(const char* infilename)
{
/* macro guards, as defined in : https://linux.die.net/man/2/lstat */
#if PLATFORM_POSIX_VERSION >= 200112L
    stat_t statbuf;
    if (UTIL_stat(infilename, &statbuf) && UTIL_isFIFOStat(&statbuf)) return 1;
#endif
    (void)infilename;
    return 0;
}

/* UTIL_isFIFO : distinguish named pipes */
int UTIL_isFIFOStat(const stat_t* statbuf)
{
/* macro guards, as defined in : https://linux.die.net/man/2/lstat */
#if PLATFORM_POSIX_VERSION >= 200112L
    if (S_ISFIFO(statbuf->st_mode)) return 1;
#endif
    (void)statbuf;
    return 0;
}

/* UTIL_isBlockDevStat : distinguish named pipes */
int UTIL_isBlockDevStat(const stat_t* statbuf)
{
/* macro guards, as defined in : https://linux.die.net/man/2/lstat */
#if PLATFORM_POSIX_VERSION >= 200112L
    if (S_ISBLK(statbuf->st_mode)) return 1;
#endif
    (void)statbuf;
    return 0;
}

int UTIL_isLink(const char* infilename)
{
/* macro guards, as defined in : https://linux.die.net/man/2/lstat */
#if PLATFORM_POSIX_VERSION >= 200112L
    stat_t statbuf;
    int const r = lstat(infilename, &statbuf);
    if (!r && S_ISLNK(statbuf.st_mode)) return 1;
#endif
    (void)infilename;
    return 0;
}

U64 UTIL_getFileSize(const char* infilename)
{
    stat_t statbuf;
    if (!UTIL_stat(infilename, &statbuf)) return UTIL_FILESIZE_UNKNOWN;
    return UTIL_getFileSizeStat(&statbuf);
}

U64 UTIL_getFileSizeStat(const stat_t* statbuf)
{
    if (!UTIL_isRegularFileStat(statbuf)) return UTIL_FILESIZE_UNKNOWN;
#if defined(_MSC_VER)
    if (!(statbuf->st_mode & S_IFREG)) return UTIL_FILESIZE_UNKNOWN;
#elif defined(__MINGW32__) && defined (__MSVCRT__)
    if (!(statbuf->st_mode & S_IFREG)) return UTIL_FILESIZE_UNKNOWN;
#else
    if (!S_ISREG(statbuf->st_mode)) return UTIL_FILESIZE_UNKNOWN;
#endif
    return (U64)statbuf->st_size;
}


U64 UTIL_getTotalFileSize(const char* const * fileNamesTable, unsigned nbFiles)
{
    U64 total = 0;
    unsigned n;
    for (n=0; n<nbFiles; n++) {
        U64 const size = UTIL_getFileSize(fileNamesTable[n]);
        if (size == UTIL_FILESIZE_UNKNOWN) return UTIL_FILESIZE_UNKNOWN;
        total += size;
    }
    return total;
}


/* condition : @file must be valid, and not have reached its end.
 * @return : length of line written into @buf, ended with `\0` instead of '\n',
 *           or 0, if there is no new line */
static size_t readLineFromFile(char* buf, size_t len, FILE* file)
{
    assert(!feof(file));
    if ( fgets(buf, (int) len, file) == NULL ) return 0;
    {   size_t linelen = strlen(buf);
        if (strlen(buf)==0) return 0;
        if (buf[linelen-1] == '\n') linelen--;
        buf[linelen] = '\0';
        return linelen+1;
    }
}

/* Conditions :
 *   size of @inputFileName file must be < @dstCapacity
 *   @dst must be initialized
 * @return : nb of lines
 *       or -1 if there's an error
 */
static int
readLinesFromFile(void* dst, size_t dstCapacity,
            const char* inputFileName)
{
    int nbFiles = 0;
    size_t pos = 0;
    char* const buf = (char*)dst;
    FILE* const inputFile = fopen(inputFileName, "r");

    assert(dst != NULL);

    if(!inputFile) {
        if (g_utilDisplayLevel >= 1) perror("zstd:util:readLinesFromFile");
        return -1;
    }

    while ( !feof(inputFile) ) {
        size_t const lineLength = readLineFromFile(buf+pos, dstCapacity-pos, inputFile);
        if (lineLength == 0) break;
        assert(pos + lineLength < dstCapacity);
        pos += lineLength;
        ++nbFiles;
    }

    CONTROL( fclose(inputFile) == 0 );

    return nbFiles;
}

/*Note: buf is not freed in case function successfully created table because filesTable->fileNames[0] = buf*/
FileNamesTable*
UTIL_createFileNamesTable_fromFileName(const char* inputFileName)
{
    size_t nbFiles = 0;
    char* buf;
    size_t bufSize;
    size_t pos = 0;
    stat_t statbuf;

    if (!UTIL_stat(inputFileName, &statbuf) || !UTIL_isRegularFileStat(&statbuf))
        return NULL;

    {   U64 const inputFileSize = UTIL_getFileSizeStat(&statbuf);
        if(inputFileSize > MAX_FILE_OF_FILE_NAMES_SIZE)
            return NULL;
        bufSize = (size_t)(inputFileSize + 1); /* (+1) to add '\0' at the end of last filename */
    }

    buf = (char*) malloc(bufSize);
    CONTROL( buf != NULL );

    {   int const ret_nbFiles = readLinesFromFile(buf, bufSize, inputFileName);

        if (ret_nbFiles <= 0) {
          free(buf);
          return NULL;
        }
        nbFiles = (size_t)ret_nbFiles;
    }

    {   const char** filenamesTable = (const char**) malloc(nbFiles * sizeof(*filenamesTable));
        CONTROL(filenamesTable != NULL);

        {   size_t fnb;
            for (fnb = 0, pos = 0; fnb < nbFiles; fnb++) {
                filenamesTable[fnb] = buf+pos;
                pos += strlen(buf+pos)+1;  /* +1 for the finishing `\0` */
        }   }
        assert(pos <= bufSize);

        return UTIL_assembleFileNamesTable(filenamesTable, nbFiles, buf);
    }
}

static FileNamesTable*
UTIL_assembleFileNamesTable2(const char** filenames, size_t tableSize, size_t tableCapacity, char* buf)
{
    FileNamesTable* const table = (FileNamesTable*) malloc(sizeof(*table));
    CONTROL(table != NULL);
    table->fileNames = filenames;
    table->buf = buf;
    table->tableSize = tableSize;
    table->tableCapacity = tableCapacity;
    return table;
}

FileNamesTable*
UTIL_assembleFileNamesTable(const char** filenames, size_t tableSize, char* buf)
{
    return UTIL_assembleFileNamesTable2(filenames, tableSize, tableSize, buf);
}

void UTIL_freeFileNamesTable(FileNamesTable* table)
{
    if (table==NULL) return;
    free((void*)table->fileNames);
    free(table->buf);
    free(table);
}

FileNamesTable* UTIL_allocateFileNamesTable(size_t tableSize)
{
    const char** const fnTable = (const char**)malloc(tableSize * sizeof(*fnTable));
    FileNamesTable* fnt;
    if (fnTable==NULL) return NULL;
    fnt = UTIL_assembleFileNamesTable(fnTable, tableSize, NULL);
    fnt->tableSize = 0;   /* the table is empty */
    return fnt;
}

void UTIL_refFilename(FileNamesTable* fnt, const char* filename)
{
    assert(fnt->tableSize < fnt->tableCapacity);
    fnt->fileNames[fnt->tableSize] = filename;
    fnt->tableSize++;
}

static size_t getTotalTableSize(FileNamesTable* table)
{
    size_t fnb = 0, totalSize = 0;
    for(fnb = 0 ; fnb < table->tableSize && table->fileNames[fnb] ; ++fnb) {
        totalSize += strlen(table->fileNames[fnb]) + 1; /* +1 to add '\0' at the end of each fileName */
    }
    return totalSize;
}

FileNamesTable*
UTIL_mergeFileNamesTable(FileNamesTable* table1, FileNamesTable* table2)
{
    unsigned newTableIdx = 0;
    size_t pos = 0;
    size_t newTotalTableSize;
    char* buf;

    FileNamesTable* const newTable = UTIL_assembleFileNamesTable(NULL, 0, NULL);
    CONTROL( newTable != NULL );

    newTotalTableSize = getTotalTableSize(table1) + getTotalTableSize(table2);

    buf = (char*) calloc(newTotalTableSize, sizeof(*buf));
    CONTROL ( buf != NULL );

    newTable->buf = buf;
    newTable->tableSize = table1->tableSize + table2->tableSize;
    newTable->fileNames = (const char **) calloc(newTable->tableSize, sizeof(*(newTable->fileNames)));
    CONTROL ( newTable->fileNames != NULL );

    {   unsigned idx1;
        for( idx1=0 ; (idx1 < table1->tableSize) && table1->fileNames[idx1] && (pos < newTotalTableSize); ++idx1, ++newTableIdx) {
            size_t const curLen = strlen(table1->fileNames[idx1]);
            memcpy(buf+pos, table1->fileNames[idx1], curLen);
            assert(newTableIdx <= newTable->tableSize);
            newTable->fileNames[newTableIdx] = buf+pos;
            pos += curLen+1;
    }   }

    {   unsigned idx2;
        for( idx2=0 ; (idx2 < table2->tableSize) && table2->fileNames[idx2] && (pos < newTotalTableSize) ; ++idx2, ++newTableIdx) {
            size_t const curLen = strlen(table2->fileNames[idx2]);
            memcpy(buf+pos, table2->fileNames[idx2], curLen);
            assert(newTableIdx <= newTable->tableSize);
            newTable->fileNames[newTableIdx] = buf+pos;
            pos += curLen+1;
    }   }
    assert(pos <= newTotalTableSize);
    newTable->tableSize = newTableIdx;

    UTIL_freeFileNamesTable(table1);
    UTIL_freeFileNamesTable(table2);

    return newTable;
}

#ifdef _WIN32
static int UTIL_prepareFileList(const char* dirName,
                                char** bufStart, size_t* pos,
                                char** bufEnd, int followLinks)
{
    char* path;
    size_t dirLength, pathLength;
    int nbFiles = 0;
    WIN32_FIND_DATAA cFile;
    HANDLE hFile;

    dirLength = strlen(dirName);
    path = (char*) malloc(dirLength + 3);
    if (!path) return 0;

    memcpy(path, dirName, dirLength);
    path[dirLength] = '\\';
    path[dirLength+1] = '*';
    path[dirLength+2] = 0;

    hFile=FindFirstFileA(path, &cFile);
    if (hFile == INVALID_HANDLE_VALUE) {
        UTIL_DISPLAYLEVEL(1, "Cannot open directory '%s'\n", dirName);
        return 0;
    }
    free(path);

    do {
        size_t const fnameLength = strlen(cFile.cFileName);
        path = (char*) malloc(dirLength + fnameLength + 2);
        if (!path) { FindClose(hFile); return 0; }
        memcpy(path, dirName, dirLength);
        path[dirLength] = '\\';
        memcpy(path+dirLength+1, cFile.cFileName, fnameLength);
        pathLength = dirLength+1+fnameLength;
        path[pathLength] = 0;
        if (cFile.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            if ( strcmp (cFile.cFileName, "..") == 0
              || strcmp (cFile.cFileName, ".") == 0 )
                continue;
            /* Recursively call "UTIL_prepareFileList" with the new path. */
            nbFiles += UTIL_prepareFileList(path, bufStart, pos, bufEnd, followLinks);
            if (*bufStart == NULL) { free(path); FindClose(hFile); return 0; }
        } else if ( (cFile.dwFileAttributes & FILE_ATTRIBUTE_NORMAL)
                 || (cFile.dwFileAttributes & FILE_ATTRIBUTE_ARCHIVE)
                 || (cFile.dwFileAttributes & FILE_ATTRIBUTE_COMPRESSED) ) {
            if (*bufStart + *pos + pathLength >= *bufEnd) {
                ptrdiff_t const newListSize = (*bufEnd - *bufStart) + LIST_SIZE_INCREASE;
                *bufStart = (char*)UTIL_realloc(*bufStart, newListSize);
                if (*bufStart == NULL) { free(path); FindClose(hFile); return 0; }
                *bufEnd = *bufStart + newListSize;
            }
            if (*bufStart + *pos + pathLength < *bufEnd) {
                memcpy(*bufStart + *pos, path, pathLength+1 /* include final \0 */);
                *pos += pathLength + 1;
                nbFiles++;
        }   }
        free(path);
    } while (FindNextFileA(hFile, &cFile));

    FindClose(hFile);
    return nbFiles;
}

#elif defined(__linux__) || (PLATFORM_POSIX_VERSION >= 200112L)  /* opendir, readdir require POSIX.1-2001 */

static int UTIL_prepareFileList(const char *dirName,
                                char** bufStart, size_t* pos,
                                char** bufEnd, int followLinks)
{
    DIR* dir;
    struct dirent * entry;
    size_t dirLength;
    int nbFiles = 0;

    if (!(dir = opendir(dirName))) {
        UTIL_DISPLAYLEVEL(1, "Cannot open directory '%s': %s\n", dirName, strerror(errno));
        return 0;
    }

    dirLength = strlen(dirName);
    errno = 0;
    while ((entry = readdir(dir)) != NULL) {
        char* path;
        size_t fnameLength, pathLength;
        if (strcmp (entry->d_name, "..") == 0 ||
            strcmp (entry->d_name, ".") == 0) continue;
        fnameLength = strlen(entry->d_name);
        path = (char*) malloc(dirLength + fnameLength + 2);
        if (!path) { closedir(dir); return 0; }
        memcpy(path, dirName, dirLength);

        path[dirLength] = '/';
        memcpy(path+dirLength+1, entry->d_name, fnameLength);
        pathLength = dirLength+1+fnameLength;
        path[pathLength] = 0;

        if (!followLinks && UTIL_isLink(path)) {
            UTIL_DISPLAYLEVEL(2, "Warning : %s is a symbolic link, ignoring\n", path);
            free(path);
            continue;
        }

        if (UTIL_isDirectory(path)) {
            nbFiles += UTIL_prepareFileList(path, bufStart, pos, bufEnd, followLinks);  /* Recursively call "UTIL_prepareFileList" with the new path. */
            if (*bufStart == NULL) { free(path); closedir(dir); return 0; }
        } else {
            if (*bufStart + *pos + pathLength >= *bufEnd) {
                ptrdiff_t newListSize = (*bufEnd - *bufStart) + LIST_SIZE_INCREASE;
                assert(newListSize >= 0);
                *bufStart = (char*)UTIL_realloc(*bufStart, (size_t)newListSize);
                *bufEnd = *bufStart + newListSize;
                if (*bufStart == NULL) { free(path); closedir(dir); return 0; }
            }
            if (*bufStart + *pos + pathLength < *bufEnd) {
                memcpy(*bufStart + *pos, path, pathLength + 1);  /* with final \0 */
                *pos += pathLength + 1;
                nbFiles++;
        }   }
        free(path);
        errno = 0; /* clear errno after UTIL_isDirectory, UTIL_prepareFileList */
    }

    if (errno != 0) {
        UTIL_DISPLAYLEVEL(1, "readdir(%s) error: %s \n", dirName, strerror(errno));
        free(*bufStart);
        *bufStart = NULL;
    }
    closedir(dir);
    return nbFiles;
}

#else

static int UTIL_prepareFileList(const char *dirName,
                                char** bufStart, size_t* pos,
                                char** bufEnd, int followLinks)
{
    (void)bufStart; (void)bufEnd; (void)pos; (void)followLinks;
    UTIL_DISPLAYLEVEL(1, "Directory %s ignored (compiled without _WIN32 or _POSIX_C_SOURCE) \n", dirName);
    return 0;
}

#endif /* #ifdef _WIN32 */

int UTIL_isCompressedFile(const char *inputName, const char *extensionList[])
{
  const char* ext = UTIL_getFileExtension(inputName);
  while(*extensionList!=NULL)
  {
    const int isCompressedExtension = strcmp(ext,*extensionList);
    if(isCompressedExtension==0)
      return 1;
    ++extensionList;
  }
   return 0;
}

/*Utility function to get file extension from file */
const char* UTIL_getFileExtension(const char* infilename)
{
   const char* extension = strrchr(infilename, '.');
   if(!extension || extension==infilename) return "";
   return extension;
}

static int pathnameHas2Dots(const char *pathname)
{
    /* We need to figure out whether any ".." present in the path is a whole
     * path token, which is the case if it is bordered on both sides by either
     * the beginning/end of the path or by a directory separator.
     */
    const char *needle = pathname;
    while (1) {
        needle = strstr(needle, "..");

        if (needle == NULL) {
            return 0;
        }

        if ((needle == pathname || needle[-1] == PATH_SEP)
         && (needle[2] == '\0' || needle[2] == PATH_SEP)) {
            return 1;
        }

        /* increment so we search for the next match */
        needle++;
    };
    return 0;
}

static int isFileNameValidForMirroredOutput(const char *filename)
{
    return !pathnameHas2Dots(filename);
}


#define DIR_DEFAULT_MODE 0755
static mode_t getDirMode(const char *dirName)
{
    stat_t st;
    if (!UTIL_stat(dirName, &st)) {
        UTIL_DISPLAY("zstd: failed to get DIR stats %s: %s\n", dirName, strerror(errno));
        return DIR_DEFAULT_MODE;
    }
    if (!UTIL_isDirectoryStat(&st)) {
        UTIL_DISPLAY("zstd: expected directory: %s\n", dirName);
        return DIR_DEFAULT_MODE;
    }
    return st.st_mode;
}

static int makeDir(const char *dir, mode_t mode)
{
#if defined(_MSC_VER) || defined(__MINGW32__) || defined (__MSVCRT__)
    int ret = _mkdir(dir);
    (void) mode;
#else
    int ret = mkdir(dir, mode);
#endif
    if (ret != 0) {
        if (errno == EEXIST)
            return 0;
        UTIL_DISPLAY("zstd: failed to create DIR %s: %s\n", dir, strerror(errno));
    }
    return ret;
}

/* this function requires a mutable input string */
static void convertPathnameToDirName(char *pathname)
{
    size_t len = 0;
    char* pos = NULL;
    /* get dir name from pathname similar to 'dirname()' */
    assert(pathname != NULL);

    /* remove trailing '/' chars */
    len = strlen(pathname);
    assert(len > 0);
    while (pathname[len] == PATH_SEP) {
        pathname[len] = '\0';
        len--;
    }
    if (len == 0) return;

    /* if input is a single file, return '.' instead. i.e.
     * "xyz/abc/file.txt" => "xyz/abc"
       "./file.txt"       => "."
       "file.txt"         => "."
     */
    pos = strrchr(pathname, PATH_SEP);
    if (pos == NULL) {
        pathname[0] = '.';
        pathname[1] = '\0';
    } else {
        *pos = '\0';
    }
}

/* pathname must be valid */
static const char* trimLeadingRootChar(const char *pathname)
{
    assert(pathname != NULL);
    if (pathname[0] == PATH_SEP)
        return pathname + 1;
    return pathname;
}

/* pathname must be valid */
static const char* trimLeadingCurrentDirConst(const char *pathname)
{
    assert(pathname != NULL);
    if ((pathname[0] == '.') && (pathname[1] == PATH_SEP))
        return pathname + 2;
    return pathname;
}

static char*
trimLeadingCurrentDir(char *pathname)
{
    /* 'union charunion' can do const-cast without compiler warning */
    union charunion {
        char *chr;
        const char* cchr;
    } ptr;
    ptr.cchr = trimLeadingCurrentDirConst(pathname);
    return ptr.chr;
}

/* remove leading './' or '/' chars here */
static const char * trimPath(const char *pathname)
{
    return trimLeadingRootChar(
            trimLeadingCurrentDirConst(pathname));
}

static char* mallocAndJoin2Dir(const char *dir1, const char *dir2)
{
    const size_t dir1Size = strlen(dir1);
    const size_t dir2Size = strlen(dir2);
    char *outDirBuffer, *buffer, trailingChar;

    assert(dir1 != NULL && dir2 != NULL);
    outDirBuffer = (char *) malloc(dir1Size + dir2Size + 2);
    CONTROL(outDirBuffer != NULL);

    memcpy(outDirBuffer, dir1, dir1Size);
    outDirBuffer[dir1Size] = '\0';

    if (dir2[0] == '.')
        return outDirBuffer;

    buffer = outDirBuffer + dir1Size;
    trailingChar = *(buffer - 1);
    if (trailingChar != PATH_SEP) {
        *buffer = PATH_SEP;
        buffer++;
    }
    memcpy(buffer, dir2, dir2Size);
    buffer[dir2Size] = '\0';

    return outDirBuffer;
}

/* this function will return NULL if input srcFileName is not valid name for mirrored output path */
char* UTIL_createMirroredDestDirName(const char* srcFileName, const char* outDirRootName)
{
    char* pathname = NULL;
    if (!isFileNameValidForMirroredOutput(srcFileName))
        return NULL;

    pathname = mallocAndJoin2Dir(outDirRootName, trimPath(srcFileName));

    convertPathnameToDirName(pathname);
    return pathname;
}

static int
mirrorSrcDir(char* srcDirName, const char* outDirName)
{
    mode_t srcMode;
    int status = 0;
    char* newDir = mallocAndJoin2Dir(outDirName, trimPath(srcDirName));
    if (!newDir)
        return -ENOMEM;

    srcMode = getDirMode(srcDirName);
    status = makeDir(newDir, srcMode);
    free(newDir);
    return status;
}

static int
mirrorSrcDirRecursive(char* srcDirName, const char* outDirName)
{
    int status = 0;
    char* pp = trimLeadingCurrentDir(srcDirName);
    char* sp = NULL;

    while ((sp = strchr(pp, PATH_SEP)) != NULL) {
        if (sp != pp) {
            *sp = '\0';
            status = mirrorSrcDir(srcDirName, outDirName);
            if (status != 0)
                return status;
            *sp = PATH_SEP;
        }
        pp = sp + 1;
    }
    status = mirrorSrcDir(srcDirName, outDirName);
    return status;
}

static void
makeMirroredDestDirsWithSameSrcDirMode(char** srcDirNames, unsigned nbFile, const char* outDirName)
{
    unsigned int i = 0;
    for (i = 0; i < nbFile; i++)
        mirrorSrcDirRecursive(srcDirNames[i], outDirName);
}

static int
firstIsParentOrSameDirOfSecond(const char* firstDir, const char* secondDir)
{
    size_t firstDirLen  = strlen(firstDir),
           secondDirLen = strlen(secondDir);
    return firstDirLen <= secondDirLen &&
           (secondDir[firstDirLen] == PATH_SEP || secondDir[firstDirLen] == '\0') &&
           0 == strncmp(firstDir, secondDir, firstDirLen);
}

static int compareDir(const void* pathname1, const void* pathname2) {
    /* sort it after remove the leading '/'  or './'*/
    const char* s1 = trimPath(*(char * const *) pathname1);
    const char* s2 = trimPath(*(char * const *) pathname2);
    return strcmp(s1, s2);
}

static void
makeUniqueMirroredDestDirs(char** srcDirNames, unsigned nbFile, const char* outDirName)
{
    unsigned int i = 0, uniqueDirNr = 0;
    char** uniqueDirNames = NULL;

    if (nbFile == 0)
        return;

    uniqueDirNames = (char** ) malloc(nbFile * sizeof (char *));
    CONTROL(uniqueDirNames != NULL);

    /* if dirs is "a/b/c" and "a/b/c/d", we only need call:
     * we just need "a/b/c/d" */
    qsort((void *)srcDirNames, nbFile, sizeof(char*), compareDir);

    uniqueDirNr = 1;
    uniqueDirNames[uniqueDirNr - 1] = srcDirNames[0];
    for (i = 1; i < nbFile; i++) {
        char* prevDirName = srcDirNames[i - 1];
        char* currDirName = srcDirNames[i];

        /* note: we alwasy compare trimmed path, i.e.:
         * src dir of "./foo" and "/foo" will be both saved into:
         * "outDirName/foo/" */
        if (!firstIsParentOrSameDirOfSecond(trimPath(prevDirName),
                                            trimPath(currDirName)))
            uniqueDirNr++;

        /* we need maintain original src dir name instead of trimmed
         * dir, so we can retrive the original src dir's mode_t */
        uniqueDirNames[uniqueDirNr - 1] = currDirName;
    }

    makeMirroredDestDirsWithSameSrcDirMode(uniqueDirNames, uniqueDirNr, outDirName);

    free(uniqueDirNames);
}

static void
makeMirroredDestDirs(char** srcFileNames, unsigned nbFile, const char* outDirName)
{
    unsigned int i = 0;
    for (i = 0; i < nbFile; ++i)
        convertPathnameToDirName(srcFileNames[i]);
    makeUniqueMirroredDestDirs(srcFileNames, nbFile, outDirName);
}

void UTIL_mirrorSourceFilesDirectories(const char** inFileNames, unsigned int nbFile, const char* outDirName)
{
    unsigned int i = 0, validFilenamesNr = 0;
    char** srcFileNames = (char **) malloc(nbFile * sizeof (char *));
    CONTROL(srcFileNames != NULL);

    /* check input filenames is valid */
    for (i = 0; i < nbFile; ++i) {
        if (isFileNameValidForMirroredOutput(inFileNames[i])) {
            char* fname = STRDUP(inFileNames[i]);
            CONTROL(fname != NULL);
            srcFileNames[validFilenamesNr++] = fname;
        }
    }

    if (validFilenamesNr > 0) {
        makeDir(outDirName, DIR_DEFAULT_MODE);
        makeMirroredDestDirs(srcFileNames, validFilenamesNr, outDirName);
    }

    for (i = 0; i < validFilenamesNr; i++)
        free(srcFileNames[i]);
    free(srcFileNames);
}

FileNamesTable*
UTIL_createExpandedFNT(const char* const* inputNames, size_t nbIfns, int followLinks)
{
    unsigned nbFiles;
    char* buf = (char*)malloc(LIST_SIZE_INCREASE);
    char* bufend = buf + LIST_SIZE_INCREASE;

    if (!buf) return NULL;

    {   size_t ifnNb, pos;
        for (ifnNb=0, pos=0, nbFiles=0; ifnNb<nbIfns; ifnNb++) {
            if (!UTIL_isDirectory(inputNames[ifnNb])) {
                size_t const len = strlen(inputNames[ifnNb]);
                if (buf + pos + len >= bufend) {
                    ptrdiff_t newListSize = (bufend - buf) + LIST_SIZE_INCREASE;
                    assert(newListSize >= 0);
                    buf = (char*)UTIL_realloc(buf, (size_t)newListSize);
                    if (!buf) return NULL;
                    bufend = buf + newListSize;
                }
                if (buf + pos + len < bufend) {
                    memcpy(buf+pos, inputNames[ifnNb], len+1);  /* including final \0 */
                    pos += len + 1;
                    nbFiles++;
                }
            } else {
                nbFiles += (unsigned)UTIL_prepareFileList(inputNames[ifnNb], &buf, &pos, &bufend, followLinks);
                if (buf == NULL) return NULL;
    }   }   }

    /* note : even if nbFiles==0, function returns a valid, though empty, FileNamesTable* object */

    {   size_t ifnNb, pos;
        size_t const fntCapacity = nbFiles + 1;  /* minimum 1, allows adding one reference, typically stdin */
        const char** const fileNamesTable = (const char**)malloc(fntCapacity * sizeof(*fileNamesTable));
        if (!fileNamesTable) { free(buf); return NULL; }

        for (ifnNb = 0, pos = 0; ifnNb < nbFiles; ifnNb++) {
            fileNamesTable[ifnNb] = buf + pos;
            if (buf + pos > bufend) { free(buf); free((void*)fileNamesTable); return NULL; }
            pos += strlen(fileNamesTable[ifnNb]) + 1;
        }
        return UTIL_assembleFileNamesTable2(fileNamesTable, nbFiles, fntCapacity, buf);
    }
}


void UTIL_expandFNT(FileNamesTable** fnt, int followLinks)
{
    FileNamesTable* const newFNT = UTIL_createExpandedFNT((*fnt)->fileNames, (*fnt)->tableSize, followLinks);
    CONTROL(newFNT != NULL);
    UTIL_freeFileNamesTable(*fnt);
    *fnt = newFNT;
}

FileNamesTable* UTIL_createFNT_fromROTable(const char** filenames, size_t nbFilenames)
{
    size_t const sizeof_FNTable = nbFilenames * sizeof(*filenames);
    const char** const newFNTable = (const char**)malloc(sizeof_FNTable);
    if (newFNTable==NULL) return NULL;
    memcpy((void*)newFNTable, filenames, sizeof_FNTable);  /* void* : mitigate a Visual compiler bug or limitation */
    return UTIL_assembleFileNamesTable(newFNTable, nbFilenames, NULL);
}


/*-****************************************
*  count the number of physical cores
******************************************/

#if defined(_WIN32) || defined(WIN32)

#include <windows.h>

typedef BOOL(WINAPI* LPFN_GLPI)(PSYSTEM_LOGICAL_PROCESSOR_INFORMATION, PDWORD);

int UTIL_countPhysicalCores(void)
{
    static int numPhysicalCores = 0;
    if (numPhysicalCores != 0) return numPhysicalCores;

    {   LPFN_GLPI glpi;
        BOOL done = FALSE;
        PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = NULL;
        PSYSTEM_LOGICAL_PROCESSOR_INFORMATION ptr = NULL;
        DWORD returnLength = 0;
        size_t byteOffset = 0;

#if defined(_MSC_VER)
/* Visual Studio does not like the following cast */
#   pragma warning( disable : 4054 )  /* conversion from function ptr to data ptr */
#   pragma warning( disable : 4055 )  /* conversion from data ptr to function ptr */
#endif
        glpi = (LPFN_GLPI)(void*)GetProcAddress(GetModuleHandle(TEXT("kernel32")),
                                               "GetLogicalProcessorInformation");

        if (glpi == NULL) {
            goto failed;
        }

        while(!done) {
            DWORD rc = glpi(buffer, &returnLength);
            if (FALSE == rc) {
                if (GetLastError() == ERROR_INSUFFICIENT_BUFFER) {
                    if (buffer)
                        free(buffer);
                    buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(returnLength);

                    if (buffer == NULL) {
                        perror("zstd");
                        exit(1);
                    }
                } else {
                    /* some other error */
                    goto failed;
                }
            } else {
                done = TRUE;
        }   }

        ptr = buffer;

        while (byteOffset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION) <= returnLength) {

            if (ptr->Relationship == RelationProcessorCore) {
                numPhysicalCores++;
            }

            ptr++;
            byteOffset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
        }

        free(buffer);

        return numPhysicalCores;
    }

failed:
    /* try to fall back on GetSystemInfo */
    {   SYSTEM_INFO sysinfo;
        GetSystemInfo(&sysinfo);
        numPhysicalCores = sysinfo.dwNumberOfProcessors;
        if (numPhysicalCores == 0) numPhysicalCores = 1; /* just in case */
    }
    return numPhysicalCores;
}

#elif defined(__APPLE__)

#include <sys/sysctl.h>

/* Use apple-provided syscall
 * see: man 3 sysctl */
int UTIL_countPhysicalCores(void)
{
    static S32 numPhysicalCores = 0; /* apple specifies int32_t */
    if (numPhysicalCores != 0) return numPhysicalCores;

    {   size_t size = sizeof(S32);
        int const ret = sysctlbyname("hw.physicalcpu", &numPhysicalCores, &size, NULL, 0);
        if (ret != 0) {
            if (errno == ENOENT) {
                /* entry not present, fall back on 1 */
                numPhysicalCores = 1;
            } else {
                perror("zstd: can't get number of physical cpus");
                exit(1);
            }
        }

        return numPhysicalCores;
    }
}

#elif defined(__linux__)

/* parse /proc/cpuinfo
 * siblings / cpu cores should give hyperthreading ratio
 * otherwise fall back on sysconf */
int UTIL_countPhysicalCores(void)
{
    static int numPhysicalCores = 0;

    if (numPhysicalCores != 0) return numPhysicalCores;

    numPhysicalCores = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (numPhysicalCores == -1) {
        /* value not queryable, fall back on 1 */
        return numPhysicalCores = 1;
    }

    /* try to determine if there's hyperthreading */
    {   FILE* const cpuinfo = fopen("/proc/cpuinfo", "r");
#define BUF_SIZE 80
        char buff[BUF_SIZE];

        int siblings = 0;
        int cpu_cores = 0;
        int ratio = 1;

        if (cpuinfo == NULL) {
            /* fall back on the sysconf value */
            return numPhysicalCores;
        }

        /* assume the cpu cores/siblings values will be constant across all
         * present processors */
        while (!feof(cpuinfo)) {
            if (fgets(buff, BUF_SIZE, cpuinfo) != NULL) {
                if (strncmp(buff, "siblings", 8) == 0) {
                    const char* const sep = strchr(buff, ':');
                    if (sep == NULL || *sep == '\0') {
                        /* formatting was broken? */
                        goto failed;
                    }

                    siblings = atoi(sep + 1);
                }
                if (strncmp(buff, "cpu cores", 9) == 0) {
                    const char* const sep = strchr(buff, ':');
                    if (sep == NULL || *sep == '\0') {
                        /* formatting was broken? */
                        goto failed;
                    }

                    cpu_cores = atoi(sep + 1);
                }
            } else if (ferror(cpuinfo)) {
                /* fall back on the sysconf value */
                goto failed;
        }   }
        if (siblings && cpu_cores && siblings > cpu_cores) {
            ratio = siblings / cpu_cores;
        }

        if (ratio && numPhysicalCores > ratio) {
            numPhysicalCores = numPhysicalCores / ratio;
        }

failed:
        fclose(cpuinfo);
        return numPhysicalCores;
    }
}

#elif defined(__FreeBSD__)

#include <sys/param.h>
#include <sys/sysctl.h>

/* Use physical core sysctl when available
 * see: man 4 smp, man 3 sysctl */
int UTIL_countPhysicalCores(void)
{
    static int numPhysicalCores = 0; /* freebsd sysctl is native int sized */
    if (numPhysicalCores != 0) return numPhysicalCores;

#if __FreeBSD_version >= 1300008
    {   size_t size = sizeof(numPhysicalCores);
        int ret = sysctlbyname("kern.smp.cores", &numPhysicalCores, &size, NULL, 0);
        if (ret == 0) return numPhysicalCores;
        if (errno != ENOENT) {
            perror("zstd: can't get number of physical cpus");
            exit(1);
        }
        /* sysctl not present, fall through to older sysconf method */
    }
#endif

    numPhysicalCores = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (numPhysicalCores == -1) {
        /* value not queryable, fall back on 1 */
        numPhysicalCores = 1;
    }
    return numPhysicalCores;
}

#elif defined(__NetBSD__) || defined(__OpenBSD__) || defined(__DragonFly__) || defined(__CYGWIN__)

/* Use POSIX sysconf
 * see: man 3 sysconf */
int UTIL_countPhysicalCores(void)
{
    static int numPhysicalCores = 0;

    if (numPhysicalCores != 0) return numPhysicalCores;

    numPhysicalCores = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (numPhysicalCores == -1) {
        /* value not queryable, fall back on 1 */
        return numPhysicalCores = 1;
    }
    return numPhysicalCores;
}

#else

int UTIL_countPhysicalCores(void)
{
    /* assume 1 */
    return 1;
}

#endif

#if defined (__cplusplus)
}
#endif
