/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  files.h,v 1.0 08/12/2016 02:46:50 PM
 *hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file files.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/12/2016 02:15:50 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef cluster_util_files_INC
#define cluster_util_files_INC

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace alisql {

static bool mkdirs(const char *dir) {
  char path_buf[1024];
  const int dir_mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
  char *p = NULL;
  size_t len = 0;
  snprintf(path_buf, sizeof(path_buf), "%s", dir);
  len = strlen(path_buf);
  if (path_buf[len - 1] == '/') {
    path_buf[len - 1] = 0;
  }
  for (p = path_buf + 1; *p; p++) {
    if (*p == '/') {
      *p = 0;
      int status = mkdir(path_buf, dir_mode);
      if (status != 0 && errno != EEXIST) {
        return false;
      }
      *p = '/';
    }
  }
  int status = mkdir(path_buf, dir_mode);
  if (status != 0 && errno != EEXIST) {
    return false;
  }
  return true;
}

static int isDir(char *filename) {
  struct stat buf;
  int ret = stat(filename, &buf);
  if (0 == ret) {
    if (buf.st_mode & S_IFDIR) {
      return 0;
    } else {
      return 1;
    }
  }
  return -1;
}

static int deleteDir(const char *dirname) {
  char chBuf[256];
  DIR *dir = NULL;
  struct dirent *ptr;
  int ret = 0;
  dir = opendir(dirname);
  if (NULL == dir) {
    remove(dirname);
    return -1;
  }
  while ((ptr = readdir(dir)) != NULL) {
    ret = strcmp(ptr->d_name, ".");
    if (0 == ret) {
      continue;
    }
    ret = strcmp(ptr->d_name, "..");
    if (0 == ret) {
      continue;
    }
    snprintf(chBuf, 256, "%s/%s", dirname, ptr->d_name);
    ret = isDir(chBuf);
    if (0 == ret) {
      ret = deleteDir(chBuf);
      if (0 != ret) {
        return -1;
      }
    } else if (1 == ret) {
      ret = remove(chBuf);
      if (0 != ret) {
        return -1;
      }
    }
  }
  (void)closedir(dir);

  ret = remove(dirname);
  if (0 != ret) {
    return -1;
  }
  return 0;
}

};      /* end of namespace alisql */

#endif  // #ifndef cluster_util_files_INC
