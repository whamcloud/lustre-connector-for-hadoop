// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

#include "lustre_helper.h"

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <ftw.h>

int mkdirs(char *path, mode_t mode) {
	struct stat info;

	// Check if path exists, but not a directory
	if (stat(path, &info) == 0) {
		if (!S_ISDIR(info.st_mode)) {
			return 1;
		}
		return 0;
	} else {
		// recurse only if path does not exist, else error out
		if (errno == ENOENT) {
			int rc = 0;
			char *ptr = path;
			char *sl = NULL;

			//ignore leading slash
			if (ptr[0] == '/') {
				ptr++;
			}

			//find the last path component
			while (ptr[0] != '\0') {
				if (ptr[0] == '/' && ptr[1] != '\0') {
					sl = ptr;
				}
				ptr++;
			}
			if (sl != NULL) {
				sl[0] = '\0';
				rc = mkdirs(path, mode);
				sl[0] = '/';
			}
			if (rc == 0) {
				if (mkdir(path, mode)) {
					rc = 1;
				}
			}
			return rc;
		} else {
			return 1;
		}
	}
}

static int del_node(const char *path,
		const struct stat *info, int typeflag, struct FTW *ftw) {
	return remove(path);
}

int del(char *path, int recursive) {
	return recursive ? nftw(path, del_node,
			64, FTW_DEPTH | FTW_PHYS) : remove(path);
}

int setstripe(int fd, long stripe_size, int stripe_count, int stripe_offset,
		char *pool_name, char* error_msg, int msglen) {

	struct lustre_file_stat info = { 0 };
	long page_size = 0;

	if (fd < 0) {
		snprintf(error_msg, msglen, "warning: your page size (%lu) is "
						"larger than expected (%lu)", page_size, LUSTRE_MIN_STRIPE_SIZE);
		return 1;
	}

	page_size = getpagesize();
		if (page_size > LUSTRE_MIN_STRIPE_SIZE) {
		snprintf(error_msg, msglen, "warning: your page size (%lu) is "
			"larger than expected (%lu)", page_size, LUSTRE_MIN_STRIPE_SIZE);
		return 1;
	}

	page_size = LUSTRE_MIN_STRIPE_SIZE;
	if ((stripe_size & (LUSTRE_MIN_STRIPE_SIZE - 1))) {
		snprintf(error_msg, msglen, "error: bad stripe_size %lu, "
			"must be an even multiple of %lu bytes", stripe_size, page_size);
		return 1;
	}
	if (stripe_offset < -1 || stripe_offset > LUSTRE_MAX_OBD_DEVICES) {
		snprintf(error_msg, msglen, "error: bad stripe offset %d",
				stripe_offset);
		return 1;
	}
	if (stripe_count < -1 || stripe_count > LUSTRE_MAX_STRIPE_COUNT) {
		snprintf(error_msg, msglen, "error: bad stripe count %d",
				stripe_count);
		return 1;
	}
	if (stripe_size >= (1ULL << 32)) {
		snprintf(error_msg, msglen, "warning: stripe size 4G or larger "
			"is not currently supported and would wrap");
		return 1;
	}

	info.lustre_magic_bytes = 0x0BD30BD0 /* V3 */;
	info.lustre_stripe_size = stripe_size;
	info.lustre_stripe_count = stripe_count;
	info.lustre_stripe_offset = stripe_offset;

	if (NULL != pool_name) {
		/* In case user gives the full pool name <fsname>.<poolname>,
		 * strip the fsname, else lustre crashes.
		 * Ideally we should verify that the pool name is valid,
		 * belongs to the named file system and that it is not empty
		 */
		char *ptr = strchr(pool_name, '.');
		ptr = (ptr == NULL) ? pool_name : (ptr + 1);
		strncpy(info.lustre_pool_name, ptr, LUSTRE_MAX_POOL_NAME);
	} else {
		info.lustre_magic_bytes = 0x0BD10BD0 /* V1 */;
	}
	return ioctl(fd, LL_IOC_LOV_SETSTRIPE, &info);
}
