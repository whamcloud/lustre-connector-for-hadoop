// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

#ifndef LUSTRE_HELPER_H
#define LUSTRE_HELPER_H

#include <jni.h>
#include <sys/stat.h>

// Required for setting stripe when creating files
// else Lustre complains that the file already exists
#define O_LUSTRE_DELAY_CREATE (0100000000 /* O_LOV_DELAY_CREATE_1_8 */ \
								| O_NOCTTY | 020000 /* FASYNC */)

#define LUSTRE_MAX_STRIPE_COUNT 2000
#define LUSTRE_MAX_OBD_DEVICES 8192
#define LUSTRE_MIN_STRIPE_SIZE (1L << 16)
#define LUSTRE_MAX_POOL_NAME 16

/**
 * LUSTRE-specific IOCTL definitions
 */
#define _IOC_SIZEBITS			 14
#define _IOC_DIRBITS			 2
#define _IOC_WRITE  			 1U
#define _IOW(type,nr,size)      _IOC(_IOC_WRITE,(type),(nr),sizeof(size))

#define LL_IOC_LOV_SETSTRIPE    _IOW ('f', 154, long)

struct lustre_object_id {
	union {
		struct objid {
			unsigned long object_id;
			unsigned long object_seq;
		} oid;
		struct fileid {
			unsigned long file_seq;
			unsigned int file_oid;
			unsigned int file_ver;
		} fid;
	} unknown;
};

struct lustre_ost_data {
	struct lustre_object_id lustre_ost_id;
	unsigned int lustre_ost_gen;
	unsigned int lustre_ost_idx;
}__attribute__((packed));

struct lustre_file_stat {
	unsigned int lustre_magic_bytes;
	unsigned int lustre_pattern;
	struct lustre_object_id mds_id;
	unsigned int lustre_stripe_size;
	unsigned short lustre_stripe_count;
	union {
		unsigned short lustre_stripe_offset;
		unsigned short lustre_layout_gen;
	};
	char lustre_pool_name[LUSTRE_MAX_POOL_NAME];
	struct lustre_ost_data stripe_data[0];
}__attribute__((packed));

// file system operations
int mkdirs(char *path, mode_t mode);
int del(char *path, int recursive);
int setstripe(int fd, long stripe_size, int stripe_count, int stripe_offset,
		char *pool_name, char* error_msg, int msglen);
#endif
