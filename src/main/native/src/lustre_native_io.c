// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

#include "lustre_helper.h"
#include "file_descriptor.h"
#include "errno_enum.h"
#include "exception.h"

#include <errno.h>
#include <jni.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

void iostreams_init(JNIEnv*);
void iostreams_deinit(JNIEnv*);

/*
 * Class:     org_apache_hadoop_lustre_io_LustreFsNativeImpl
 * Method:    init
 * Signature: ()V
 */

JNIEXPORT void JNICALL
Java_org_apache_hadoop_lustre_io_LustreFsNativeImpl_init(JNIEnv * env,
		jclass clazz) {
	nioe_init(env);
	PASS_EXCEPTIONS_GOTO(env, error);
	fd_init(env);
	PASS_EXCEPTIONS_GOTO(env, error);
	errno_enum_init(env);
	PASS_EXCEPTIONS_GOTO(env, error);
	iostreams_init(env);
	PASS_EXCEPTIONS_GOTO(env, error);
	return;
error:
	nioe_deinit(env);
	fd_deinit(env);
	errno_enum_deinit(env);
	iostreams_deinit(env);
}

/*
 * Class:     org_apache_hadoop_lustre_io_LustreFsNativeImpl
 * Method:    mkdir
 * Signature: (Ljava/lang/String;SZ)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_hadoop_lustre_io_LustreFsNativeImpl_mkdirs(JNIEnv *env,
		jobject object, jstring jpath, jshort jmode) {

	int rc;
	mode_t oumask, mode = (mode_t)( jmode | S_IRWXU | S_IRWXG | S_IRWXO );
	char *path = (char *)(*env)->GetStringUTFChars(env, jpath, NULL);

	if(path == NULL) {
		rc = 1;
		goto cleanup;
	}
	oumask = umask(0);
	umask(oumask & ~(S_IWUSR | S_IXUSR));
	rc = mkdirs(path, mode);
	umask(oumask);

cleanup:
	if (path != NULL) {
		(*env)->ReleaseStringUTFChars(env, jpath, path);
	}
	if(rc == 1) {
		throw_nioe(env, errno);
		return (jboolean)0;
	}
	else {
		return (jboolean)1;
	}
}

/*
 * Class:     org_apache_hadoop_lustre_io_LustreFsNativeImpl
 * Method:    rm
 * Signature: (Ljava/lang/String;Z)Z
 */
JNIEXPORT jboolean JNICALL
 Java_org_apache_hadoop_lustre_io_LustreFsNativeImpl_delete(
 JNIEnv *env, jobject object, jstring jpath, jboolean jrecusrive) {
	int rc;

	char *path = (char *)(*env)->GetStringUTFChars(env, jpath, NULL);
	if(path == NULL) {
		rc = 1;
		goto cleanup;
	}
	rc = del(path, (int)jrecusrive);
cleanup:
	if (path != NULL) {
		(*env)->ReleaseStringUTFChars(env, jpath, path);
	}
	if(rc == 1) {
		throw_nioe(env, errno);
		return (jboolean) 0;
	} else {
		return (jboolean) 1;
	}
 }

/*
 * Class:     org_apache_hadoop_lustre_io_LustreFsNativeImpl
 * Method:    setstripe
 * Signature: (Ljava/io/FileDescriptor;JIILjava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_lustre_io_LustreFsNativeImpl_setstripe(JNIEnv *env,
		jobject object, jobject fd_object, jlong jstripesize, jint jstripecount,
		jint jstripeoffset, jstring jpoolname) {

	char message[100];
	char *pool_name = NULL;

	if(jpoolname) {
		pool_name = (char *)(*env)->GetStringUTFChars(env, jpoolname, 0);
	}
	int rc = setstripe(fd_get(env, fd_object),
			(long) jstripesize, (int) jstripecount, (int) jstripeoffset,
			pool_name, message, sizeof(message));

	if(rc == 1) {
		THROW(env, "java/lang/IllegalArgumentException", message);
	}
	else if(rc == -1) {
		throw_nioe(env, errno);
	}

	if (pool_name != NULL) {
		(*env)->ReleaseStringUTFChars(env, jpoolname, pool_name);
	}
}

/*
 * Class:     org_apache_hadoop_lustre_io_LustreFsNativeImpl
 * Method:    open
 * Signature: (Ljava/lang/String;IIJIILjava/lang/String;)Ljava/io/FileDescriptor;
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_lustre_io_LustreFsNativeImpl_open
  (JNIEnv *env, jclass clazz, jstring jpath, jint flags, jint jmode,
	jlong jstripesize, jint jstripecount, jint jstripeoffset, jstring jpoolname) {

	int fd = -1;
	jobject ret = NULL;
	mode_t oumask, mode = (mode_t)( jmode | S_IRWXU | S_IRWXG | S_IRWXO );

	char *pool_name = NULL;
	char *path = (char *)(*env)->GetStringUTFChars(env, jpath, NULL);
	if (path == NULL)
		goto cleanup; // JVM throws Exception for us

	if (flags & O_CREAT) {
		char *delim = strrchr(path, '/');
		if(delim != NULL) {
			int rc = 0;
			delim[0] = '\0';
			oumask = umask(0);
			umask(oumask & ~(S_IWUSR | S_IXUSR));
			rc = mkdirs(path, mode);
			umask(oumask);
			delim[0] = '/';
			if(rc != 0) {
				throw_nioe(env, errno);
				goto cleanup;
			}
		}
retry_open:
		fd = open(path, flags | O_LUSTRE_DELAY_CREATE, mode);
		if (fd < 0) {
			if (errno == EISDIR && !(flags & O_DIRECTORY)) {
				flags = O_DIRECTORY | O_RDONLY;
				goto retry_open;
			}
		}
		if(fd >= 0) {
			char message[100];
			if(jpoolname) {
				pool_name = (char *)(*env)->GetStringUTFChars(env, jpoolname, 0);
			}
			int rc = setstripe(fd, (long) jstripesize,
					(int) jstripecount, (int) jstripeoffset,
					pool_name, message, sizeof(message));
			if(rc != 0) {
				if(rc == 1) {
					THROW(env, "java/lang/IllegalArgumentException", message);
				} else if (rc == -1) {
					throw_nioe(env, errno);
				}
				goto cleanup;
			}
		}
	} else {
		fd = open(path, flags);
	}
	if (fd < 0) {
		throw_nioe(env, errno);
		goto cleanup;
	}

	ret = fd_create(env, fd);

cleanup:
	if (pool_name != NULL) {
		(*env)->ReleaseStringUTFChars(env, jpoolname, pool_name);
	}
	if (path != NULL) {
		(*env)->ReleaseStringUTFChars(env, jpath, path);
	}
	return ret;

}


/*
 * Class:     org_apache_hadoop_lustre_io_LustreFsNativeImpl
 * Method:    close
 * Signature: (Ljava/lang/String;IIJIILjava/lang/String;)Ljava/io/FileDescriptor;
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_hadoop_lustre_io_LustreFsNativeImpl_close
  (JNIEnv *env, jclass clazz, jobject fd_object) {
	if(close(fd_get(env, fd_object))) {
		throw_nioe(env, errno);
	}
	return (jboolean)1;
}
