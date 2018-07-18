// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

#include "asyncio.h"
#include "errno_enum.h"
#include "exception.h"
#include "file_descriptor.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <jni.h>

static int _io_initialized = 0;
// internal direct buffer fields
static jfieldID fis_buf, fos_buf;
// io completion callback methods
static jmethodID fis_complete, fis_error;
static jmethodID fos_complete, fos_error;

void iostreams_deinit(JNIEnv *env) {
  fis_buf = NULL;
  fis_complete = NULL;
  fis_error = NULL;

  fos_buf = NULL;
  fos_complete = NULL;
  fos_error = NULL;

  _io_initialized = 0;
}

void iostreams_init(JNIEnv* env)
{
  if (_io_initialized) return; // already initialized

  jclass fis_class = (*env)->FindClass(env, "org/apache/hadoop/lustre/io/AsyncFileInputStream");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  fis_buf = (*env)->GetFieldID(env, fis_class, "_nativeBuf", "Ljava/nio/ByteBuffer;");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  fis_complete = (*env)->GetMethodID(env, fis_class, "complete", "(I)V");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  fis_error = (*env)->GetMethodID(env, fis_class, "error", "(Ljava/io/IOException;)V");
  PASS_EXCEPTIONS_GOTO(env, cleanup);

  jclass fos_class = (*env)->FindClass(env, "org/apache/hadoop/lustre/io/AsyncFileOutputStream");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  fos_buf = (*env)->GetFieldID(env, fos_class, "_nativeBuf", "Ljava/nio/ByteBuffer;");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  fos_complete = (*env)->GetMethodID(env, fos_class, "complete", "(I)V");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  fos_error = (*env)->GetMethodID(env, fos_class, "error", "(Ljava/io/IOException;)V");
  PASS_EXCEPTIONS_GOTO(env, cleanup);
  _io_initialized = 1;
  return;

cleanup:
	iostreams_deinit(env);
}

/*
 * Class:     org_apache_hadoop_lustre_io_MemUtils
 * Method:    allocate
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_lustre_io_MemUtils_allocate
	(JNIEnv *env, jclass clazz, jint jsize) {
	char *buffer;
	posix_memalign((void *)&buffer, (size_t)getpagesize(), (size_t)jsize);
	if(buffer == NULL) {
		throw_nioe(env, errno);
		return NULL;
	}
	return (*env)->NewDirectByteBuffer(env, (void*)buffer, (jlong)jsize);
}

/*
 * Class:     org_apache_hadoop_lustre_io_MemUtils
 * Method:    free
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL
Java_org_apache_hadoop_lustre_io_MemUtils_free
	(JNIEnv *env, jclass clazz, jobject jbuffer) {
	char *buffer = NULL;
	if(jbuffer == NULL) {
		return 0;
	}
	buffer = (char *)(*env)->GetDirectBufferAddress(env, jbuffer);
	if(buffer == NULL) {
		return 0;
	}
	free(buffer);
	if(errno) {
		throw_nioe(env, errno);
		return 1;
	}
	return 0;
}

struct _JVM_Context {
	jobject target;
	jmethodID success;
	jmethodID failure;
	directio_cb *cb;
};

typedef struct _JVM_Context JVM_Context;

static void *JVM_thread_wrapper(void *vmptr) {
	JNIEnv *env;
	JavaVM *jvm = (JavaVM *)vmptr;
	(*jvm)->AttachCurrentThreadAsDaemon(jvm, (void **)&env, NULL);
	directio_fetcher((void *)env);
	(*jvm)->DetachCurrentThread(jvm);
	return NULL;
}

static void request_handler(void *envptr, void *args, char *data, int length, int rc) {
	JNIEnv *env = (JNIEnv *)envptr;
	JVM_Context *ctx = (JVM_Context *)args;
	if(length < 0) {
		// if length is negative, it denotes the error code.
		(*env)->CallVoidMethod(env, ctx->target, ctx->failure, new_nioe(env, length));
	}
	else (*env)->CallVoidMethod(env, ctx->target, ctx->success, length);
	(*env)->DeleteGlobalRef(env, ctx->target);
	free(ctx->cb);
	free(ctx);
}

static void write_request_handler(void *envptr, void *args, char *data, int length, int rc) {
	if(data != NULL) free(data);
	request_handler(envptr, args, NULL, length, rc);
}

directio_cb *create_callback(JNIEnv *env, jobject target, jmethodID success,
		jmethodID failure, fn_directio_callback callback) {
	directio_cb *cb = malloc(sizeof(directio_cb));
	JVM_Context *ctx = malloc(sizeof(JVM_Context));
	ctx->cb = cb;
	ctx->target = (*env)->NewGlobalRef(env, target);
	ctx->success = success;
	ctx->failure = failure;
	cb->arguments = (void *)ctx;
	cb->callback = callback;
	return cb;
}

/*
 * Class:     org_apache_hadoop_lustre_io_AsyncFileInputStream
 * Method:    read
 * Signature: (Ljava/io/FileDescriptor;JI)I
 */

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_lustre_io_AsyncFileInputStream_read
 	 (JNIEnv *env, jobject jtarget, jobject fd_object, jlong jpos, jint jcount) {
	jobject buf = (*env)->GetObjectField(env, jtarget, fis_buf);
	char *buffer = (char *)(*env)->GetDirectBufferAddress(env, buf);
	return directio_read(fd_get(env, fd_object), buffer,
			(int) jcount, (long long) jpos, create_callback(env, jtarget,
					fis_complete, fis_error, request_handler));
}

/*
 * Class:     org_apache_hadoop_lustre_io_AsyncFileInputStream
 * Method:    size
 * Signature: (Ljava/io/FileDescriptor;)J
 */

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_lustre_io_AsyncFileInputStream_size
 	 (JNIEnv *env, jobject jtarget, jobject fd_object) {
	return (jlong)lseek(fd_get(env, fd_object), (off_t)0, SEEK_END);
}

/*
 * Class:     org_apache_hadoop_lustre_io_AsyncFileOutputStream
 * Method:    write
 * Signature: (Ljava/io/FileDescriptor;JI)I
 */

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_lustre_io_AsyncFileOutputStream_write
 	 (JNIEnv *env, jobject jtarget, jobject fd_object, jlong jpos, jint jcount) {
	jobject buf = (*env)->GetObjectField(env, jtarget, fos_buf);
//	char *buffer;
//	posix_memalign((void *)&buffer, (size_t)getpagesize(), (size_t)jcount);
//	if(buffer == NULL) {
//		throw_nioe(env, errno);
//		return -1;
//	}
//	memcpy(buffer, (char *)(*env)->GetDirectBufferAddress(env, buf), (size_t)jcount);
	char *buffer = (char *)(*env)->GetDirectBufferAddress(env, buf);
	return directio_write(fd_get(env, fd_object), buffer,
			(int) jcount, (long long) jpos, create_callback(env, jtarget,
					fos_complete, fos_error, request_handler));
}

/*
 * Class:     org_apache_hadoop_lustre_io_AsyncFileOutputStream
 * Method:    truncate
 * Signature: (Ljava/io/FileDescriptor;J)I
 */

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_lustre_io_AsyncFileOutputStream_truncate
 	 (JNIEnv *env, jobject jtarget, jobject fd_object, jlong jlength) {
	if(ftruncate(fd_get(env, fd_object), (off_t)jlength))
		throw_nioe(env, errno);
}


jint JNI_OnLoad(JavaVM *vm, void *reserved) {
	printf("Loaded Lustre Native Library\n");
	if(directio_init(JVM_thread_wrapper, (void *)vm)) {
		printf("Unable to initialize direct I/O\n");
	}
	return JNI_VERSION_1_4;
}

// Clean up
void JNI_OnUnload(JavaVM *vm, void *reserved) {
	printf("Unloading Lustre Native Library\n");
	directio_deinit();
	printf("Unloaded Lustre Native Library\n");
}
