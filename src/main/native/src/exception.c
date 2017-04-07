/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "exception.h"
#include "errno_enum.h"
#include "macros.h"

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// the NativeIOException class and its constructor
static jclass nioe_clazz;
static jmethodID nioe_ctor;


void nioe_init(JNIEnv *env) {
	// Init NativeIOException
	nioe_clazz = (*env)->FindClass(env,
			"org/apache/hadoop/io/nativeio/NativeIOException");
	PASS_EXCEPTIONS(env);
	nioe_clazz = (*env)->NewGlobalRef(env, nioe_clazz);
	nioe_ctor = (*env)->GetMethodID(env, nioe_clazz, "<init>",
			"(Ljava/lang/String;Lorg/apache/hadoop/io/nativeio/Errno;)V");
}

void nioe_deinit(JNIEnv *env) {
	if (nioe_clazz != NULL) {
		(*env)->DeleteGlobalRef(env, nioe_clazz);
		nioe_clazz = NULL;
	}
	nioe_ctor = NULL;
}


/*
 * Throw a java.IO.IOException, generating the message from errno.
 */
jthrowable new_nioe(JNIEnv* env, int errnum) {
	char message[100];
	jstring jstr_message;
	if ((errnum >= 0) && (errnum < sys_nerr)) {
		snprintf(message, sizeof(message), "%s", sys_errlist[errnum]);
	} else {
		snprintf(message, sizeof(message), "Unknown error %d", errnum);
	}
	jstr_message = (*env)->NewStringUTF(env, message);
	return (jstr_message == NULL) ? NULL :
		(jthrowable)(*env)->NewObject(env, nioe_clazz, nioe_ctor,
				jstr_message, errno_to_enum(env, errnum));
}

void throw_nioe(JNIEnv* env, int errnum) {
	jthrowable nioe = new_nioe(env, errnum);
	if(nioe != NULL) {
		(*env)->Throw(env, nioe);
	}
}

jthrowable newExceptionV(JNIEnv* env, const char *name,
                         const char *fmt, va_list ap)
{
  int need;
  char buf[1], *msg = NULL;
  va_list ap2;
  jstring jstr = NULL;
  jthrowable jthr;
  jclass clazz;
  jmethodID excCtor;

  va_copy(ap2, ap);
  clazz = (*env)->FindClass(env, name);
  if (!clazz) {
    jthr = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);
    goto done;
  }
  excCtor = (*env)->GetMethodID(env,
        clazz, "<init>", "(Ljava/lang/String;)V");
  if (!excCtor) {
    jthr = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);
    goto done;
  }
  need = vsnprintf(buf, sizeof(buf), fmt, ap);
  if (need < 0) {
    fmt = "vsnprintf error";
    need = strlen(fmt);
  }
  msg = malloc(need + 1);
  vsnprintf(msg, need + 1, fmt, ap2);
  jstr = (*env)->NewStringUTF(env, msg);
  if (!jstr) {
    jthr = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);
    goto done;
  }
  jthr = (*env)->NewObject(env, clazz, excCtor, jstr);
  if (!jthr) {
    jthr = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);
    goto done;
  }

done:
  free(msg);
  va_end(ap2);
  (*env)->DeleteLocalRef(env, jstr);
  return jthr;
}

jthrowable newException(JNIEnv* env, const char *name, const char *fmt, ...)
{
  va_list ap;
  jthrowable jthr;

  va_start(ap, fmt);
  jthr = newExceptionV(env, name, fmt, ap);
  va_end(ap);
  return jthr;
}

jthrowable newRuntimeException(JNIEnv* env, const char *fmt, ...)
{
  va_list ap;
  jthrowable jthr;

  va_start(ap, fmt);
  jthr = newExceptionV(env, "java/lang/RuntimeException", fmt, ap);
  va_end(ap);
  return jthr;
}

jthrowable newIOException(JNIEnv* env, const char *fmt, ...)
{
  va_list ap;
  jthrowable jthr;

  va_start(ap, fmt);
  jthr = newExceptionV(env, "java/io/IOException", fmt, ap);
  va_end(ap);
  return jthr;
}

const char* terror(int errnum)
{
  if ((errnum < 0) || (errnum >= sys_nerr)) {
    return "unknown error.";
  }
  return sys_errlist[errnum];
}

