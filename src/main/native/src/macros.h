/**
 * This file includes some common utilities
 * for all native code used in hadoop.
 */

#if !defined MACROS_H
#define MACROS_H

#if defined(_WIN32)
#undef UNIX
#define WINDOWS
#else
#undef WINDOWS
#define UNIX
#endif

/* A helper macro to 'throw' a java exception. */
#define THROW(env, exception_name, message) \
  { \
	jclass ecls = (*env)->FindClass(env, exception_name); \
	if (ecls) { \
	  (*env)->ThrowNew(env, ecls, message); \
	  (*env)->DeleteLocalRef(env, ecls); \
	} \
  }

/* Helper macro to return if an exception is pending */
#define PASS_EXCEPTIONS(env) \
  { \
    if ((*env)->ExceptionCheck(env)) return; \
  }

#define PASS_EXCEPTIONS_GOTO(env, target) \
  { \
    if ((*env)->ExceptionCheck(env)) goto target; \
  }

#define PASS_EXCEPTIONS_RET(env, ret) \
  { \
    if ((*env)->ExceptionCheck(env)) return (ret); \
  }

#endif
