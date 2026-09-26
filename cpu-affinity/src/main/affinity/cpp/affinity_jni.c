/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

// Use different error code to differentiate non-implemented error
static const int NOT_IMPLEMENTED = -2;

#ifdef __linux__
#define _GNU_SOURCE
#include <sched.h>
#include <unistd.h>
#include <sys/syscall.h>

static int set_affinity(int cpuid) {
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET((size_t)cpuid, &cpus);
    int threadId = (int)syscall(SYS_gettid);
    return sched_setaffinity(threadId, sizeof(cpu_set_t), &cpus);
}

static const int IS_AVAILABLE = 1;

#else

static int set_affinity(int cpuid) { return NOT_IMPLEMENTED; }

static const int IS_AVAILABLE = 0;

#endif

#ifdef _WIN32

#define strerror_r(errno,buf,len) strerror_s(buf,len,errno)

#endif

#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>

#include <org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni.h>

/*
 * Class:     org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni
 * Method:    isRoot
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_isRoot(JNIEnv *env, jclass cls) {
#ifdef __linux__
    return getuid() == 0;
#else
    return 0;
#endif
}

/*
 * Class:     org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni
 * Method:    isAvailable
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_isAvailable(JNIEnv *env, jclass cls) {
    return IS_AVAILABLE == 1;
}

/*
 * Class:     org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni
 * Method:    setAffinity
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_setAffinity(
    JNIEnv *env, jclass cls, jint cpuid) {
    int res = set_affinity(cpuid);

    if (res == 0) {
        // Success
        return;
    } else if (res == NOT_IMPLEMENTED) {
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/Exception"), "CPU affinity not implemented");
    } else {
        // Error in sched_setaffinity, get message from errno
        char buffer[1024];
        strerror_r(errno, buffer, sizeof(buffer));
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/Exception"), buffer);
    }
}

/* Explicit thread-mask API. Independent of legacy root/core acquisition and host CPU online policy. */
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#define EXPLICIT_MAX_CPUS 65536

static void mask_error(JNIEnv *env, const char *operation, int error) {
    char message[160];
    snprintf(message, sizeof(message), "%s failed (errno=%d)", operation, error);
    (*env)->ThrowNew(env, (*env)->FindClass(env, "java/io/IOException"), message);
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_getAffinityMask(JNIEnv *env, jclass cls) {
#ifdef __linux__
    int capacity;
    for (capacity = 128; capacity <= EXPLICIT_MAX_CPUS; capacity *= 2) {
        size_t size = CPU_ALLOC_SIZE(capacity);
        cpu_set_t *mask = CPU_ALLOC(capacity);
        if (mask == NULL) {
            mask_error(env, "CPU_ALLOC", ENOMEM);
            return NULL;
        }
        CPU_ZERO_S(size, mask);
        int result = sched_getaffinity(0, size, mask);
        int error = errno;
        if (result != 0) {
            CPU_FREE(mask);
            if (error == EINVAL && capacity < EXPLICIT_MAX_CPUS) {
                continue;
            }
            mask_error(env, "sched_getaffinity", error);
            return NULL;
        }
        int words_count = capacity / 64;
        jlong *words = calloc((size_t)words_count, sizeof(jlong));
        if (words == NULL) {
            CPU_FREE(mask);
            mask_error(env, "mask allocation", ENOMEM);
            return NULL;
        }
        int cpu;
        for (cpu = 0; cpu < capacity; cpu++) {
            if (CPU_ISSET_S(cpu, size, mask)) {
                words[cpu / 64] |= (jlong)(UINT64_C(1) << (cpu % 64));
            }
        }
        CPU_FREE(mask);
        jlongArray output = (*env)->NewLongArray(env, words_count);
        if (output != NULL) {
            (*env)->SetLongArrayRegion(env, output, 0, words_count, words);
        }
        free(words);
        return output;
    }
    mask_error(env, "mask capacity", EINVAL);
#else
    mask_error(env, "current-thread affinity unsupported", NOT_IMPLEMENTED);
#endif
    return NULL;
}

JNIEXPORT void JNICALL
Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_setAffinityMask(
        JNIEnv *env, jclass cls, jintArray cpu_ids) {
#ifdef __linux__
    if (cpu_ids == NULL) {
        mask_error(env, "null CPU mask", EINVAL);
        return;
    }
    jsize count = (*env)->GetArrayLength(env, cpu_ids);
    if (count == 0 || count > EXPLICIT_MAX_CPUS) {
        mask_error(env, "CPU mask length", EINVAL);
        return;
    }
    jint *ids = (*env)->GetIntArrayElements(env, cpu_ids, NULL);
    if (ids == NULL) {
        return; /* JNI has raised OutOfMemoryError. */
    }
    int capacity = 128;
    int i;
    for (i = 0; i < count; i++) {
        if (ids[i] < 0 || ids[i] >= EXPLICIT_MAX_CPUS) {
            (*env)->ReleaseIntArrayElements(env, cpu_ids, ids, JNI_ABORT);
            mask_error(env, "CPU index", EINVAL);
            return;
        }
        if (ids[i] >= capacity) {
            capacity = ids[i] + 1;
        }
    }
    size_t size = CPU_ALLOC_SIZE(capacity);
    cpu_set_t *mask = CPU_ALLOC(capacity);
    if (mask == NULL) {
        (*env)->ReleaseIntArrayElements(env, cpu_ids, ids, JNI_ABORT);
        mask_error(env, "CPU_ALLOC", ENOMEM);
        return;
    }
    CPU_ZERO_S(size, mask);
    for (i = 0; i < count; i++) {
        if (CPU_ISSET_S(ids[i], size, mask)) {
            CPU_FREE(mask);
            (*env)->ReleaseIntArrayElements(env, cpu_ids, ids, JNI_ABORT);
            mask_error(env, "duplicate CPU index", EINVAL);
            return;
        }
        CPU_SET_S(ids[i], size, mask);
    }
    (*env)->ReleaseIntArrayElements(env, cpu_ids, ids, JNI_ABORT);
    int result = sched_setaffinity(0, size, mask);
    int error = errno;
    CPU_FREE(mask);
    if (result != 0) {
        mask_error(env, "sched_setaffinity", error);
    }
#else
    mask_error(env, "current-thread affinity unsupported", NOT_IMPLEMENTED);
#endif
}
