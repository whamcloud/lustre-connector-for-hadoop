/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#ifndef ASYNC_IO_H
#define ASYNC_IO_H

typedef void (*fn_directio_callback)(void *, void *, char *, int, int);

// direct IO functions
struct _java_directio_callback {
	void *arguments;
	fn_directio_callback callback;
};

typedef struct _java_directio_callback directio_cb;

int directio_init();
int directio_deinit();
void directio_fetcher(void *env);
int directio_read(int fd, char *buffer, int count, long long pos, directio_cb *callback);
int directio_write(int fd, char *buffer, int count, long long pos, directio_cb *callback);

#endif
