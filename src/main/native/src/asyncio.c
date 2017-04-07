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


#include "asyncio.h"

#include <time.h>
#include <libaio.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>

// Asynchronous IO

#define MAX_IO_EVENTS 512

int __initialized = 0;

// thread stuff
int stop_polling = 1, waiting_for_requests = 0;

pthread_t io_thread;
pthread_attr_t iothr_attr;

pthread_mutex_t request_mutex;
pthread_cond_t request_cv;

// I/O stuff
int num_requests = 0;
static io_context_t ctx;

void directio_fetcher(void *env) {

	int nevents;
	struct io_event events[MAX_IO_EVENTS], *eptr;
	struct timespec timeout = {0,200000000L};

	pthread_mutex_lock(&request_mutex);
	while(!stop_polling) {
		nevents = 0;
		if(!num_requests) {
			pthread_cond_wait(&request_cv, &request_mutex);
		}
		nevents = num_requests;
		pthread_mutex_unlock(&request_mutex);

		if(nevents > 0) {
			int n = nevents = io_getevents(ctx, 1, MAX_IO_EVENTS, events, &timeout);
			if(nevents < 0) {
				stop_polling = 1;
			}
			for (eptr = events ; n-- > 0 ; eptr++) {
				directio_cb *cb = (directio_cb *) eptr->data;
				if(cb != NULL) {
					cb->callback(env, cb->arguments, eptr->obj->u.c.buf, eptr->res, eptr->res2);
					free(eptr->obj);
				}
			}
		}
		pthread_mutex_lock(&request_mutex);
		num_requests -= nevents;
		pthread_cond_signal(&request_cv);
	}
	pthread_mutex_unlock(&request_mutex);
}

static int submit_request(int fd, char *buffer, int count, long long pos,
		directio_cb *callback, io_iocb_cmd_t opcode) {

	int ec, rc = 0;

	if(fd < 0 || buffer == NULL || count < 0 || pos < 0) {
		errno = EINVAL;
		return 1;
	}
	struct iocb *request = (struct iocb*) malloc(sizeof (struct iocb));
	memset(request, 0, sizeof (struct iocb));
	request->aio_fildes = fd;
	request->aio_lio_opcode = opcode;
	request->aio_reqprio = 0;
	request->u.c.buf = buffer;
	request->u.c.nbytes = count;
	request->u.c.offset = pos;
	request->data = (void *)callback;

	pthread_mutex_lock(&request_mutex);

retry_submit:
	ec = io_submit(ctx, 1, &request);
	switch(ec) {
		case EAGAIN:
			pthread_cond_wait(&request_cv, &request_mutex);
			goto retry_submit;
		case 1:
			break;
		default:
			rc = -1;
			goto exit_submit;
	}
	num_requests ++;
exit_submit:
	pthread_cond_signal(&request_cv);
	pthread_mutex_unlock(&request_mutex);
	return rc;
}

int directio_read(int fd, char *buffer, int count, long long pos, directio_cb *callback) {
	return submit_request(fd, buffer, count, pos, callback, IO_CMD_PREAD);
}

int directio_write(int fd, char *buffer, int count, long long pos, directio_cb *callback) {
	return submit_request(fd, buffer, count, pos, callback, IO_CMD_PWRITE);
}

int directio_init(void *(*thread_wrapper) (void *), void *args) {

	memset(&ctx, 0, sizeof(ctx));
	if(io_queue_init(MAX_IO_EVENTS, &ctx)) {
		return 1;
	}

	// set the thread loop condition
	stop_polling = 0;

	/* Initialize mutex and condition variable objects */
	pthread_mutex_init(&request_mutex, NULL);
	pthread_cond_init (&request_cv, NULL);

	/* Initialize the thread along with attributes */
	pthread_attr_init(&iothr_attr);
	pthread_attr_setdetachstate(&iothr_attr, PTHREAD_CREATE_JOINABLE);
	pthread_create(&io_thread, &iothr_attr, thread_wrapper, args);

	__initialized = 1;
	return 0;
}

int directio_deinit() {
	if(__initialized) {
		pthread_mutex_lock(&request_mutex);
		stop_polling = 1;
		pthread_cond_signal(&request_cv);
		pthread_mutex_unlock(&request_mutex);

		pthread_join(io_thread, NULL);

		/* Clean up and exit */
		pthread_attr_destroy(&iothr_attr);
		pthread_mutex_destroy(&request_mutex);
		pthread_cond_destroy(&request_cv);

		io_destroy(ctx);
	}
	return 0;
}
