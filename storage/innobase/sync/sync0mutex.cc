/*****************************************************************************

Copyright (c) 1995, 2012, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2008, Google Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file sync/sync0mutex.cc
Mutex, the basic synchronization primitive

Created 2012/08/15 Sunny Bains
*******************************************************/

#include "sync0mutex.h"

#ifdef UNIV_NONINL
#include "sync0mutex.ic"
#endif /* UNIV_NOINL */

#include "ut0rnd.h"
#include "os0sync.h"
#include "srv0srv.h"
#include "os0thread.h"
#include "ut0counter.h"

/*
	REASONS FOR IMPLEMENTING THE SPIN LOCK MUTEX
	============================================

Semaphore operations in operating systems are slow: Solaris on a 1993 Sparc
takes 3 microseconds (us) for a lock-unlock pair and Windows NT on a 1995
Pentium takes 20 microseconds for a lock-unlock pair. Therefore, we have to
implement our own efficient spin lock mutex. Future operating systems may
provide efficient spin locks, but we cannot count on that.

Another reason for implementing a spin lock is that on multiprocessor systems
it can be more efficient for a processor to run a loop waiting for the
semaphore to be released than to switch to a different thread. A thread switch
takes 25 us on both platforms mentioned above. See Gray and Reuter's book
Transaction processing for background.

How long should the spin loop last before suspending the thread? On a
uniprocessor, spinning does not help at all, because if the thread owning the
mutex is not executing, it cannot be released. Spinning actually wastes
resources.

On a multiprocessor, we do not know if the thread owning the mutex is
executing or not. Thus it would make sense to spin as long as the operation
guarded by the mutex would typically last assuming that the thread is
executing. If the mutex is not released by that time, we may assume that the
thread owning the mutex is not executing and suspend the waiting thread.

A typical operation (where no i/o involved) guarded by a mutex or a read-write
lock may last 1 - 20 us on the current Pentium platform. The longest
operations are the binary searches on an index node.

We conclude that the best choice is to set the spin time at 20 us. Then the
system should work well on a multiprocessor. On a uniprocessor we have to
make sure that thread swithches due to mutex collisions are not frequent,
i.e., they do not happen every 100 us or so, because that wastes too much
resources. If the thread switches are not frequent, the 20 us wasted in spin
loop is not too much.

Empirical studies on the effect of spin time should be done for different
platforms.


	IMPLEMENTATION OF THE MUTEX
	===========================

For background, see Curt Schimmel's book on Unix implementation on modern
architectures. The key points in the implementation are atomicity and
serialization of memory accesses. The test-and-set instruction (XCHG in
Pentium) must be atomic. As new processors may have weak memory models, also
serialization of memory references may be necessary. The successor of Pentium,
P6, has at least one mode where the memory model is weak. As far as we know,
in Pentium all memory accesses are serialized in the program order and we do
not have to worry about the memory model. On other processors there are
special machine instructions called a fence, memory barrier, or storage
barrier (STBAR in Sparc), which can be used to serialize the memory accesses
to happen in program order relative to the fence instruction.

Leslie Lamport has devised a "bakery algorithm" to implement a mutex without
the atomic test-and-set, but his algorithm should be modified for weak memory
models. We do not use Lamport's algorithm, because we guess it is slower than
the atomic test-and-set.

Our mutex implementation works as follows: After that we perform the atomic
test-and-set instruction on the memory word. If the test returns zero, we
know we got the lock first. If the test returns not zero, some other thread
was quicker and got the lock: then we spin in a loop reading the memory word,
waiting it to become zero. It is wise to just read the word in the loop, not
perform numerous test-and-set instructions, because they generate memory
traffic between the cache and the main memory. The read loop can just access
the cache, saving bus bandwidth.

If we cannot acquire the mutex lock in the specified time, we reserve a cell
in the wait array, set the waiters byte in the mutex to 1. To avoid a race
condition, after setting the waiters byte and before suspending the waiting
thread, we still have to check that the mutex is reserved, because it may
have happened that the thread which was holding the mutex has just released
it and did not see the waiters byte set to 1, a case which would lead the
other thread to an infinite wait.

LEMMA 1: After a thread resets the event of a mutex (or rw_lock), some
=======
thread will eventually call os_event_set() on that particular event.
Thus no infinite wait is possible in this case.

Proof:	After making the reservation the thread sets the waiters field in the
mutex to 1. Then it checks that the mutex is still reserved by some thread,
or it reserves the mutex for itself. In any case, some thread (which may be
also some earlier thread, not necessarily the one currently holding the mutex)
will set the waiters field to 0 in mutex_exit, and then call
os_event_set() with the mutex as an argument.
Q.E.D.

LEMMA 2: If an os_event_set() call is made after some thread has called
=======
the os_event_reset() and before it starts wait on that event, the call
will not be lost to the second thread. This is true even if there is an
intervening call to os_event_reset() by another thread.
Thus no infinite wait is possible in this case.

Proof (non-windows platforms): os_event_reset() returns a monotonically
increasing value of signal_count. This value is increased at every
call of os_event_set() If thread A has called os_event_reset() followed
by thread B calling os_event_set() and then some other thread C calling
os_event_reset(), the is_set flag of the event will be set to FALSE;
but now if thread A calls os_event_wait_low() with the signal_count
value returned from the earlier call of os_event_reset(), it will
return immediately without waiting.
Q.E.D.

Proof (windows): If there is a writer thread which is forced to wait for
the lock, it may be able to set the state of rw_lock to RW_LOCK_WAIT_EX
The design of rw_lock ensures that there is one and only one thread
that is able to change the state to RW_LOCK_WAIT_EX and this thread is
guaranteed to acquire the lock after it is released by the current
holders and before any other waiter gets the lock.
On windows this thread waits on a separate event i.e.: wait_ex_event.
Since only one thread can wait on this event there is no chance
of this event getting reset before the writer starts wait on it.
Therefore, this thread is guaranteed to catch the os_set_event()
signalled unconditionally at the release of the lock.
Q.E.D. */

/* Number of spin waits on mutexes: for performance monitoring */

/** The number of iterations in the mutex_spin_wait() spin loop.
Intended for performance monitoring. */
UNIV_INTERN ib_counter_t<ib_int64_t, IB_N_SLOTS>
						mutex_spin_round_count;

/** The number of mutex_spin_wait() calls.  Intended for
performance monitoring. */
UNIV_INTERN ib_counter_t<ib_int64_t, IB_N_SLOTS>
						mutex_spin_wait_count;

/** The number of OS waits in mutex_spin_wait().  Intended for
performance monitoring. */
UNIV_INTERN ib_counter_t<ib_int64_t, IB_N_SLOTS>
						mutex_os_wait_count;

/** The number of mutex_exit() calls. Intended for performance
monitoring. */
UNIV_INTERN ib_int64_t				mutex_exit_count;

/** Mutex protecting sync_thread_level_arrays */
extern ib_mutex_t				sync_thread_mutex;

/******************************************************************//**
Creates, or rather, initializes a mutex object in a specified memory
location (which must be appropriately aligned). The mutex is initialized
in the reset state. Explicit freeing of the mutex with mutex_free is
necessary only if the memory block containing it is freed. */
UNIV_INTERN
void
mutex_create_func(
/*==============*/
	ib_mutex_t*	mutex,		/*!< in: pointer to memory */
#ifdef UNIV_DEBUG
	const char*	cmutex_name,	/*!< in: mutex name */
# ifdef UNIV_SYNC_DEBUG
	ulint		level,		/*!< in: level */
# endif /* UNIV_SYNC_DEBUG */
#endif /* UNIV_DEBUG */
	const char*	cfile_name,	/*!< in: file name where created */
	ulint		cline)		/*!< in: file line where created */
{
#if defined(HAVE_ATOMIC_BUILTINS)
	mutex_reset_lock_word(mutex);
#else
	os_fast_mutex_init(PFS_NOT_INSTRUMENTED, &mutex->os_fast_mutex);
	mutex->lock_word = 0;
#endif
	mutex->event = os_event_create(NULL);
	mutex_set_waiters(mutex, 0);
#ifdef UNIV_DEBUG
	mutex->magic_n = MUTEX_MAGIC_N;
#endif /* UNIV_DEBUG */
#ifdef UNIV_SYNC_DEBUG
	mutex->line = 0;
	mutex->file_name = "not yet reserved";
	mutex->level = level;
#endif /* UNIV_SYNC_DEBUG */
	mutex->cfile_name = cfile_name;
	mutex->cline = cline;
	mutex->count_os_wait = 0;

	/* Check that lock_word is aligned; this is important on Intel */
	ut_ad(((ulint)(&(mutex->lock_word))) % 4 == 0);

	/* NOTE! The very first mutexes are not put to the mutex list */

	if ((mutex == &mutex_list_mutex)
#ifdef UNIV_SYNC_DEBUG
	    || (mutex == &sync_thread_mutex)
#endif /* UNIV_SYNC_DEBUG */
	    ) {

		return;
	}

	mutex_enter(&mutex_list_mutex);

	ut_ad(UT_LIST_GET_LEN(mutex_list) == 0
	      || UT_LIST_GET_FIRST(mutex_list)->magic_n == MUTEX_MAGIC_N);

	UT_LIST_ADD_FIRST(list, mutex_list, mutex);

	mutex_exit(&mutex_list_mutex);
}

/******************************************************************//**
NOTE! Use the corresponding macro mutex_free(), not directly this function!
Calling this function is obligatory only if the memory buffer containing
the mutex is freed. Removes a mutex object from the mutex list. The mutex
is checked to be in the reset state. */
UNIV_INTERN
void
mutex_free_func(
/*============*/
	ib_mutex_t*	mutex)	/*!< in: mutex */
{
	ut_ad(mutex_validate(mutex));
	ut_a(mutex_get_lock_word(mutex) == 0);
	ut_a(mutex_get_waiters(mutex) == 0);

#ifdef UNIV_MEM_DEBUG
	if (mutex == &mem_hash_mutex) {
		ut_ad(UT_LIST_GET_LEN(mutex_list) == 1);
		ut_ad(UT_LIST_GET_FIRST(mutex_list) == &mem_hash_mutex);
		UT_LIST_REMOVE(list, mutex_list, mutex);
		goto func_exit;
	}
#endif /* UNIV_MEM_DEBUG */

	if (mutex != &mutex_list_mutex
#ifdef UNIV_SYNC_DEBUG
	    && mutex != &sync_thread_mutex
#endif /* UNIV_SYNC_DEBUG */
	    ) {

		mutex_enter(&mutex_list_mutex);

		ut_ad(!UT_LIST_GET_PREV(list, mutex)
		      || UT_LIST_GET_PREV(list, mutex)->magic_n
		      == MUTEX_MAGIC_N);
		ut_ad(!UT_LIST_GET_NEXT(list, mutex)
		      || UT_LIST_GET_NEXT(list, mutex)->magic_n
		      == MUTEX_MAGIC_N);

		UT_LIST_REMOVE(list, mutex_list, mutex);

		mutex_exit(&mutex_list_mutex);
	}

	os_event_free(mutex->event);
#ifdef UNIV_MEM_DEBUG
func_exit:
#endif /* UNIV_MEM_DEBUG */
#if !defined(HAVE_ATOMIC_BUILTINS)
	os_fast_mutex_free(&(mutex->os_fast_mutex));
#endif
	/* If we free the mutex protecting the mutex list (freeing is
	not necessary), we have to reset the magic number AFTER removing
	it from the list. */
#ifdef UNIV_DEBUG
	mutex->magic_n = 0;
#endif /* UNIV_DEBUG */
	return;
}

#ifdef UNIV_DEBUG
/******************************************************************//**
Checks that the mutex has been initialized.
@return	TRUE */
UNIV_INTERN
ibool
mutex_validate(
/*===========*/
	const ib_mutex_t*	mutex)	/*!< in: mutex */
{
	ut_a(mutex);
	ut_a(mutex->magic_n == MUTEX_MAGIC_N);

	return(TRUE);
}

/******************************************************************//**
Checks that the current thread owns the mutex. Works only in the debug
version.
@return	TRUE if owns */
UNIV_INTERN
ibool
mutex_own(
/*======*/
	const ib_mutex_t*	mutex)	/*!< in: mutex */
{
	ut_ad(mutex_validate(mutex));

	return(mutex_get_lock_word(mutex) == 1
	       && os_thread_eq(mutex->thread_id, os_thread_get_curr_id()));
}
#endif /* UNIV_DEBUG */

#ifdef UNIV_SYNC_DEBUG
/******************************************************************//**
Prints debug info of currently reserved mutexes. */
UNIV_INTERN
void
mutex_list_print_info(
/*==================*/
	FILE*	file)		/*!< in: file where to print */
{
	ulint	count		= 0;

	fprintf(stderr,
		"----------\n"
		"MUTEX INFO\n"
		"----------\n");

	mutex_enter(&mutex_list_mutex);

	for (const ib_mutex_t* mutex = UT_LIST_GET_FIRST(mutex_list);
	     mutex != NULL;
	     mutex = UT_LIST_GET_NEXT(list, mutex)) {

		++count;

		if (mutex_get_lock_word(mutex) != 0) {

			ulint		line;
			os_thread_id_t	thread_id;
			const char*	file_name;

			mutex_get_debug_info(
				mutex, &file_name, &line, &thread_id);

			fprintf(file,
				"Locked mutex: addr %p thread %ld"
				" file %s line %ld\n",
				(void*) mutex, os_thread_pf(thread_id),
				file_name, line);
		}
	}

	fprintf(file, "Total number of mutexes %ld\n", count);

	mutex_exit(&mutex_list_mutex);
}

/******************************************************************//**
Counts currently reserved mutexes. Works only in the debug version.
@return	number of reserved mutexes */
UNIV_INTERN
ulint
mutex_n_reserved()
/*==============*/
{
	ulint	count	= 0;

	mutex_enter(&mutex_list_mutex);

	for (const ib_mutex_t* mutex = UT_LIST_GET_FIRST(mutex_list);
	     mutex != NULL;
	     mutex = UT_LIST_GET_NEXT(list, mutex)) {

		if (mutex_get_lock_word(mutex) != 0) {

			++count;
		}
	}

	mutex_exit(&mutex_list_mutex);

	ut_a(count >= 1);

	/* Subtract one, because this function itself was holding
	one mutex (mutex_list_mutex) */

	return(count - 1);
}

/******************************************************************//**
Sets the debug information for a reserved mutex. */
UNIV_INTERN
void
mutex_set_debug_info(
/*=================*/
	ib_mutex_t*	mutex,		/*!< in/out: mutex */
	const char*	file_name,	/*!< in: file where requested */
	ulint		line)		/*!< in: line where requested */
{
	ut_ad(line > 0);
	ut_ad(file_name);

	sync_thread_add_level(mutex, mutex->level, FALSE);

	mutex->line = line;
	mutex->file_name = file_name;
}

/******************************************************************//**
Gets the debug information for a reserved mutex. */
UNIV_INTERN
void
mutex_get_debug_info(
/*=================*/
	const ib_mutex_t*	mutex,		/*!< in: mutex */
	const char**		file_name,	/*!< out: where it was locked */
	ulint*			line,		/*!< out: where it was locked */
	os_thread_id_t*		thread_id)	/*!< out: owner thread id */
{
	*line = mutex->line;
	*file_name = mutex->file_name;
	*thread_id = mutex->thread_id;
}
#endif /* UNIV_SYNC_DEBUG */

/******************************************************************//**
@return total number of spin rounds since startup. */
UNIV_INTERN
ib_uint64_t
mutex_spin_round_count_get()
/*========================*/
{
	return(mutex_spin_round_count);
}

/******************************************************************//**
@return total number of spin wait calls since startup. */
UNIV_INTERN
ib_uint64_t
mutex_spin_wait_count_get()
/*=======================*/
{
	return(mutex_spin_wait_count);
}

/******************************************************************//**
@return total number of OS waits since startup. */
UNIV_INTERN
ib_uint64_t
mutex_os_wait_count_get()
/*=====================*/
{
	return(mutex_os_wait_count);
}
