/* Copyright (C) 2009 Sun Microsystems, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/**
   This is a unit test for the 'meta data locking' classes.
   It is written to illustrate how we can use googletest for unit testing
   of MySQL code.
   For documentation on googletest, see http://code.google.com/p/googletest/
   and the contained wiki pages GoogleTestPrimer and GoogleTestAdvancedGuide.
   The code below should hopefully be (mostly) self-explanatory.
 */

// First include (the generated) my_config.h, to get correct platform defines,
// then gtest.h (before any other MySQL headers), to avoid min() macros etc ...
#include "my_config.h"
#include <gtest/gtest.h>

#include "mdl.h"
#include <mysqld_error.h>

#include "thr_malloc.h"
#include "thread_utils.h"

pthread_key(MEM_ROOT**,THR_MALLOC);
pthread_key(THD*, THR_THD);
mysql_mutex_t LOCK_open;
uint    opt_debug_sync_timeout= 0;

static mysql_mutex_t *current_mutex= NULL;
extern "C"
const char* thd_enter_cond(MYSQL_THD thd, mysql_cond_t *cond,
                           mysql_mutex_t *mutex, const char *msg)
{
  current_mutex= mutex;
  return NULL;
}

extern "C"
void thd_exit_cond(MYSQL_THD thd, const char *old_msg)
{
  mysql_mutex_unlock(current_mutex);
}

extern "C" int thd_killed(const MYSQL_THD thd)
{
  return 0;
}

/*
  A mock error handler.
*/
static uint expected_error= 0;
extern "C" void test_error_handler_hook(uint err, const char *str, myf MyFlags)
{
  EXPECT_EQ(expected_error, err) << str;
}

/*
  A mock out-of-memory handler.
  We do not expect this to be called during testing.
*/
extern "C" void sql_alloc_error_handler(void)
{
  ADD_FAILURE();
}

namespace {
bool notify_thread(THD*);
}

/*
  We need to mock away this global function, because the real version
  pulls in a lot of dependencies.
  (The @note for the real version of this function indicates that the
  coupling between THD and MDL is too tight.)
   @retval  TRUE  if the thread was woken up
   @retval  FALSE otherwise.
*/
bool mysql_notify_thread_having_shared_lock(THD *thd, THD *in_use,
                                            bool needs_thr_lock_abort)
{
  if (in_use != NULL)
    return notify_thread(in_use);
  return FALSE;
}

/*
  Mock away this function as well, with an empty function.
  @todo didrik: Consider verifying that the MDL module actually calls
  this with correct arguments.
*/
void mysql_ha_flush(THD *)
{
  DBUG_PRINT("mysql_ha_flush", ("mock version"));
}

/*
  We need to mock away this global function, the real version pulls in
  too many dependencies.
 */
extern "C" const char *set_thd_proc_info(void *thd, const char *info,
                                         const char *calling_function,
                                         const char *calling_file,
                                         const unsigned int calling_line)
{
  DBUG_PRINT("proc_info", ("%s:%d  %s", calling_file, calling_line,
                           (info != NULL) ? info : "(null)"));
  return info;
}

/*
  Mock away this global function.
  We don't need DEBUG_SYNC functionality in a unit test.
 */
void debug_sync(THD *thd, const char *sync_point_name, size_t name_len)
{
  DBUG_PRINT("debug_sync_point", ("hit: '%s'", sync_point_name));
  FAIL() << "Not yet implemented.";
}

/*
  Putting everything in an unnamed namespace prevents any (unintentional)
  name clashes with the code under test.
*/
namespace {

using thread::Notification;
using thread::Thread;

const char db_name[]= "some_database";
const char table_name1[]= "some_table1";
const char table_name2[]= "some_table2";
const char table_name3[]= "some_table3";
const char table_name4[]= "some_table4";
const ulong zero_timeout= 0;
const ulong long_timeout= (ulong) 3600L*24L*365L;


class MDL_test : public ::testing::Test
{
protected:
  MDL_test()
  : m_thd(NULL),
    m_null_ticket(NULL),
    m_null_request(NULL)
  {
  }

  static void SetUpTestCase()
  {
    error_handler_hook= test_error_handler_hook;
  }

  void SetUp()
  {
    expected_error= 0;
    mdl_init();
    m_mdl_context.init(m_thd);
    EXPECT_FALSE(m_mdl_context.has_locks());
    m_global_request.init(MDL_key::GLOBAL, "", "", MDL_INTENTION_EXCLUSIVE);
  }

  void TearDown()
  {
    m_mdl_context.destroy();
    mdl_destroy();
  }

  // A utility member for testing single lock requests.
  void test_one_simple_shared_lock(enum_mdl_type lock_type);

  THD               *m_thd;
  const MDL_ticket  *m_null_ticket;
  const MDL_request *m_null_request;
  MDL_context        m_mdl_context;
  MDL_request        m_request;
  MDL_request        m_global_request;
  MDL_request_list   m_request_list;
private:
  GTEST_DISALLOW_COPY_AND_ASSIGN_(MDL_test);
};


/*
  Will grab a lock on table_name of given type in the run() function.
  The two notifications are for synchronizing with the main thread.
  Does *not* take ownership of the notifications.
*/
class MDL_thread : public Thread
{
public:
  MDL_thread(const char   *table_name,
             enum_mdl_type mdl_type,
             Notification *lock_grabbed,
             Notification *release_locks)
  : m_table_name(table_name),
    m_mdl_type(mdl_type),
    m_lock_grabbed(lock_grabbed),
    m_release_locks(release_locks),
    m_ignore_notify(false)
  {
    m_thd= reinterpret_cast<THD*>(this);    // See notify_thread below.
    m_mdl_context.init(m_thd);
  }

  ~MDL_thread()
  {
    m_mdl_context.destroy();
  }

  virtual void run();
  void ignore_notify() { m_ignore_notify= true; }

  bool notify()
  {
    if (m_ignore_notify)
      return false;
    m_release_locks->notify();
    return true;
  }

private:
  const char    *m_table_name;
  enum_mdl_type  m_mdl_type;
  Notification  *m_lock_grabbed;
  Notification  *m_release_locks;
  bool           m_ignore_notify;
  THD           *m_thd;
  MDL_context    m_mdl_context;
};


// Admittedly an ugly hack, to avoid pulling in the THD in this unit test.
bool notify_thread(THD *thd)
{
  MDL_thread *thread = (MDL_thread*) thd;
  return thread->notify();
}


void MDL_thread::run()
{
  MDL_request request;
  MDL_request global_request;
  MDL_request_list request_list;
  global_request.init(MDL_key::GLOBAL, "", "", MDL_INTENTION_EXCLUSIVE);
  request.init(MDL_key::TABLE, db_name, m_table_name, m_mdl_type);

  request_list.push_front(&request);
  if (m_mdl_type >= MDL_SHARED_NO_WRITE)
    request_list.push_front(&global_request);

  EXPECT_FALSE(m_mdl_context.acquire_locks(&request_list, long_timeout));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, m_table_name, m_mdl_type));

  // Tell the main thread that we have grabbed our locks.
  m_lock_grabbed->notify();
  // Hold on to locks until we are told to release them
  m_release_locks->wait_for_notification();

  m_mdl_context.rollback_to_savepoint(NULL);
}

// googletest recommends DeathTest suffix for classes use in death tests.
typedef MDL_test MDL_DeathTest;


/*
  Verifies that we die with a DBUG_ASSERT if we destry a non-empty MDL_context.
 */
#if GTEST_HAS_DEATH_TEST && !defined(DBUG_OFF)
TEST_F(MDL_DeathTest, die_when_m_tickets_nonempty)
{
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);

  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&m_request));
  EXPECT_DEATH(m_mdl_context.destroy(), ".*Assertion .*m_tickets.is_empty.*");
  m_mdl_context.release_all_locks_for_name(m_request.ticket);
}
#endif  // GTEST_HAS_DEATH_TEST && !defined(DBUG_OFF)



/*
  The most basic test: just construct and destruct our test fixture.
 */
TEST_F(MDL_test, construct_and_destruct)
{
}


/*
  Verifies that we can create requests with the factory function
  MDL_request::create().
 */
TEST_F(MDL_test, factory_function)
{
  MEM_ROOT mem_root;
  init_sql_alloc(&mem_root, 1024, 0);
  // This request should not be destroyed in the normal C++ fashion.
  MDL_request *request=
    MDL_request::create(MDL_key::TABLE,
                        db_name, table_name1, MDL_SHARED, &mem_root);
  ASSERT_NE(m_null_request, request);
  EXPECT_EQ(m_null_ticket, request->ticket);
  free_root(&mem_root, MYF(0));
}


void MDL_test::test_one_simple_shared_lock(enum_mdl_type lock_type)
{
  m_request.init(MDL_key::TABLE, db_name, table_name1, lock_type);

  EXPECT_EQ(lock_type, m_request.type);
  EXPECT_EQ(m_null_ticket, m_request.ticket);

  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&m_request));
  EXPECT_NE(m_null_ticket, m_request.ticket);
  EXPECT_TRUE(m_mdl_context.has_locks());
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, lock_type));

  MDL_request request_2;
  request_2.init(&m_request.key, lock_type);
  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&request_2));
  EXPECT_EQ(m_request.ticket, request_2.ticket);

  m_mdl_context.release_all_locks_for_name(m_request.ticket);
  EXPECT_FALSE(m_mdl_context.has_locks());
}


/*
  Acquires one lock of type MDL_SHARED.
 */
TEST_F(MDL_test, one_shared)
{
  test_one_simple_shared_lock(MDL_SHARED);
}


/*
  Acquires one lock of type MDL_SHARED_HIGH_PRIO.
 */
TEST_F(MDL_test, one_shared_high_prio)
{
  test_one_simple_shared_lock(MDL_SHARED_HIGH_PRIO);
}


/*
  Acquires one lock of type MDL_SHARED_READ.
 */
TEST_F(MDL_test, one_shared_read)
{
  test_one_simple_shared_lock(MDL_SHARED_READ);
}


/*
  Acquires one lock of type MDL_SHARED_WRITE.
 */
TEST_F(MDL_test, one_shared_write)
{
  test_one_simple_shared_lock(MDL_SHARED_WRITE);
}


/*
  Acquires one lock of type MDL_EXCLUSIVE.  
 */
TEST_F(MDL_test, one_exclusive)
{
  const enum_mdl_type lock_type= MDL_EXCLUSIVE;
  m_request.init(MDL_key::TABLE, db_name, table_name1, lock_type);
  EXPECT_EQ(m_null_ticket, m_request.ticket);

  m_request_list.push_front(&m_request);
  m_request_list.push_front(&m_global_request);

  EXPECT_FALSE(m_mdl_context.acquire_locks(&m_request_list, long_timeout));

  EXPECT_NE(m_null_ticket, m_request.ticket);
  EXPECT_NE(m_null_ticket, m_global_request.ticket);
  EXPECT_TRUE(m_mdl_context.has_locks());
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, lock_type));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::GLOBAL, "", "", MDL_INTENTION_EXCLUSIVE));
  EXPECT_TRUE(m_request.ticket->is_upgradable_or_exclusive());

  m_mdl_context.release_all_locks_for_name(m_request.ticket);
  m_mdl_context.release_lock(m_global_request.ticket);
  EXPECT_FALSE(m_mdl_context.has_locks());
}


/*
  Acquires two locks, on different tables, of type MDL_SHARED.
  Verifies that they are independent.
 */
TEST_F(MDL_test, two_shared)
{
  MDL_request request_2;
  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);
  request_2.init(MDL_key::TABLE, db_name, table_name2, MDL_SHARED);

  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&m_request));
  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&request_2));
  EXPECT_TRUE(m_mdl_context.has_locks());
  ASSERT_NE(m_null_ticket, m_request.ticket);
  ASSERT_NE(m_null_ticket, request_2.ticket);

  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name2, MDL_SHARED));
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE, db_name, table_name3, MDL_SHARED));

  m_mdl_context.release_lock(m_request.ticket);
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));
  EXPECT_TRUE(m_mdl_context.has_locks());

  m_mdl_context.release_lock(request_2.ticket);
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE, db_name, table_name2, MDL_SHARED));
  EXPECT_FALSE(m_mdl_context.has_locks());
}


/*
  Verifies that two different contexts can acquire a shared lock
  on the same table.
 */
TEST_F(MDL_test, shared_locks_between_contexts)
{
  THD         *thd2= (THD*) this;
  MDL_context  mdl_context2;
  mdl_context2.init(thd2);
  MDL_request request_2;
  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);
  request_2.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);
  
  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&m_request));
  EXPECT_FALSE(mdl_context2.try_acquire_lock(&request_2));

  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));
  EXPECT_TRUE(mdl_context2.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));

  m_mdl_context.release_all_locks_for_name(m_request.ticket);
  mdl_context2.release_all_locks_for_name(request_2.ticket);
}


/*
  Verifies that we can upgrade a shared lock to exclusive.
 */
TEST_F(MDL_test, upgrade_shared_upgradable)
{
  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED_NO_WRITE);

  m_request_list.push_front(&m_request);
  m_request_list.push_front(&m_global_request);

  EXPECT_FALSE(m_mdl_context.acquire_locks(&m_request_list, long_timeout));
  EXPECT_FALSE(m_mdl_context.
               upgrade_shared_lock_to_exclusive(m_request.ticket, long_timeout));
  EXPECT_EQ(MDL_EXCLUSIVE, m_request.ticket->get_type());

  // Another upgrade should be a no-op.
  EXPECT_FALSE(m_mdl_context.
               upgrade_shared_lock_to_exclusive(m_request.ticket, long_timeout));
  EXPECT_EQ(MDL_EXCLUSIVE, m_request.ticket->get_type());

  m_mdl_context.release_all_locks_for_name(m_request.ticket);
  m_mdl_context.release_lock(m_global_request.ticket);
}


/*
  Verifies that only upgradable locks can be upgraded to exclusive.
 */
TEST_F(MDL_DeathTest, die_upgrade_shared)
{
  MDL_request request_2;
  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);
  request_2.init(MDL_key::TABLE, db_name, table_name2, MDL_SHARED_NO_READ_WRITE);

  m_request_list.push_front(&m_request);
  m_request_list.push_front(&request_2);
  m_request_list.push_front(&m_global_request);
  
  EXPECT_FALSE(m_mdl_context.acquire_locks(&m_request_list, long_timeout));

#if GTEST_HAS_DEATH_TEST && !defined(DBUG_OFF)
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH_IF_SUPPORTED(m_mdl_context.
                            upgrade_shared_lock_to_exclusive(m_request.ticket,
                                                             long_timeout),
                            ".*MDL_SHARED_NO_.*");
#endif
  EXPECT_FALSE(m_mdl_context.
               upgrade_shared_lock_to_exclusive(request_2.ticket, long_timeout));
  m_mdl_context.rollback_to_savepoint(NULL);
}


/*
  Verfies that locks are released when we roll back to a savepoint.
 */
TEST_F(MDL_test, savepoint)
{
  MDL_request request_2;
  MDL_request request_3;
  MDL_request request_4;
  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);
  request_2.init(MDL_key::TABLE, db_name, table_name2, MDL_SHARED);
  request_3.init(MDL_key::TABLE, db_name, table_name3, MDL_SHARED);
  request_4.init(MDL_key::TABLE, db_name, table_name4, MDL_SHARED);

  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&m_request));
  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&request_2));
  MDL_ticket *savepoint= m_mdl_context.mdl_savepoint();
  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&request_3));
  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&request_4));

  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name2, MDL_SHARED));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name3, MDL_SHARED));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name4, MDL_SHARED));

  m_mdl_context.rollback_to_savepoint(savepoint);
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name2, MDL_SHARED));
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE, db_name, table_name3, MDL_SHARED));
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE, db_name, table_name4, MDL_SHARED));

  m_mdl_context.rollback_to_savepoint(NULL);
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE, db_name, table_name2, MDL_SHARED));
}


/*
  Verifies that we can grab shared locks concurrently, in different threads.
 */
TEST_F(MDL_test, concurrent_shared)
{
  Notification lock_grabbed;
  Notification release_locks;
  MDL_thread mdl_thread(table_name1, MDL_SHARED, &lock_grabbed, &release_locks);
  mdl_thread.start();
  lock_grabbed.wait_for_notification();

  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);

  EXPECT_FALSE(m_mdl_context.acquire_lock(&m_request, long_timeout));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE, db_name, table_name1, MDL_SHARED));

  release_locks.notify();
  mdl_thread.join();

  m_mdl_context.release_all_locks_for_name(m_request.ticket);
}


/*
  Verifies that we cannot grab an exclusive lock on something which
  is locked with a shared lock in a different thread.
 */
TEST_F(MDL_test, concurrent_shared_exclusive)
{
  expected_error= ER_LOCK_WAIT_TIMEOUT;

  Notification lock_grabbed;
  Notification release_locks;
  MDL_thread mdl_thread(table_name1, MDL_SHARED, &lock_grabbed, &release_locks);
  mdl_thread.ignore_notify();
  mdl_thread.start();
  lock_grabbed.wait_for_notification();

  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_EXCLUSIVE);

  m_request_list.push_front(&m_request);
  m_request_list.push_front(&m_global_request);

  // We should *not* be able to grab the lock here.
  EXPECT_TRUE(m_mdl_context.acquire_locks(&m_request_list, zero_timeout));
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE,
                             db_name, table_name1, MDL_EXCLUSIVE));

  release_locks.notify();
  mdl_thread.join();

  // Now we should be able to grab the lock.
  EXPECT_FALSE(m_mdl_context.acquire_locks(&m_request_list, zero_timeout));
  EXPECT_NE(m_null_ticket, m_request.ticket);

  m_mdl_context.release_all_locks_for_name(m_request.ticket);
  m_mdl_context.release_lock(m_global_request.ticket);
}


/*
  Verifies that we cannot we cannot grab a shared lock on something which
  is locked exlusively in a different thread.
 */
TEST_F(MDL_test, concurrent_exclusive_shared)
{
  Notification lock_grabbed;
  Notification release_locks;
  MDL_thread mdl_thread(table_name1, MDL_EXCLUSIVE,
                        &lock_grabbed, &release_locks);
  mdl_thread.start();
  lock_grabbed.wait_for_notification();

  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED);

  // We should *not* be able to grab the lock here.
  EXPECT_FALSE(m_mdl_context.try_acquire_lock(&m_request));
  EXPECT_EQ(m_null_ticket, m_request.ticket);

  release_locks.notify();

  // The other thread should eventually release its locks.
  EXPECT_FALSE(m_mdl_context.acquire_lock(&m_request, long_timeout));
  EXPECT_NE(m_null_ticket, m_request.ticket);

  mdl_thread.join();
  m_mdl_context.release_all_locks_for_name(m_request.ticket);
}


/*
  Verifies the following scenario:
  Thread 1: grabs a shared upgradable lock.
  Thread 2: grabs a shared lock.
  Thread 1: asks for an upgrade to exclusive (needs to wait for thread 2)
  Thread 2: gets notified, and releases lock.
  Thread 1: gets the exclusive lock.
 */
TEST_F(MDL_test, concurrent_upgrade)
{
  m_request.init(MDL_key::TABLE, db_name, table_name1, MDL_SHARED_NO_WRITE);
  m_request_list.push_front(&m_request);
  m_request_list.push_front(&m_global_request);

  EXPECT_FALSE(m_mdl_context.acquire_locks(&m_request_list, long_timeout));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE,
                            db_name, table_name1, MDL_SHARED_NO_WRITE));
  EXPECT_FALSE(m_mdl_context.
               is_lock_owner(MDL_key::TABLE,
                             db_name, table_name1, MDL_EXCLUSIVE));

  Notification lock_grabbed;
  Notification release_locks;
  MDL_thread mdl_thread(table_name1, MDL_SHARED, &lock_grabbed, &release_locks);
  mdl_thread.start();
  lock_grabbed.wait_for_notification();

  EXPECT_FALSE(m_mdl_context.
               upgrade_shared_lock_to_exclusive(m_request.ticket, long_timeout));
  EXPECT_TRUE(m_mdl_context.
              is_lock_owner(MDL_key::TABLE,
                            db_name, table_name1, MDL_EXCLUSIVE));

  mdl_thread.join();
  m_mdl_context.rollback_to_savepoint(NULL);
}

}  // namespace
