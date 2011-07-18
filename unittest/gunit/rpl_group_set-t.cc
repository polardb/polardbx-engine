/* Copyright (c) 2011, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */


#include <gtest/gtest.h>
#define FRIEND_OF_GROUP_SET class GroupTest_Group_containers_Test
#define FRIEND_OF_GROUP_CACHE class GroupTest_Group_containers_Test
#define FRIEND_OF_GROUP_LOG_STATE class GroupTest_Group_containers_Test
#include "zgroups.h"
#include <string.h>
#include "sql_class.h"
#include "my_pthread.h"
#include "binlog.h"


#define N_SIDS 16


class GroupTest : public ::testing::Test
{
public:
  static const char *uuids[16];
  rpl_sid sids[16];
  int seed;


  void SetUp()
  {
    seed= (int)time(NULL);
    srand(seed);
    for (int i= 0; i < 16; i++)
      sids[i].parse(uuids[i]);

    verbose= false;
    errtext_stack_pos= 0;
    errtext_stack[0]= 0;
    append_errtext(__LINE__, "seed=%d", seed);
  }


  void TearDown()
  {
  }


  /*
    Test that different, equivalent ways to construct a Group_set give
    the same resulting Group_set.  This is used to test Group_set,
    Sid_map, Group_cache, Group_log_state, and Owned_groups.

    We will generate sets of groups in *stages*.  Each stage is
    divided into 200 *sub-stages*.  In each sub-stage, we randomly
    sample one sub-group from a fixed set of groups.  The fixed set of
    groups consists of groups from 16 different SIDs.  For the Nth SID
    (1 <= N <= 16), the fixed set of groups contains all GNOS from the
    closed interval [N, N - 1 + N * N].  The stage consists of the set
    of groups from the 200 sub-stages.
  */

  #define BEGIN_SUBSTAGE_LOOP(group_test, stage, do_errtext)            \
    (group_test)->push_errtext();                                       \
    for (int substage_i= 0; substage_i < Stage::N_SUBSTAGES; substage_i++) { \
      Substage &substage= (stage)->substages[substage_i];               \
      if (do_errtext)                                                   \
        (group_test)->append_errtext(__LINE__,                          \
                                     "sidno=%d group=%s substage_i=%d", \
                                     substage.sidno, substage.ugid_str, \
                                     substage_i);
  #define END_SUBSTAGE_LOOP(group_test) } group_test->pop_errtext()

  /**
    A substage, i.e., one of the 200 groups.
  */
  struct Substage
  {
    rpl_sidno sidno;
    rpl_gno gno;
    const rpl_sid *sid;
    char sid_str[rpl_sid::TEXT_LENGTH + 1];
    char ugid_str[rpl_sid::TEXT_LENGTH + 1 + MAX_GNO_TEXT_LENGTH + 1];
    bool is_first, is_last, is_auto;
  };

  /**
    A stage, i.e., the 200 randomly generated groups.
  */
  struct Stage
  {
    class GroupTest *group_test;
    Sid_map *sid_map;

    // List of groups added in the present stage.
    static const int N_SUBSTAGES= 200;
    Substage substages[N_SUBSTAGES];

    // Set of groups added in the present stage.
    Group_set set;
    int str_len;
    char *str;

    // The subset of groups that can be added as automatic groups.
    Group_set automatic_groups;
    // The subset of groups that cannot be added as automatic groups.
    Group_set non_automatic_groups;

    Stage(class GroupTest *gt, Sid_map *sm)
      : group_test(gt), sid_map(sm),
        set(sm), str_len(0), str(NULL),
        automatic_groups(sm), non_automatic_groups(sm)
    {
      rpl_sidno max_sidno= sm->get_max_sidno();
      set.ensure_sidno(max_sidno);
      automatic_groups.ensure_sidno(max_sidno);
      non_automatic_groups.ensure_sidno(max_sidno);
    }

    ~Stage() { free(str); }

    /**
      Generate the 200 random groups that constitute a stage.

      @param done_groups The set of all groups added in previous
      stages.
      @param other_sm Sid_map to which groups should be added.
    */
    void new_stage(const Group_set *done_groups, Sid_map *other_sm)
    {
      set.clear();
      automatic_groups.clear();
      non_automatic_groups.clear();

      BEGIN_SUBSTAGE_LOOP(group_test, this, false)
      {
        // generate random UGID
        substage.sidno= 1 + (rand() % N_SIDS);
        substage.gno=
          1 + (rand() % (substage.sidno * substage.sidno));
        // compute alternative forms
        substage.sid= sid_map->sidno_to_sid(substage.sidno);
        ASSERT_NE((rpl_sid *)NULL, substage.sid) << group_test->errtext;
        substage.sid->to_string(substage.sid_str);
        substage.sid->to_string(substage.ugid_str);
        substage.ugid_str[rpl_sid::TEXT_LENGTH]= ':';
        format_gno(substage.ugid_str + rpl_sid::TEXT_LENGTH + 1, substage.gno);

        ASSERT_LE(1, other_sm->add_permanent(substage.sid))
          << group_test->errtext;

        // check if this group could be added as an 'automatic' group
        Group_set::Const_interval_iterator ivit(done_groups, substage.sidno);
        const Group_set::Interval *iv= ivit.get();
        substage.is_auto=
          !set.contains_group(substage.sidno, substage.gno) &&
          (((iv == NULL || iv->start > 1) ? substage.gno == 1 :
            substage.gno == iv->end));
        //printf("is_auto=%d\n", substage.is_auto);

        // check if this sub-group is the first in its group in this
        // stage, and add it to the set
        if (set.contains_group(substage.sidno, substage.gno))
          substage.is_first= true;
        else
          set.add(substage.ugid_str);
      } END_SUBSTAGE_LOOP(group_test);

      // Iterate backwards so that we can detect when a subgroup is
      // the last subgroup of its group.
      set.clear();
      for (int substage_i= N_SUBSTAGES - 1; substage_i >= 0; substage_i--)
      {
        substages[substage_i].is_last=
          !set.contains_group(substages[substage_i].sidno,
                              substages[substage_i].gno);
        set.add(substages[substage_i].ugid_str);
      }

      str_len= set.get_string_length();
      str= (char *)realloc(str, str_len + 1);
      set.to_string(str);
    }
  };


  /*
    We maintain a text that contains the state of the test.  We print
    this text when a test assertion fails.  The text is updated each
    iteration of each loop, so that we can easier track the exact
    point in time when an error occurs.  Since loops may be nested, we
    maintain a stack of offsets in the error text: before a new loop
    is entered, the position of the end of the string is pushed to the
    stack and the text appended in each iteration is added to that
    position.
  */
  char errtext[1000];
  int errtext_stack[100];
  int errtext_stack_pos;
  bool verbose;

  void append_errtext(int line, const char *fmt, ...) ATTRIBUTE_FORMAT(printf, 3, 4)
  {
    va_list argp;
    va_start(argp, fmt);
    vsprintf(errtext + errtext_stack[errtext_stack_pos], fmt, argp);
    if (verbose)
      printf("@line %d: %s\n", line, errtext);
    va_end(argp);
  }

  void push_errtext()
  {
    int old_len= errtext_stack[errtext_stack_pos];
    int len= old_len + strlen(errtext + old_len);
    strcpy(errtext + len, " | ");
    errtext_stack[++errtext_stack_pos]= len + 3;
  }

  void pop_errtext()
  {
    errtext[errtext_stack[errtext_stack_pos--] - 3]= 0;
  }
};


const char *GroupTest::uuids[16]=
{
  "00000000-0000-0000-0000-000000000000",
  "11111111-1111-1111-1111-111111111111",
  "22222222-2222-2222-2222-222222222222",
  "33333333-3333-3333-3333-333333333333",
  "44444444-4444-4444-4444-444444444444",
  "55555555-5555-5555-5555-555555555555",
  "66666666-6666-6666-6666-666666666666",
  "77777777-7777-7777-7777-777777777777",
  "88888888-8888-8888-8888-888888888888",
  "99999999-9999-9999-9999-999999999999",
  "aaaaAAAA-aaaa-AAAA-aaaa-aAaAaAaAaaaa",
  "bbbbBBBB-bbbb-BBBB-bbbb-bBbBbBbBbbbb",
  "ccccCCCC-cccc-CCCC-cccc-cCcCcCcCcccc",
  "ddddDDDD-dddd-DDDD-dddd-dDdDdDdDdddd",
  "eeeeEEEE-eeee-EEEE-eeee-eEeEeEeEeeee",
  "ffffFFFF-ffff-FFFF-ffff-fFfFfFfFffff",
};


TEST_F(GroupTest, Uuid)
{
  Uuid u;
  char buf[100];

  // check that we get back the same UUID after parse + print
  for (int i= 0; i < N_SIDS; i++)
  {
    EXPECT_EQ(GS_SUCCESS, u.parse(uuids[i])) << "i=" << i;
    u.to_string(buf);
    EXPECT_STRCASEEQ(uuids[i], buf) << "i=" << i;
  }
  // check error cases
  EXPECT_EQ(GS_SUCCESS, u.parse("ffffFFFF-ffff-FFFF-ffff-ffffffffFFFFf"));
  EXPECT_EQ(GS_ERROR_PARSE, u.parse("ffffFFFF-ffff-FFFF-ffff-ffffffffFFFg"));
  EXPECT_EQ(GS_ERROR_PARSE, u.parse("ffffFFFF-ffff-FFFF-ffff-ffffffffFFF"));
  EXPECT_EQ(GS_ERROR_PARSE, u.parse("ffffFFFF-ffff-FFFF-fff-fffffffffFFFF"));
  EXPECT_EQ(GS_ERROR_PARSE, u.parse("ffffFFFF-ffff-FFFF-ffff-ffffffffFFF-"));
  EXPECT_EQ(GS_ERROR_PARSE, u.parse(" ffffFFFF-ffff-FFFF-ffff-ffffffffFFFF"));
  EXPECT_EQ(GS_ERROR_PARSE, u.parse("ffffFFFFfffff-FFFF-ffff-ffffffffFFFF"));
}


TEST_F(GroupTest, Sid_map)
{
  Checkable_rwlock lock;
  Sid_map sm(&lock);

  // Add a random SID until we have N_SID SIDs in the map.
  lock.rdlock();
  while (sm.get_max_sidno() < N_SIDS)
    sm.add_permanent(&sids[rand() % N_SIDS]);

  // Check that all N_SID SIDs are in the map, and that
  // get_sorted_sidno() has the correct order.  This implies that no
  // SID was added twice.
  for (int i= 0; i < N_SIDS; i++)
  {
    rpl_sidno sidno= sm.get_sorted_sidno(i);
    const rpl_sid *sid;
    char buf[100];
    EXPECT_NE((rpl_sid *)NULL, sid= sm.sidno_to_sid(sidno)) << errtext;
    const int max_len= Uuid::TEXT_LENGTH;
    EXPECT_EQ(max_len, sid->to_string(buf)) << errtext;
    EXPECT_STRCASEEQ(uuids[i], buf) << errtext;
    EXPECT_EQ(sidno, sm.sid_to_sidno(sid)) << errtext;
  }
  lock.unlock();
}


TEST_F(GroupTest, Group_containers)
{
  /*
    In this test, we maintain 298 Group_sets.  We add groups to these
    Group_sets in stages, as described above.  We add the groups to
    each of the 298 Group_sets in different ways, as described below.
    At the end of each stage, we check that all the 298 resulting
    Group_sets are mutually equal.

    We add groups in the two ways:

    A. Test Group_sets and Sid_maps.  We vary two parameters:

       Parameter 1: vary the way that groups are added:
        0. Add one group at a time, using add(sidno, gno).
        1. Add one group at a time, using add(text).
        2. Add all new groups at once, using add(gs_new).
        3. add all new groups at once, using add(gs_new.to_string()).
        4. Maintain a string that contains the concatenation of all
           gs_new.to_string(). in each stage, we set gs[4] to a new
           Group_set created from this string.

       Parameter 2: vary the Sid_map object:
        0. Use a Sid_map that has all the SIDs in order.
        1. Use a Sid_map where SIDs are added in the order they appear.

       We vary these parameters in all combinations; thus we construct
       10 Group_sets.
  */
  enum enum_sets_method {
    METHOD_SIDNO_GNO= 0, METHOD_GROUP_TEXT,
    METHOD_GROUP_SET, METHOD_GROUP_SET_TEXT, METHOD_ALL_TEXTS_CONCATENATED,
    MAX_METHOD
  };
  enum enum_sets_sid_map {
    SID_MAP_0= 0, SID_MAP_1, MAX_SID_MAP
  };
  const int N_COMBINATIONS_SETS= MAX_METHOD * MAX_SID_MAP;
  /*
    B. Test Group_cache, Group_log_state, and Owned_groups.  All
       sub-groups for the stage are added to the Group_cache, the
       Group_cache is flushed to the Group_log_state, and the
       Group_set is extracted from the Group_log_state.  We vary the
       following parameters.

       Parameter 1: type of statement:
        0. Transactional replayed statement: add all groups to the
           transaction group cache (which is flushed to a
           Group_log_state at the end of the stage).  Set
           UGID_NEXT_LIST to the list of all groups in the stage.
        1. Non-transactional replayed statement: add all groups to the
           stmt group cache (which is flushed to the Group_log_state
           at the end of each sub-stage).  Set UGID_NEXT_LIST = NULL.
        2. Randomize: for each sub-stage, choose 0 or 1 with 50%
           chance.  Set UGID_NEXT_LIST to the list of all groups in
           the stage.
        3. Automatic groups: add all groups to the stmt group cache,
           but make the group automatic if possible, i.e., if the SID
           and GNO are unlogged and there is no smaller unlogged GNO
           for this SID.  Set UGID_NEXT_LIST = NULL.

       Parameter 2: ended or non-ended sub-groups:
        0. All sub-groups are unended (except automatic sub-groups).
        1. For each group, the last sub-group of the group in the
           stage is ended.  Don't add groups that are already ended in the
           Group_log_state.
        2. For each group in the stage, choose 0 or 1 with 50% chance.

       Parameter 3: dummy or normal sub-group:
        0. Generate only normal (and possibly automatic) sub-groups.
        1. Generate only dummy (and possibly automatic) sub-groups.
        2. Generate only dummy (and possibly automatic) sub-groups.
           Add the sub-groups implicitly: do not call
           add_dummy_subgroup(); instead rely on
           ugid_before_flush_trx_cache() to add dummy subgroups.
        3. Choose 0 or 1 with 33% chance.

       Parameter 4: insert anonymous sub-groups or not:
        0. Do not generate anonymous sub-groups.
        1. Generate an anomous sub-group before each sub-group with
           50% chance and an anonymous group after each sub-group with
           50% chance.

       We vary these parameters in all combinations; thus we construct
       4*3*4*2=96 Group_sets.
  */
  enum enum_caches_type
  {
    TYPE_TRX= 0, TYPE_NONTRX, TYPE_RANDOMIZE, TYPE_AUTO, MAX_TYPE
  };
  enum enum_caches_end
  {
    END_OFF= 0, END_ON, END_RANDOMIZE, MAX_END
  };
  enum enum_caches_dummy
  {
    DUMMY_OFF= 0, DUMMY_ON, DUMMY_IMPLICIT, DUMMY_RANDOMIZE, MAX_DUMMY
  };
  enum enum_caches_anon {
    ANON_OFF= 0, ANON_ON, MAX_ANON
  };
  const int N_COMBINATIONS_CACHES=
    MAX_TYPE * MAX_END * MAX_DUMMY * MAX_ANON;
  const int N_COMBINATIONS= N_COMBINATIONS_SETS + N_COMBINATIONS_CACHES;

  // Auxiliary macros to loop through all combinations of parameters.
#define BEGIN_LOOP_A                                                    \
  push_errtext();                                                       \
  for (int method_i= 0, combination_i= 0; method_i < MAX_METHOD; method_i++) { \
    for (int sid_map_i= 0; sid_map_i < MAX_SID_MAP; sid_map_i++, combination_i++) { \
      Group_set &group_set __attribute__((unused))=                     \
        containers[combination_i]->group_set;                           \
      Sid_map *&sid_map __attribute__((unused))=                        \
        sid_maps[sid_map_i];                                            \
      append_errtext(__LINE__,                                          \
                     "sid_map_i=%d method_i=%d combination_i=%d",       \
                     sid_map_i, method_i, combination_i);

#define END_LOOP_A } } pop_errtext()

#define BEGIN_LOOP_B                                                    \
  push_errtext();                                                       \
  for (int type_i= 0, combination_i= N_COMBINATIONS_SETS;               \
       type_i < MAX_TYPE; type_i++) {                                   \
    for (int end_i= 0; end_i < MAX_END; end_i++) {                      \
      for (int dummy_i= 0; dummy_i < MAX_DUMMY; dummy_i++) {            \
        for (int anon_i= 0; anon_i < MAX_ANON; anon_i++, combination_i++) { \
          /*if (combination_i != 82) continue;*/                        \
          Group_set &group_set __attribute__((unused))=                 \
            containers[combination_i]->group_set;                       \
          Group_cache &stmt_cache __attribute__((unused))=              \
            containers[combination_i]->stmt_cache;                      \
          Group_cache &trx_cache __attribute__((unused))=               \
            containers[combination_i]->trx_cache;                       \
          Group_log_state &group_log_state __attribute__((unused))=     \
            containers[combination_i]->group_log_state;                 \
          append_errtext(__LINE__,                                      \
                         "type_i=%d end_i=%d dummy_i=%d "               \
                         "anon_i=%d combination_i=%d",                  \
                         type_i, end_i, dummy_i,                        \
                         anon_i, combination_i);

#define END_LOOP_B } } } } pop_errtext()

  // Create Sid_maps.
  Checkable_rwlock &lock= mysql_bin_log.sid_lock;
  Sid_map **sid_maps= new Sid_map*[2];
  for (int i= 0; i < 2; i++)
    sid_maps[i]= new Sid_map(&lock);
  /*
    Make sid_maps[0] and sid_maps[1] different: sid_maps[0] is
    generated in order; sid_maps[1] is generated in the order that
    SIDS are inserted in the Group_set.
  */
  lock.rdlock();
  for (int i= 0; i < N_SIDS; i++)
    ASSERT_LE(1, sid_maps[0]->add_permanent(&sids[i])) << errtext;

  // Create list of container objects.  These are the objects that we
  // test.
  struct Containers
  {
    Group_set group_set;
    Group_cache stmt_cache;
    Group_cache trx_cache;
    Group_log_state group_log_state;
    Containers(Checkable_rwlock *lock, Sid_map *sm)
      : group_set(sm), group_log_state(lock, sm)
    {
      group_log_state.ensure_sidno();
    };
  };
  Containers **containers= new Containers*[N_COMBINATIONS];
  BEGIN_LOOP_A
  {
    containers[combination_i]= new Containers(&lock, sid_map);
  } END_LOOP_A;
  BEGIN_LOOP_B
  {
    containers[combination_i] = new Containers(&lock, sid_maps[0]);
  } END_LOOP_B;

  //verbose= true;

  /*
    Construct a Group_set that contains the set of all groups from
    which we sample.
  */
  enum_group_status error;
  static char all_groups_str[100*100];
  char *s= all_groups_str;
  s += sprintf(s, "%s:1", uuids[0]);
  for (rpl_sidno sidno= 2; sidno <= N_SIDS; sidno++)
    s += sprintf(s, ",\n%s:1-%d", uuids[sidno - 1], sidno * sidno);
  Group_set all_groups(sid_maps[0], all_groups_str, &error);
  EXPECT_EQ(GS_SUCCESS, error) << errtext;

  // The set of groups that were added in some previous stage.
  Group_set done_groups(sid_maps[0]);
  done_groups.ensure_sidno(sid_maps[0]->get_max_sidno());

  /*
    Iterate through stages. In each stage, create the "stage group
    set" by taking 200 randomly sampled subgroups.  Add this stage
    group set to each of the group sets in different ways.  Stop when
    the union of all stage group sets is equal to the full set from
    which we took the samples.
  */
  char *done_str= NULL;
  int done_str_len= 0;
  Stage stage(this, sid_maps[0]);
  int stage_i= 0;

  /*
    We need a THD object only to read THD::variables.ugid_next,
    THD::variables.ugid_end, THD::variables.ugid_next_list,
    THD::thread_id, THD::server_status.  We don't want to invoke the
    THD constructor because that would require setting up mutexes,
    etc.  Hence we use malloc instead of new.
  */
  THD *thd= (THD *)malloc(sizeof(THD));
  ASSERT_NE((THD *)NULL, thd) << errtext;
  Ugid_specification *ugid_next= &thd->variables.ugid_next;
  thd->thread_id= 4711;
  ugid_next->type= Ugid_specification::AUTOMATIC;
  my_bool &ugid_end= thd->variables.ugid_end;
  my_bool &ugid_commit= thd->variables.ugid_commit;
  thd->server_status= 0;
  thd->system_thread= NON_SYSTEM_THREAD;
  thd->variables.ugid_next_list.group_set= &stage.set;

  push_errtext();
  while (!all_groups.equals(&done_groups))
  {
    stage_i++;
    append_errtext(__LINE__, "stage_i=%d", stage_i);
    stage.new_stage(&done_groups, sid_maps[1]);

    // Create a string that contains all previous stage.str,
    // concatenated.
    done_str= (char *)realloc(done_str,
                              done_str_len + 1 + stage.str_len + 1);
    ASSERT_NE((char *)NULL, done_str) << errtext;
    done_str_len += sprintf(done_str + done_str_len, ",%s", stage.str);

    // Add groups to Group_sets.
    BEGIN_LOOP_A
    {
      switch (method_i)
      {
      case METHOD_SIDNO_GNO:
        BEGIN_SUBSTAGE_LOOP(this, &stage, true)
        {
          rpl_sidno sidno_1= sid_map->sid_to_sidno(substage.sid);
          EXPECT_NE(0, sidno_1) << errtext;
          group_set.add(sidno_1, substage.gno);
        } END_SUBSTAGE_LOOP(this);
        break;
      case METHOD_GROUP_TEXT:
        BEGIN_SUBSTAGE_LOOP(this, &stage, true)
        {
          group_set.add(substage.ugid_str);
        } END_SUBSTAGE_LOOP(this);
        break;
      case METHOD_GROUP_SET:
        EXPECT_EQ(GS_SUCCESS, group_set.add(&stage.set)) << errtext;
        break;
      case METHOD_GROUP_SET_TEXT:
        EXPECT_EQ(GS_SUCCESS, group_set.add(stage.str)) << errtext;
        break;
      case METHOD_ALL_TEXTS_CONCATENATED:
        group_set.clear();
        error= group_set.add(done_str);
        EXPECT_EQ(GS_SUCCESS, error) << errtext;
      case MAX_METHOD:
        break;
      }
    } END_LOOP_A;

    // Add groups to Group_caches.
    BEGIN_LOOP_B
    {
#define END_SUPER_GROUP                                                 \
      {                                                                 \
        lock.unlock();                                                  \
        ugid_before_flush_trx_cache(thd, &lock, &group_log_state, &trx_cache); \
        ugid_flush_group_cache(thd, &lock, &group_log_state, &trx_cache); \
        /* Here we would like to call ugid_after_flush_trx_cache, */    \
        /* but that would invoke trans_commit which does not work */    \
        /* when the server is not running.  So instead we simulate */   \
        /* the call by clearing the */                                  \
        /* SERVER_STATUS_IN_MASTER_SUPER_GROUP flag. */                 \
        thd->server_status &= ~SERVER_STATUS_IN_MASTER_SUPER_GROUP;     \
        lock.rdlock();                                                  \
      }

      Group_set ended_groups(sid_maps[0]);
      BEGIN_SUBSTAGE_LOOP(this, &stage, true)
      {
        int type_j;
        if (type_i == TYPE_RANDOMIZE)
          type_j= rand() % 2;
        else if (type_i == TYPE_AUTO && !substage.is_auto)
          type_j= TYPE_NONTRX;
        else
          type_j= type_i;
        int end_j;
        if (substage.is_first &&
            ((end_i == END_RANDOMIZE && (rand() % 2)) ||
             end_i == END_ON))
          ended_groups.add(substage.sidno, substage.gno);
        end_j= substage.is_last &&
          ended_groups.contains_group(substage.sidno, substage.gno);
        /*
          In DUMMY_RANDOMIZE mode, we have to determine once *per
          group* (not substage) if we use DUMMY_END or not. So we
          determine this for the first subgroup of the group, and then
          we memoize which groups use DUMMY_END using the Group_set
          dummy_end.
        */
        int dummy_j;
        if (dummy_i == DUMMY_RANDOMIZE)
          dummy_j= rand() % 3;
        else
          dummy_j= dummy_i;
        int anon_j1, anon_j2;
        if (type_j != TYPE_TRX || anon_i == ANON_OFF)
          anon_j1= anon_j2= ANON_OFF;
        else
        {
          anon_j1= rand() % 2;
          anon_j2= rand() % 2;
        }

        thd->variables.ugid_next_list.is_non_null=
          (type_i == TYPE_NONTRX || type_i == TYPE_AUTO) ? 0 : 1;
        ugid_commit=
          (substage_i == Stage::N_SUBSTAGES - 1) ||
          !thd->variables.ugid_next_list.is_non_null;

        if (type_j == TYPE_AUTO)
        {
          ugid_next->type= Ugid_specification::AUTOMATIC;
          ugid_next->group.sidno= substage.sidno;
          ugid_next->group.gno= 0;
          ugid_end= false;
          lock.unlock();
          ugid_before_statement(thd, &lock, &group_log_state,
                                &stmt_cache, &trx_cache);
          lock.rdlock();
          stmt_cache.add_logged_subgroup(thd, 20 + rand() % 100/*binlog_len*/);
        }
        else
        {
          Group_cache &cache= type_j == TYPE_TRX ? trx_cache : stmt_cache;

          if (anon_j1)
          {
            ugid_next->type= Ugid_specification::ANONYMOUS;
            ugid_end= false;
            lock.unlock();
            ugid_before_statement(thd, &lock, &group_log_state,
                                  &stmt_cache, &trx_cache);
            lock.rdlock();
            cache.add_logged_subgroup(thd, 20 + rand() % 100/*binlog_len*/);
          }

          ugid_next->type= Ugid_specification::UGID;
          ugid_next->group.sidno= substage.sidno;
          ugid_next->group.gno= substage.gno;
          ugid_end= (end_j == END_ON) ? true : false;
          lock.unlock();
          ugid_before_statement(thd, &lock, &group_log_state,
                                &stmt_cache, &trx_cache);
          lock.rdlock();
          if (!group_log_state.is_ended(substage.sidno, substage.gno))
          {
            switch (dummy_j)
            {
            case DUMMY_OFF:
              cache.add_logged_subgroup(thd, 20 + rand() % 100/*binlog_len*/);
              break;
            case DUMMY_ON:
              cache.add_dummy_subgroup(substage.sidno, substage.gno,
                                       end_j ? true : false);
              break;
            case DUMMY_IMPLICIT:
              break; // do nothing
            default:
              assert(0);
            }
          }

          if (anon_j2)
          {
            ugid_next->type= Ugid_specification::ANONYMOUS;
            ugid_end= false;
            lock.unlock();
            ugid_before_statement(thd, &lock, &group_log_state,
                                  &stmt_cache, &trx_cache);
            lock.rdlock();
            cache.add_logged_subgroup(thd, 20 + rand() % 100/*binlog_len*/);
          }
        }

        ugid_flush_group_cache(thd, &lock, &group_log_state,
                               &stmt_cache, &trx_cache);
        ugid_before_flush_trx_cache(thd, &lock, &group_log_state, &trx_cache);
        if (ugid_commit)
        {
          // simulate ugid_after_flush_trx_cache() but don't
          // execute a COMMIT statement
          thd->server_status &= ~SERVER_STATUS_IN_MASTER_SUPER_GROUP;
          ugid_flush_group_cache(thd, &lock, &group_log_state,
                                 &trx_cache, &trx_cache);
        }
      } END_SUBSTAGE_LOOP(this);

      group_set.clear();
      group_log_state.owned_groups.get_partial_set(&group_set);
      group_set.add(&group_log_state.ended_groups);
    } END_LOOP_B;

    done_groups.add(&stage.set);

    /*
      Verify that all group sets are equal.  We test both a.equals(b)
      and b.equals(a) and a.equals(a), because we want to verify that
      the equals function is correct too.  We compare both the sets
      using Group_set::equals, and the output of to_string() using
      EXPECT_STREQ.
    */
    BEGIN_LOOP_A
    {
      char *buf1= new char[group_set.get_string_length() + 1];
      group_set.to_string(buf1);
      for (int i= 0; i < N_COMBINATIONS_SETS; i++)
      {
        Group_set &group_set_2= containers[i]->group_set;
        ASSERT_EQ(true, group_set.equals(&group_set_2)) << errtext << " i=" << i;
        if (combination_i < i)
        {
          char *buf2= new char[group_set_2.get_string_length() + 1];
          group_set_2.to_string(buf2);
          ASSERT_STREQ(buf1, buf2) << errtext << " i=" << i;
          delete(buf2);
        }
      }
      delete(buf1);
    } END_LOOP_A;
    BEGIN_LOOP_B
    {
      //containers[combination_i]->group_set.print();
      ASSERT_EQ(true, containers[combination_i]->group_set.equals(&done_groups)) << errtext;
    } END_LOOP_B;
  }
  pop_errtext();

  // Finally, verify that the string representations of
  // done_groups is as expected
  static char buf[100*100];
  done_groups.to_string(buf);
  EXPECT_STRCASEEQ(all_groups_str, buf) << errtext;
  lock.unlock();

  // Clean up.
  free(done_str);
  for (int i= 0; i < N_COMBINATIONS; i++)
    delete containers[i];
  delete containers;
  for (int i= 0; i < 2; i++)
    delete sid_maps[i];
  free(thd);
}
