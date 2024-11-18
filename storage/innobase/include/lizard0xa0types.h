/*****************************************************************************

Copyright (c) 2013, 2021, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0xa0types.h
  Lizard XA transaction structure.

 Created 2021-08-10 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0xa0types_h
#define lizard0xa0types_h

#include <string>
#include <unordered_set>
#include "trx0types.h"
#include "trx0xa.h"
#include "ut0guarded.h"

class Xa_group;

/**
  The description of XA transaction within trx_t
*/
struct xa_desc_t {
 public:
  xa_desc_t() : m_xid(), m_gid(), m_group(nullptr) {}
  virtual ~xa_desc_t() { reset(); }

  void reset() {
    m_xid.null();
    m_gid.clear();
    m_group = nullptr;
  }

  bool is_xid_null() const { return m_xid.is_null(); }

  bool is_group_null() const { return m_group == nullptr; }

  const XID *xid() const { return &m_xid; }

  void copy_xid(const XID *xid) { m_xid = *xid; }

  const std::string &gid() const { return m_gid; }

  bool build_gid();

  Xa_group *group() { return m_group; }

  const Xa_group *group() const { return m_group; }

  void set_group(Xa_group *group) { m_group = group; }

 private:
  /** Whether valid is determined by xid.is_null() */
  XID m_xid;

  /** Build group in advance to improve search. */
  std::string m_gid;

  /** Xa transaction group. */
  Xa_group *m_group;

  /** Use format v1 for 0 - 9999. */
  static constexpr lint FORMAT_V1_RANGE = 9999;

  /** To distinguish between different versions, format version number
  is appended to the end of group id. Notice that the format version
  number should have the same length. */
  static constexpr char FORMAT_V1[] = "1";
  static constexpr char FORMAT_V2[] = "2";

  bool build_gid_v1();

  bool build_gid_v2();

  /** Bqual format: 'xxx@nnnn' */
  static constexpr unsigned int XID_GROUP_SUFFIX_SIZE_V1 = 5;

  static constexpr char XID_GROUP_SPLIT_CHAR_V1 = '@';

  bool check_if_match_format_v1(const XID *xid);
};

/** Group of transactions which are diferent branches of the same XA
transaction. */
class Xa_group {
  using XaGroupMutex = ib_mutex_t;

 public:
  Xa_group() : m_trx_ids(), m_fix_count(0), m_modified_clock(0), m_closed(false) {
    mutex_create(LATCH_ID_TRX_SYS_GROUP, &m_mutex);
  }

  virtual ~Xa_group() {
    ut_ad(m_fix_count.load(std::memory_order_relaxed) == 0);

    m_trx_ids.clear();
    m_fix_count = 0;
    m_modified_clock = 0;
    m_closed = true;

    mutex_free(&m_mutex);
  }

  bool is_referenced() const {
    auto fix_count_val = m_fix_count.load(std::memory_order_relaxed);
    ut_ad(fix_count_val >= 0);
    return fix_count_val > 0;
  }

  /** Reference the Xa Group. */
  void acquire() { m_fix_count.fetch_add(1, std::memory_order_relaxed); }

  /** Release the reference of the Xa Group.
  @return true if nobody is referencing the Xa Group. */
  bool release() {
    lint fix_count_val = m_fix_count.fetch_sub(1, std::memory_order_relaxed);
    return fix_count_val == 1;
  }

  /** Insert new trx id into group */
  void insert(const trx_id_t &trx_id) {
    mutex_enter(&m_mutex);

    m_trx_ids.emplace(trx_id);
    if (++m_modified_clock == 0) ++m_modified_clock;

    mutex_exit(&m_mutex);
  }

  /** Clone trx ids into container and set clock if modified
   *
   * @param[in/out]		container
   * @param[in/out]		clock
   * */
  template <typename Container>
  void clone(Container &ids, ulint &clock) {
    mutex_enter(&m_mutex);
    if (clock != m_modified_clock) {
      for (auto const id : m_trx_ids) {
        ids.insert(id);
      }
      clock = m_modified_clock;
    }
    mutex_exit(&m_mutex);
  }

  bool is_closed() const { return m_closed.load(std::memory_order_relaxed); }

  void close() {
    if (!m_closed.load(std::memory_order_relaxed))
      m_closed.store(true, std::memory_order_relaxed);
  }

 private:
  std::unordered_set<trx_id_t> m_trx_ids;
  std::atomic<lint> m_fix_count;

  /** A sequence number used to count the number of Xa Group modified.
  It is only set 0 when the Xa_group is first created, otherwise just
  skip 0 to avoid possible vision loss when overflow as the
  lizard::vision->group_clock is initialized to 0.
  */
  ulint m_modified_clock;

  /** Set true if there is at least one branch transaction that has been
  prepared, i.e. TRX_STATE_PREPARED. It will prevent starting new XA
  transaction branch. */
  std::atomic<bool> m_closed;

  /** Mutex protecting the members above, except atomic variables. */
  XaGroupMutex m_mutex;
};

class Xa_group_by_id {
  using By_id = std::unordered_map<std::string, Xa_group *>;

  By_id m_by_id;

 public:
  By_id const &by_id() const { return m_by_id; }

  /** Get Xa_group by id.
  @param[in]  id  id of xa group.
  @param[in]  create_if_not_exist  whether to create if not exist.
  @return xa group.*/
  Xa_group *get(const std::string &id, bool create_if_not_exist = false) {
    const auto it = m_by_id.find(id);
    Xa_group *xa_group = (it == m_by_id.end()) ? nullptr : it->second;
    if (create_if_not_exist && xa_group == nullptr) {
      xa_group = insert(id);
    }
    return xa_group;
  }

  Xa_group *insert(const std::string &id) {
    ut_ad(0 == m_by_id.count(id));
    Xa_group *xa_group = ut::new_withkey<Xa_group>(UT_NEW_THIS_FILE_PSI_KEY);

    m_by_id.emplace(id, xa_group);
    return xa_group;
  }

  void erase(const std::string &id) {
    ut_ad(1 == m_by_id.count(id));
    ut_ad(!m_by_id.at(id)->is_referenced());

    auto it = m_by_id.find(id);
    ut::delete_(it->second);

    m_by_id.erase(id);
  }
  ~Xa_group_by_id() { ut_ad(m_by_id.size() == 0); }
};

/** Shard for subset of XA groups. */
struct Xa_group_shard {
  /** Mapping from trx->xa_desc->id to xa group.
  Use latch_and_execute() interface to access other members. */
  ut::Cacheline_padded<
      ut::Guarded<Xa_group_by_id, LATCH_ID_TRX_SYS_GROUP_SHARD>>
      xa_groups;
};

/** Number of shards created for xa transaction group. */
constexpr size_t XA_GROUP_SHARDS_N = 256;

/**
 * Computes shard number of xa_group for a given id.
 * @param[in]  id  id for which shard_no should be computed
 * @return the computed shard number (number in range 0..XA_GROUP_SHARDS_N-1)
 */
static inline size_t trx_get_xa_group_shard_no(const std::string &id) {
  return std::hash<std::string>{}(id) % XA_GROUP_SHARDS_N;
}



#endif
