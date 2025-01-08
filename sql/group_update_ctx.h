/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  group_update_ctx.h,v 1.0 02/23/2016 03:01:53 PM
 *       yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file group_update_ctx.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 02/23/2016 03:01:53 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef group_update_ctx_INC
#define group_update_ctx_INC

class THD;
class GroupUpdate;

/**
 * @class GroupUpdateCtx
 *
 * @brief context of group update in THD
 *
 **/
class GroupUpdateCtx {
 public:
  GroupUpdateCtx()
      : idx(2),
        role_(ROLE_NOT_GU),
        leader_(nullptr),
        gu_(nullptr),
        n_user_(0),
        need_release_gu_(false) {}

  virtual ~GroupUpdateCtx() {}

  void reset() {
    role_ = ROLE_NOT_GU;
    need_release_gu_ = false;
    // leader_ = nullptr;
    // gu_ = nullptr;
    // n_user_ = 0;
  }

  bool is_not_gu() const { return role_ == ROLE_NOT_GU; }
  bool is_gu() const { return role_ != ROLE_NOT_GU; }
  void tag_leader() { role_ = ROLE_LEADER; }
  bool is_leader() const { return role_ == ROLE_LEADER; }
  void tag_follower() { role_ = ROLE_FOLLOWER; }
  bool is_follower() const { return role_ == ROLE_FOLLOWER; }
  void set_ctx(GroupUpdate *gu_arg, bool is_leader, uint idx_arg,
               ulonglong version_arg, ulonglong reuse_version_arg) {
    gu_ = gu_arg;
    role_ = is_leader ? ROLE_LEADER : ROLE_FOLLOWER;
    idx = idx_arg;
    version = version_arg;
    reuse_version = reuse_version_arg;
  }

  void set_leader(THD *thd) { leader_ = thd; }
  GroupUpdate *get_gu() { return gu_; }
  void set_user(ulonglong n) { n_user_ = n; }
  void set_need_release_gu(bool need_release) {
    need_release_gu_ = need_release;
  }
  bool need_release_gu() { return need_release_gu_; }

 public:
  uint idx;
  ulonglong version;
  ulonglong reuse_version;

 protected:
  enum RoleType { ROLE_NOT_GU = -1, ROLE_LEADER = 0, ROLE_FOLLOWER = 1 };
  /* not gu:-1 leader:0 follower:1 */
  enum RoleType role_;
  THD *leader_;
  GroupUpdate *gu_;
  ulonglong n_user_;
  bool need_release_gu_;

 private:
  GroupUpdateCtx(const GroupUpdateCtx &other);  // copy constructor
  const GroupUpdateCtx &operator=(
      const GroupUpdateCtx &other);  // assignment operator
};

#endif  // group_update_ctx_INC
