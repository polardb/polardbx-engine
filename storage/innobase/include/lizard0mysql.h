
#ifndef lizard0mysql_h
#define lizard0mysql_h

#include "lizard0scn.h"
#include "lizard0read0types.h"
#include "sql/lizard/lizard_snapshot.h"

struct row_prebuilt_t;

namespace lizard {

/** Whether to enable use as of query (true by default) */
extern bool srv_force_normal_query_if_fbq;

/** The max tolerable lease time of a snapshot */
extern ulint srv_scn_valid_volumn;

class Vision;
class Snapshot_vision;

/**
  as of query context in row_prebuilt_t
*/
struct asof_query_context_t {
  /** If it has been set, only set once */
  bool m_is_assigned;

  /** Snapshot vision that is used by asof query. */
  Snapshot_vision *m_snapshot_vision;

  /** TODO: ban constructor <16-10-20, zanye.zjy> */
  explicit asof_query_context_t()
      : m_is_assigned(false), m_snapshot_vision(nullptr) {}

  ~asof_query_context_t() { release_vision(); }

  bool is_assigned() { return m_is_assigned; }

  bool is_asof_query() const { return m_snapshot_vision != nullptr; }

  Snapshot_vision *snapshot_vision() const { return m_snapshot_vision; }

  bool is_asof_scn() const {
    return m_snapshot_vision &&
           m_snapshot_vision->type() == Snapshot_type::AS_OF_SCN;
  }
  bool is_asof_gcn() const {
    return m_snapshot_vision &&
           m_snapshot_vision->type() == Snapshot_type::AS_OF_GCN;
  }

  void assign_vision(Snapshot_vision *v) {
    ut_ad(v && v->is_vision());
    m_snapshot_vision = v;
    /** Only set once */
    m_is_assigned = true;
  }

  /** Judge whether snapshot vision is tool old.

      @retval		true	too old
      @retval		false	normal
  */
  bool too_old() { return m_snapshot_vision && m_snapshot_vision->too_old(); }

  void release_vision() {
    m_is_assigned = false;
    m_snapshot_vision = nullptr;
  }
};

/**
  Try to convert flashback query in mysql to as-of query in innodb.

  @param[in/out]    prebuilt    row_prebuilt_t::m_scn_query could be modified

  @return           dberr_t     DB_SUCCESS if convert successfully or it's not
                                a flashback query.
                                ERROR: DB_AS_OF_INTERNAL, DB_SNAPSHOT_OUT_OF_RANGE,
                                DB_AS_OF_TABLE_DEF_CHANGED
*/
dberr_t convert_fbq_ctx_to_innobase(row_prebuilt_t *prebuilt);

/**
  Reset row_prebuilt_t::m_scn_query, query block level.

  @param[in/out]    prebuilt    row_prebuilt_t

  @return           dberr_t     DB_SUCCESS, or DB_SNAPSHOT_OUT_OF_RANGE.
*/
dberr_t reset_prebuilt_flashback_query_ctx(row_prebuilt_t *prebuilt);

/**
  Try get gcn from the variable(innodb_commit_seq). The **thd** might be:
  * trx->mysql_thd. Usually a commit of an ordinary transaction.
  * current_thd. Commit of external XA transactions.

  TODO: This code is just a temporary solution, and will be refactored.

  @params[in]    trx     the transaction

  @return        gcn_t   GCN_NULL if hasn't the gcn
*/
gcn_t trx_mysql_has_gcn(const trx_t *trx);

}
#endif
