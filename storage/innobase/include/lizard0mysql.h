
#ifndef lizard0mysql_h
#define lizard0mysql_h

#include "lizard0gcs0read0types.h"
#include "sql/transaction/komodo0snapshot.h"

struct row_prebuilt_t;

namespace lizard {

/** The max tolerable lease time of a snapshot */
extern ulint srv_scn_valid_volumn;

class Vision;

/**
  as of query context in row_prebuilt_t
*/
struct asof_query_context_t {
  /** If it has been set, only set once */
  bool m_is_assigned;

  /** Snapshot vision that is used by asof query. */
  komodo::Snapshot_vision *m_snapshot_vision;

  /** TODO: ban constructor <16-10-20, zanye.zjy> */
  explicit asof_query_context_t()
      : m_is_assigned(false), m_snapshot_vision(nullptr) {}

  ~asof_query_context_t() { release_vision(); }

  bool is_assigned() { return m_is_assigned; }

  bool is_asof_query() const { return m_snapshot_vision != nullptr; }

  komodo::Snapshot_vision *snapshot_vision() {
    return m_snapshot_vision;
  }

  bool is_asof_scn() const {
    return m_snapshot_vision &&
           m_snapshot_vision->type() == komodo::Snapshot_type::AS_OF_SCN;
  }
  bool is_asof_gcn() const {
    return m_snapshot_vision &&
           (m_snapshot_vision->type() ==
                komodo::Snapshot_type::AS_OF_AUTOMATIC_GCN ||
            m_snapshot_vision->type() ==
                komodo::Snapshot_type::AS_OF_ASSIGNED_GCN);
  }

  void assign_vision(komodo::Snapshot_vision *v) {
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
  A helper class: backup trx->vision.m_snapshot_scn and restores it.
*/
class AsofVisonWrapper {
 public:
  AsofVisonWrapper() : m_vision(nullptr) {}

  ~AsofVisonWrapper() { release_snapshot_vision(); }

  void trx_store_snapshot_vision(row_prebuilt_t *prebuilt);

  void release_snapshot_vision();

 private:
  Vision *m_vision;
};

/**
  Try to convert flashback query in mysql to as-of query in innodb.

  @param[in/out]    prebuilt    row_prebuilt_t::m_scn_query could be modified

  @return           dberr_t     DB_SUCCESS if convert successfully or it's not
                                a flashback query.
                                ERROR: DB_AS_OF_INTERNAL,
  DB_SNAPSHOT_OUT_OF_RANGE, DB_AS_OF_TABLE_DEF_CHANGED
*/
dberr_t row_prebuilt_bind_flashback_query(row_prebuilt_t *prebuilt);

/**
  Reset row_prebuilt_t::m_scn_query, query block level.

  @param[in/out]    prebuilt    row_prebuilt_t

  @return           dberr_t     DB_SUCCESS, or DB_SNAPSHOT_OUT_OF_RANGE.
*/
dberr_t row_prebuilt_unbind_flashback_query(row_prebuilt_t *prebuilt);

/**
  Get Snapshot_vison from prebuilt

  @param[in]        prebuilt    row_prebuilt_t

  @return           snapshot_vision if has,
                    nullptr if hasn't
*/
extern komodo::Snapshot_vision *row_prebuilt_get_snapshot_vision(
    row_prebuilt_t *prebuilt);

extern int convert_timestamp_to_scn(THD *thd, utc_t utc, scn_t *scn);
}  // namespace lizard
#endif
