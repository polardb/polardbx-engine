/* Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

#include "dd/impl/sdi.h"

#include "my_rapidjson_size_t.h"  // IWYU pragma: keep

#include <rapidjson/document.h>     // rapidjson::GenericValue
#include <rapidjson/prettywriter.h> // rapidjson::PrettyWrite
#include <rapidjson/error/error.h>  // rapidjson::ParseErrorCode
#include <rapidjson/error/en.h>     // rapidjson::GetParseError_En
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <vector>

#include "dd/cache/dictionary_client.h" // dd::Dictionary_client
#include "dd/impl/dictionary_impl.h"    // dd::Dictionary_impl::get_target_dd_version
#include "dd/impl/sdi.h"                // dd::sdi::Import_target
#include "dd/impl/sdi_impl.h"           // sdi read/write functions
#include "dd/impl/sdi_utils.h"          // dd::checked_return
#include "dd/dd.h"                      // dd::create_object
#include "dd/object_id.h"
#include "dd/sdi_file.h"                // dd::sdi_file::store
#include "dd/sdi_fwd.h"
#include "dd/sdi_tablespace.h"          // dd::sdi_tablespace::store
#include "dd/types/abstract_table.h"
#include "dd/types/column.h"            // dd::Column
#include "dd/types/index.h"             // dd::Index
#include "dd/types/schema.h"            // dd::Schema
#include "dd/types/table.h"             // dd::Table
#include "dd/types/tablespace.h"        // dd::Tablespace
#include "dd_sql_view.h"                // update_referencing_views_metadata()

#include "handler.h"              // ha_resolve_by_name_raw
#include "m_string.h"             // STRING_WITH_LEN
#include "mdl.h"
#include "my_dbug.h"
#include "my_global.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql_version.h"        // MYSQL_VERSION_ID
#include "mysqld_error.h"
#include "prealloced_array.h"
#include "rapidjson/stringbuffer.h"
#include "sql_class.h"            // THD
#include "sql_plugin_ref.h"
#include "strfunc.h"              // lex_cstring_handle
#include "table.h"                // TABLE_LIST
#include "template_utils.h"

namespace dd {
class Weak_object;
}  // namespace dd

/**
  @defgroup sdi Serialized Dictionary Information
  @ingroup Runtime_Environment
  @{
  Code to serialize and deserialize data dictionary objects, and for
  storing and retrieving the serialized representation from files or
  tablespaces.

  @file
  Definition of all sdi functions, except those that are -
  (de)serialize() member function in data dictionary objects -
  function templates which are defined in sdi_impl.h

  The file is made up of 4 groups:
  - @ref sdi_cc_internal
  - @ref sdi_internal
  - @ref sdi_api
  - @ref sdi_ut
  @}
*/

/**
  @defgroup sdi_cc_internal TU-internal definitions
  @ingroup sdi
  @{
  Functions and classes internal to the
  translation unit in the anonymous namespace.
*/

using namespace dd::sdi_utils;

namespace {
const dd::String_type empty_= "";

char *generic_buf_handle(Byte_buffer *buf, size_t sz)
{
  if (buf->reserve(sz))
  {
    DBUG_ASSERT(false);
    return nullptr;
  }
  return &(*(buf->begin()));
}

}

/** @} */ // sdi_cc_internal


namespace dd {
/**
  @defgroup sdi_internal SDI Internal
  @ingroup sdi

  Objects internal to sdi-creation, and not callable from general server code.
*/

/**
  Opaque context which keeps reusable resources needed during
  serialization.
*/

class Sdi_wcontext
{
  /** A reusable byte buffer for e.g. base64 encoding. */
  Byte_buffer m_buf;
  /** Thread context */
  THD *m_thd;
  /** Pointer to schema name to use for schema references in SDI */
  const String_type *m_schema_name;

  /** Flag indicating that an error has occured */
  bool m_error;

  friend char *buf_handle(Sdi_wcontext *wctx, size_t sz);

  friend const String_type &lookup_schema_name(Sdi_wcontext *wctx);

  friend const String_type &lookup_tablespace_name(Sdi_wcontext *wctx,
                                                   dd::Object_id id);

public:
  Sdi_wcontext(THD *thd, const String_type *schema_name) :
    m_thd(thd), m_schema_name(schema_name), m_error(false) {}

  bool error() const
  {
    return m_error;
  }
};


char *buf_handle(Sdi_wcontext *wctx, size_t sz)
{
  return generic_buf_handle(&wctx->m_buf, sz);
}


const String_type &lookup_schema_name(Sdi_wcontext *wctx)
{
  return *wctx->m_schema_name;
}

static constexpr std::uint64_t sdi_version= 1;
template <typename T>
String_type generic_serialize(THD *thd, const char *dd_object_type,
                              size_t dd_object_type_size, const T &dd_obj,
                              const String_type *schema_name)
{
  dd::Sdi_wcontext wctx(thd, schema_name);
  dd::RJ_StringBuffer buf;
  dd::Sdi_writer w(buf);

  w.StartObject();
  w.String(STRING_WITH_LEN("mysqld_version_id"));
  w.Uint64(MYSQL_VERSION_ID);

  w.String(STRING_WITH_LEN("dd_version"));
  w.Uint(Dictionary_impl::get_target_dd_version());

  w.String(STRING_WITH_LEN("sdi_version"));
  w.Uint64(sdi_version);

  w.String(STRING_WITH_LEN("dd_object_type"));
  w.String(dd_object_type, dd_object_type_size);

  w.String(STRING_WITH_LEN("dd_object"));
  dd_obj.serialize(&wctx, &w);
  w.EndObject();

  return (wctx.error() ? empty_ : String_type(buf.GetString(), buf.GetSize()));
}


const String_type &lookup_tablespace_name(Sdi_wcontext *wctx, dd::Object_id id)
{
  if (wctx->m_thd == nullptr || id == INVALID_OBJECT_ID)
  {
    return empty_;
  }

  // FIXME: Need to
  // - Check if id already in wctx cache
  // - if not; acquire_uncached, store the (id, name) pair in wctx,
  // - return reference to name corresponding to id in wctx

  dd::cache::Dictionary_client *dc= wctx->m_thd->dd_client();
  dd::cache::Dictionary_client::Auto_releaser releaser(dc);
  const Tablespace *tsp= nullptr;
  if (dc->acquire(id, &tsp))
  {
    wctx->m_error= true;
    return empty_;
  }
  DBUG_ASSERT(tsp != nullptr);

  return tsp->name();
}

/**
  Opaque context which keeps reusable resoureces needed during
  deserialization.
*/

class Sdi_rcontext
{
  /** A reusable byte buffer for e.g. base64 decoding. */
  Byte_buffer buf;

  /** Column objects created during deserialization */
  dd_vector<Column*> m_column_object_opx;

  /** Index objects created during deserialization */
  dd_vector<Index*> m_index_object_opx;

  /** Thread context */
  THD *m_thd;

  /** Target dd version from SDI */
  uint m_target_dd_version;

  /** Sdi version from SDI */
  std::uint64_t m_sdi_version;

  /** Flag indicating that an error has occured */
  bool m_error;

  friend void track_object(Sdi_rcontext *rctx, Column *column_object);
  friend void track_object(Sdi_rcontext *rctx, Index *index_object);

  friend Index *get_by_opx(Sdi_rcontext *rctx, const Index*, uint opx);
  friend Column *get_by_opx(Sdi_rcontext *rctx, const Column*, uint opx);

  friend char *buf_handle(Sdi_rcontext *rctx, size_t sz);

  friend bool lookup_schema_ref(Sdi_rcontext *rctx,
                                const String_type &name, dd::Object_id *idp);
  friend bool lookup_tablespace_ref(Sdi_rcontext *rctx,
                                    const String_type &name, Object_id *idp);

public:
  Sdi_rcontext(THD *thd, uint target_dd_version, std::uint64_t sdi_version) :
    m_thd(thd),
    m_target_dd_version(target_dd_version),
    m_sdi_version(sdi_version),
    m_error(false)
  {}

  String_type m_schema_name;

  bool error() const
  {
    return m_error;
  }
};


template <typename T>
void generic_track_object(dd_vector<T*> *tvp, T *t)
{
  DBUG_ASSERT(t->ordinal_position() > 0);
  uint opx= t->ordinal_position()-1;
  dd_vector<T*> &tv= *tvp;

  if (opx >= tv.size())
  {
    tv.resize(opx+1);
  }
  tv[opx]= t;
}

void track_object(Sdi_rcontext *sdictx, Column *column_object)
{
  generic_track_object(&sdictx->m_column_object_opx, column_object);
}


void track_object(Sdi_rcontext *sdictx, Index *index_object)
{
  generic_track_object(&sdictx->m_index_object_opx, index_object);
}


Index *get_by_opx(dd::Sdi_rcontext *sdictx, const Index*, uint opx)
{
  return sdictx->m_index_object_opx[opx];
}


Column *get_by_opx(dd::Sdi_rcontext *sdictx, const Column*, uint opx)
{
  return sdictx->m_column_object_opx[opx];
}


char *buf_handle(Sdi_rcontext *rctx, size_t sz)
{
  return generic_buf_handle(&rctx->buf, sz);
}


template <typename T>
bool generic_lookup_ref(THD *thd, MDL_key::enum_mdl_namespace mdlns,
                        const String_type &name, dd::Object_id *idp)
{
  if (thd == nullptr)
  {
    return false;
  }

  // Acquire MDL here so that it becomes possible to acquire the
  // schema to look up its id in the current DD
  if (mdl_lock(thd, mdlns, name, "", MDL_INTENTION_EXCLUSIVE))
  {
    return true;
  }

  dd::cache::Dictionary_client *dc= thd->dd_client();
  dd::cache::Dictionary_client::Auto_releaser releaser(dc);

  const T *p= nullptr;
  // TODO: Split in two. Use error flag in ctx object
  if (dc->acquire(name, &p) || p == nullptr)
  {
    return true;
  }
  *idp= p->id();
  return false;
}


bool lookup_schema_ref(Sdi_rcontext *sdictx, const String_type &name,
                       dd::Object_id *idp)
{
  sdictx->m_schema_name= name;
  *idp= INVALID_OBJECT_ID;
  return false;
}

bool lookup_tablespace_ref(Sdi_rcontext *sdictx, const String_type &name,
                           Object_id *idp)
{
  return generic_lookup_ref<Tablespace>(sdictx->m_thd, MDL_key::TABLESPACE,
                                        name, idp);
}

/** @} */ // sdi_cc_internal


/**
  @defgroup sdi_api SDI API
  @ingroup sdi

  Definition of externally visible functions and classes, declared in sdi.h
  @{
*/

sdi_t serialize(const Schema &schema)
{
  return generic_serialize(nullptr, STRING_WITH_LEN("Schema"), schema, nullptr);
}


sdi_t serialize(THD *thd, const Table &table, const String_type &schema_name)
{
  return generic_serialize(thd, STRING_WITH_LEN("Table"), table, &schema_name);
}


sdi_t serialize(const Tablespace &tablespace)
{
  return generic_serialize(nullptr, STRING_WITH_LEN("Tablespace"), tablespace,
                           nullptr);
}


template <class Dd_type>
bool generic_deserialize(THD *thd, const sdi_t &sdi,
                         const String_type &object_type_name, Dd_type *dst,
                         String_type *schema_name_from_sdi= nullptr)
{
  RJ_Document doc;
  doc.Parse<0>(sdi.c_str());
  if (doc.HasParseError())
  {
    my_error(ER_INVALID_JSON_DATA, MYF(0), "deserialize()",
             rapidjson::GetParseError_En(doc.GetParseError()));
    return true;
  }

  if (doc.HasMember("mysqld_version_id"))
  {
    RJ_Value &mysqld_version_id= doc["mysqld_version_id"];
    DBUG_ASSERT(mysqld_version_id.IsUint64());
    if (mysqld_version_id.GetUint64() > std::uint64_t(MYSQL_VERSION_ID))
    {
      // Cannot deserialize SDIs from newer versions. Required?
      my_error(ER_IMP_INCOMPATIBLE_MYSQLD_VERSION, MYF(0),
               mysqld_version_id.GetUint64(), std::uint64_t(MYSQL_VERSION_ID));
      return true;
    }
  }
  else
  {
    DBUG_ASSERT(false);
  }

  DBUG_ASSERT(doc.HasMember("dd_version"));
  RJ_Value &dd_version_val= doc["dd_version"];
  DBUG_ASSERT(dd_version_val.IsUint());
  uint dd_version= dd_version_val.GetUint();
  if (dd_version != Dictionary_impl::get_target_dd_version())
  {
    // Incompatible change
    my_error(ER_IMP_INCOMPATIBLE_DD_VERSION, MYF(0),
             dd_version, Dictionary_impl::get_target_dd_version());
    return true;
  }

  DBUG_ASSERT(doc.HasMember("sdi_version"));
  RJ_Value &sdi_version_val= doc["sdi_version"];
  DBUG_ASSERT(sdi_version_val.IsUint64());
  std::uint64_t sdi_version_= sdi_version_val.GetUint64();
  if (sdi_version_ != sdi_version)
  {
    // Incompatible change
    my_error(ER_IMP_INCOMPATIBLE_SDI_VERSION, MYF(0),
             sdi_version_, sdi_version);
    return true;
  }

  DBUG_ASSERT(doc.HasMember("dd_object_type"));
  RJ_Value &dd_object_type_val= doc["dd_object_type"];
  DBUG_ASSERT(dd_object_type_val.IsString());
  String_type dd_object_type(dd_object_type_val.GetString());
  DBUG_ASSERT(dd_object_type == object_type_name);

  DBUG_ASSERT(doc.HasMember("dd_object"));
  RJ_Value &dd_object_val= doc["dd_object"];
  DBUG_ASSERT(dd_object_val.IsObject());

  Sdi_rcontext rctx(thd, dd_version, sdi_version_);
  if (dst->deserialize(&rctx, dd_object_val))
  {
    return checked_return(true);
  }
  if (schema_name_from_sdi != nullptr)
  {
    *schema_name_from_sdi= std::move(rctx.m_schema_name);
  }

  return false;
}

bool deserialize(THD *thd, const sdi_t &sdi, Schema *dst_schema)
{
  return generic_deserialize(thd, sdi, "Schema", dst_schema);
}

bool deserialize(THD *thd, const sdi_t &sdi, Table *dst_table,
                 String_type *deser_schema_name)
{
  return generic_deserialize(thd, sdi, "Table", dst_table, deser_schema_name);
}

bool deserialize(THD *thd, const sdi_t &sdi, Tablespace *dst_tablespace)
{
  return generic_deserialize(thd, sdi, "Tablespace", dst_tablespace);
}


namespace {
/**
  Templated convenience wrapper which first attempts to resolve the
  handlerton using the data dictionary object's engine() string.

  @param thd
  @param ddt    Data dictionary object

  @return handlerton pointer for this object
    @retval handlerton pointer on success
    @retval nullptr on error
*/

template <typename DDT>
static handlerton *resolve_hton(THD *thd, const DDT &ddt)
{
  plugin_ref pr=
    ha_resolve_by_name_raw(thd, lex_cstring_handle(ddt.engine()));
  if (pr)
  {
    return plugin_data<handlerton*>(pr);
  }
  return nullptr;
}


/**
  Covenience function for acquiring the schema and invoking a closure
  which uses the schema object.

  @param thd
  @param key key to use when acquiring Schema object
  @param clos closure to invoke with the Schema object
  @return error status
    @retval false on success
    @retval true otherwise
 */
template <class AKT, class CLOS>
bool with_schema(THD *thd, const AKT &key, CLOS &&clos)
{
  cache::Dictionary_client *dc= thd->dd_client();
  cache::Dictionary_client::Auto_releaser releaser(dc);

  const Schema *s= nullptr;
  if (dc->acquire(key, &s))
  {
    return true;
  }
  return clos(s);
}


/**
  Predicate which returns true if an n-character prefix of two
  character ranges are equal.

  @param begin1 beginning of first range
  @param end1 end of first range
  @param begin2 beginning of second range
  @param end2 end of second range
  @param n number of characters to compare
  @param csi character set to use (defaults to system_charset_info)
  @return true if prefix compares equal, false otherwise
 */

template <class CHAR_IT>
bool equal_prefix_chars(CHAR_IT &&begin1, CHAR_IT &&end1,
                        CHAR_IT &&begin2, CHAR_IT &&end2,
                        size_t n, const CHARSET_INFO *csi= system_charset_info)
{
  size_t char_count= 0;
  for (size_t rem_bytes= 0;
       char_count < n && begin1 < end1 && begin2 < end2;
       ++begin1, ++begin2)
  {
    if (*begin1 != *begin2)
    {
      return false;
    }
    if (rem_bytes == 0)
    {
      rem_bytes= my_mbcharlen(csi, static_cast<uchar>(*begin1));
      DBUG_ASSERT(rem_bytes > 0);
    }
    --rem_bytes;

    if (rem_bytes == 0)
    {
      ++char_count;
    }
  }
  return ((begin1 == end1 && begin2 == end2) || char_count == n);
}


/**
  Convenience function for comparing a prefix of the names of two DD objects.
  @param a first DD object
  @param b second DD object
  @param prefix_chars number characters in prefix to compare
  @return true if prefix compares equal, false otherwise
 */
template <class DDT>
bool equal_prefix_chars_name(const DDT &a, const DDT &b, size_t prefix_chars)
{
  return equal_prefix_chars(a.name().begin(), a.name().end(),
                            b.name().begin(), b.name().end(), prefix_chars);
}
}


namespace sdi {

bool store(THD *thd, const Schema *s)
{
  sdi_t sdi= serialize(*s);
  if (sdi.empty())
  {
    return checked_return(true);
  }

  cache::Dictionary_client *dc= thd->dd_client();
  cache::Dictionary_client::Auto_releaser releaser(dc);

  // This may actually be an update, so the schema need not be empty
  typedef std::vector<const Abstract_table*> sc_type;
  sc_type tables;
  if (dc->fetch_schema_components(s, &tables))
  {
    return checked_return(true);
  }

  for (const Abstract_table *at : tables)
  {
    const Table *tbl= dynamic_cast<const Table*>(at);
    if (!tbl)
    {
      continue;
    }

    // TODO: This will be sub-optimal as we may end up storing
    // the updated SDI multiple times in the same tablespace if
    // multiple tables in this schema are stored in the same tablespace.
    // Maybe we need to track which tablespace ids we have stored the
    // modified schema SDI for?
    handlerton *hton= resolve_hton(thd, *tbl);
    if (hton->store_schema_sdi &&
        hton->store_schema_sdi(thd, hton, lex_cstring_handle(sdi), s, tbl))
    {
      return checked_return(true);
    }
  }

  // Finally, update SDI file
  return checked_return(sdi_file::store(thd, lex_cstring_handle(sdi), s));
}


bool store(THD *thd, const Table *t)
{
  return with_schema
    (thd, t->schema_id(),
     [&](const Schema *s) {
      DBUG_ASSERT(s != nullptr);
      sdi_t sdi= serialize(thd, *t, s->name());
      if (sdi.empty())
      {
        return checked_return(true);
      }
      DBUG_EXECUTE_IF("abort_rename_after_update",
      { my_error(ER_ERROR_ON_WRITE, MYF(0), "error inject", 42,
                 "simulated write error");
        return true;});
      handlerton *hton= resolve_hton(thd, *t);
      return checked_return
        (hton->store_table_sdi(thd, hton,
                               lex_cstring_handle(sdi), t, s));
    });
}


bool store(THD *thd, const Tablespace *ts)
{
  sdi_t sdi= serialize(*ts);
  if (sdi.empty())
  {
    return checked_return(true);
  }
  handlerton *hton= resolve_hton(thd, *ts);
  return checked_return(sdi_tablespace::store(hton, lex_cstring_handle(sdi),
                                              ts));
}

bool drop(THD *thd, const Schema *s)
{
  return checked_return(sdi_file::remove(thd, s));
}

bool drop(THD *thd, const Table *t)
{
  return with_schema(thd, t->schema_id(),
                     [&](const Schema *s) {
                       DBUG_ASSERT(s != nullptr);
                       handlerton *hton= resolve_hton(thd, *t);
                       return checked_return(hton->remove_table_sdi(thd, hton, t, s));
                     });
}

bool drop(THD *thd, const Tablespace *ts)
{
  return checked_return(sdi_tablespace::remove(resolve_hton(thd, *ts), ts));
}

bool drop_after_update(THD *thd, const Schema *old_s,
                       const Schema *new_s)
{
  // Currently this test is not really necessary since the schema sdi
  // file name cannot change.
  return equal_prefix_chars_name(*old_s, *new_s,
                                 sdi_file::FILENAME_PREFIX_CHARS) ?
    false : checked_return(sdi_file::remove(thd, old_s));
}

bool drop_after_update(THD *thd, const Table *old_t, const Table *new_t)
{
  if ((old_t->schema_id() == new_t->schema_id() &&
       equal_prefix_chars_name(*old_t, *new_t, sdi_file::FILENAME_PREFIX_CHARS))
      // Hack to avoid calling resolve_hton() during unit tests
      // reslove_hton() will crash in unit tests because the
      // plugin_LOCK mutex has not been initialized.
      // Reviewers: Please feel free to suggest alternative solutions.
       || old_t->engine() == "innodb")
  {
    return false;
  }

  handlerton *old_hton= resolve_hton(thd, *old_t);
  if (old_hton->sdi_set)
  {
    return false;
  }

  return with_schema(thd, old_t->schema_id(),
                     [&](const Schema *s) {
                       DBUG_ASSERT(s != nullptr);
                       return checked_return(sdi_file::remove(thd, nullptr,
                                                              old_t, s));
                     });
}

} // namespace sdi
} // namespace dd
/** @} */ // end of group sdi_api


/**
  @defgroup sdi_ut SDI Unit-testing API
  @ingroup sdi

  Special functions used by unit tests but which are not available in
  the normal api.

  @{
*/

/**
  @namespace sdi_unittest
  Namespace from dd_sdi-t unit-test. Also used to contain driver/hook
  functions only used by unit-testing.
*/

namespace sdi_unittest {

typedef void (*cb)(dd::Sdi_wcontext*, const dd::Weak_object*, dd::Sdi_writer*);
void setup_wctx(cb fp, const dd::Weak_object *wo, dd::Sdi_writer *w)
{
  dd::String_type s("driver_schema");
  dd::Sdi_wcontext wctx(nullptr, &s);

  fp(&wctx, wo, w);
}

typedef void (*dcb)(dd::Sdi_rcontext*, dd::Weak_object*,
                    dd::RJ_Document &doc);
void setup_rctx(dcb fp, dd::Weak_object *wo, dd::RJ_Document &doc)
{
  dd::Sdi_rcontext rctx(nullptr, 0, 0); // restore ids for comparison
  fp(&rctx, wo, doc);
}

bool equal_prefix_chars_driver(const dd::String_type &a, const dd::String_type &b, size_t prefix)
{
  return dd::equal_prefix_chars(a.begin(), a.end(), b.begin(), b.end(), prefix);
}
} // namespace sdi_unittest

/** @} */ // End of group sdi_ut
