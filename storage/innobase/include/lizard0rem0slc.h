/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0rem0slc.h
 Slice implementation.

 Created 2025-12-12 by Yichang Song
 *******************************************************/
#ifndef lizard0rem0slc_h
#define lizard0rem0slc_h

#include "mem0mem.h"

#include "lizard0rec0spc0types.h"
#include "lizard0rem0rec.h"

class Slice_ctx;
class DD_slice_ctx;
class DDL_slice_ctx;
class Rlog_slice_ctx;
class Unzip_slice_ctx;
class Inheap_slice_ctx;
class DSlice_ctx;

/**
 * An abstract logical record.
 *
 * Different physical storage format need to implement it.
 *
 * Only allowed to construct from various slice context, so define
 * pretected/private constructor.
 * */
class Slice {
 protected:
  explicit Slice() : m_reg(), m_offsets(nullptr), m_index(nullptr) {}

  virtual ~Slice() = default;

 public:
  /** --------------------------------------------------------------------*/
  /** Fundamental methods. */

  /** Reg Getter */
  reg_t reg() const { return m_reg; }
  creg_t creg() const { return m_reg; }

  /** Rec Getter */
  const rec_t *rec() const { return m_reg.rec; }
  rec_t *rec() { return m_reg.rec; }

  /** Index Getter */
  const dict_index_t *index() const { return m_index; }

  /** Offsets Getter */
  const ulint *offsets() const { return m_offsets; }
  ulint *offsets() { return m_offsets; }

  /** Size of data stored on heap space which is contiguous.
   *
   * @retval	data size (bytes) */
  ulint data_size() const;

  /** Size of extra stored on heap space which is contiguous.
   *
   * @retval	extra size (bytes) */
  ulint extra_size() const;

  /** Return rec size on heap space which is contiguous.
   *
   * @retval	extra + data (bytes) */
  ulint rec_size() const { return extra_size() + data_size(); }

  /** Copy the rec that stored on heap space
   *
   * @param[in/out]	buffer
   *
   * @retval	copied rec.*/
  reg_t rec_copy(boc_t &cbuf) const;

  /** Get start address of contigous record. */
  byte *rec_start() const {
    ut_ad(validate());
    return m_reg.rec - extra_size();
  }

  /** Get end address of contigous record. */
  byte *rec_end() const {
    ut_ad(validate());
    return m_reg.rec + data_size();
  }

  /**
   * Make slice positioned on new record and correct related offsets.
   *
   * @param[in]		new record
   * @param[in]		index
   * @param[in]		check status or not. */
  void store_position(creg_t reg, const dict_index_t *index,
                      bool check_status = true);

  /** Whether slice is positioned on record.
   *
   * @param[in]		record
   * @param[in]		index
   *
   * @retval	true	yes
   * @retval	false	no */
  bool is_positioned(reg_t reg, const dict_index_t *index) const {
    ut_ad(reg.loc == m_reg.loc);
    return is_positioned(reg.rec, index);
  }

  /** Whether slice is positioned on record.
   *
   * @param[in]		record
   * @param[in]		index
   *
   * @retval	true	yes
   * @retval	false	no */
  bool is_positioned(creg_t creg, const dict_index_t *index) const {
    ut_ad(creg.loc == m_reg.loc);
    return is_positioned(creg.rec, index);
  }

  /** Whether slice is positioned on record.
   *
   * @param[in]		record
   * @param[in]		index
   *
   * @retval	true	yes
   * @retval	false	no */
  bool is_positioned(const rec_t *rec, const dict_index_t *index) const {
    ut_ad(rec == m_reg.rec);
    ut_ad(index == m_index);

    return validate();
  }

  /** Whether slice is positioned on index.
   *
   * @param[in]		index
   *
   * @retval	true	yes
   * @retval	false	no */
  bool is_positioned(const dict_index_t *index) const {
    ut_ad(index == m_index);
    return validate();
  }

  /** Get nth field length (ignore storage format.).
   *
   * @param[in]		index
   * @param[in]		nth field
   *
   * @retval	field length. */
  ulint get_nth_field_len(const dict_index_t *index, ulint n) const;

  /** --------------------------------------------------------------------*/
  /** Virtual func that has default implemention*/
  /** --------------------------------------------------------------------*/
  /** Validate record which stored on page or buffer. */
  virtual bool validate() const;

  /** --------------------------------------------------------------------*/
  /** Pure Virtual func that must be implemented by different storage format. */
  /** --------------------------------------------------------------------*/
  /** Whether record is physically contiguous. */
  virtual bool contiguous() const = 0;

  /** Record pack information getter. */
  virtual rec_pack_t rec_pack() const = 0;

  /** Row logical size (bytes), which is calculated by innodb compact format,
   * so any other different storage format should be equal with it.
   *
   * @retval	size.*/
  virtual ulint row_size() const = 0;

  /** Copy the row first n fields prefix to buffer using compact format.
   *
   * @param[in/out]	target buffer
   * @param[in/out]	target buffer size
   * @param[in]		n fields
   *
   * @retval	copied rec */
  virtual reg_t row_copy_prefix(boc_t *boc, size_t *buf_size,
                                ulint n_fields) const = 0;

  /** Copy the whole row to buffer using compact format.
   *
   * @param[in/out]	target buffer
   *
   * @retval	copied rec */
  virtual reg_t row_copy(boc_t &cbuf) const = 0;

  /** Copy the logical contigous data section to dest
   *
   * @param[in]		target buffer
   * @param[in]		prefix data length */
  virtual void dtuple_copy(byte *dest, ulint copy_len) const = 0;

  /**The nth byte of logical contigous data section.*/
  virtual byte operator[](size_t i) const = 0;

  /** Get system column physical offset on rec.
   *
   * @param[in] 	system column type
   * @param[in]		nth column
   * @param[in/out]	field length.
   *
   * @retval	physical offset. */
  virtual ulint get_nth_sys_field_offs(ulint sys_col_type, ulint n,
                                       ulint *len) const = 0;

  /** Get memory contigous nth-field value address by calculate or realloc.
   *
   * @param[in]		index
   * @param[in]		nth field
   * @param[in/out]	field length
   *
   * @retval	field value address. */
  virtual byte *get_nth_field(const dict_index_t *index, ulint n,
                              ulint *len) = 0;

  /** Get nth-field, maybe instant column. */
  virtual const byte *get_nth_field_instant(const dict_index_t *index, ulint n,
                                            ulint *len) = 0;

  /** Overwrite nth field value, only suitable for dslice.
   *
   * @param[in]		index
   * @param[in]		nth field
   * @param[in]		data source
   * @param[in]		data length
   * */
  virtual void set_nth_field(const dict_index_t *index, ulint n,
                             const void *data, ulint len) = 0;

 protected:
  /** Validates the consistency of a physical record.
   * @return true if ok */
  bool rec_validate_low() const;

 protected:
  /** Record which point to heap storage address.  */
  reg_t m_reg;
  /** Offsets of record. */
  ulint *m_offsets;
  /** Index. */
  const dict_index_t *m_index;
};

/* Default Record slice which used innodb row format. */
class DSlice : public Slice {
 protected:
  explicit DSlice() : Slice() {}

  virtual ~DSlice() = default;

 public:
  /** --------------------------------------------------------------------*/
  /** Virtual func */
  /** --------------------------------------------------------------------*/

  /** DSlice record is physically contiguous. */
  virtual bool contiguous() const override { return true; }

  /** DSlice didn't have record pack information. */
  virtual rec_pack_t rec_pack() const override { return rec_pack_t{}; }

  /** Row logical size (bytes), which is calculated by dslice row format,
   *  so it's equal with rec storage size
   *
   * @retval	size.*/
  virtual ulint row_size() const override { return rec_size(); }

  /** Copy the whole row to buffer using compact format.
   *
   * @param[in/out]	target buffer
   *
   * @retval	copied rec */
  reg_t row_copy(boc_t &cbuf) const override { return rec_copy(cbuf); }

  /** Copy the row first n fields prefix to buffer using compact format.
   *
   * @param[in/out]	target buffer
   * @param[in/out]	target buffer size
   * @param[in]		n fields
   *
   * @retval	copied rec */
  reg_t row_copy_prefix(boc_t *boc, size_t *buf_size,
                        ulint n_fields) const override;

  /** Copy the logical contigous data section to dest
   *
   * @param[in]		target buffer
   * @param[in]		prefix data length */
  virtual void dtuple_copy(byte *dest, ulint copy_len) const override;

  /**The nth byte of logical contigous data section.*/
  virtual byte operator[](size_t i) const override { return m_reg.rec[i]; }

  /** Get system column physical offset on rec.
   *
   * @param[in] 	system column type
   * @param[in]		nth column
   * @param[in/out]	field length.
   *
   * @retval	physical offset. */
  virtual ulint get_nth_sys_field_offs(ulint sys_col_type, ulint n,
                                       ulint *len) const override;

  /** Get memory contigous nth-field value address by calculate directly.
   *
   * @param[in]		index
   * @param[in]		nth field
   * @param[in/out]	field length
   *
   * @retval	field value address. */
  byte *get_nth_field(const dict_index_t *index, ulint n, ulint *len) override;

  /** Get nth-field, maybe instant column. */
  virtual const byte *get_nth_field_instant(const dict_index_t *index, ulint n,
                                            ulint *len) override;

  /** Validate slice according to original innodb rec format. */
  virtual bool validate() const override;

  /** Overwrite nth field value, only suitable for dslice.
   *
   * @param[in]		index
   * @param[in]		nth field
   * @param[in]		data source
   * @param[in]		data length
   * */
  virtual void set_nth_field(const dict_index_t *index, ulint n,
                             const void *data, ulint len) override;
  /** --------------------------------------------------------------------*/

  /* Correspond to btr_rec_get_field_ref */
  byte *get_lob_field_ref(ulint n);

  /* Correspond to btr_rec_get_field_ref */
  const byte *get_lob_field_ref(ulint n) const;

 private:
  /** Set slice attributes through slice context -> get_offsets() */
  void set_offsets(creg_t reg, ulint *offsets, const dict_index_t *index);

  friend class Slice_ctx;
  friend class DDL_slice_ctx;
  friend class Rlog_slice_ctx;
  friend class DD_slice_ctx;
  friend class Unzip_slice_ctx;
  friend class DSlice_ctx;
  friend class Cached_slice_ctx;
  friend class Inheap_slice_ctx;
};

/* Secondary Prefix Compressed Record Type. */
class PSlice : public Slice {
 private:
  explicit PSlice() : Slice(), m_rec_pack(), m_heap(nullptr), m_drec(nullptr) {}

  virtual ~PSlice() = default;

  /** PSlice record is split. */
  virtual bool contiguous() const override { return false; }

  /** Getter pack information from record. */
  virtual rec_pack_t rec_pack() const override {
    ut_ad(m_rec_pack.is_spc());
    return m_rec_pack;
  }

  /** Row logical size (bytes), which is calculated by compact format,
   *
   * @retval	size.*/
  virtual ulint row_size() const override;

  /** Copy the whole row to buffer using compact format.
   *
   * @param[in/out]	target buffer
   *
   * @retval	copied rec */
  virtual reg_t row_copy(boc_t &cbuf) const override;

  /** Copy the row first n fields prefix to buffer using compact format.
   *
   * @param[in/out]	target buffer
   * @param[in/out]	target buffer size
   * @param[in]		n fields
   *
   * @retval	copied rec */
  virtual reg_t row_copy_prefix(boc_t *boc, size_t *buf_size,
                                ulint n_fields) const override;

  /** Copy the logical contigous data section to dest
   *
   * @param[in]		target buffer
   * @param[in]		prefix data length */
  virtual void dtuple_copy(byte *dest, ulint copy_len) const override;

  /**The nth byte of logical contigous data section.*/
  virtual byte operator[](size_t i) const override;

  /** Get system column physical offset on rec.
   *
   * @param[in] 	system column type
   * @param[in]		nth column
   * @param[in/out]	field length.
   *
   * @retval	physical offset. */
  virtual ulint get_nth_sys_field_offs(ulint sys_col_type, ulint n,
                                       ulint *len) const override;
  /** Get memory contigous nth-field value address by calculate or realloc.
   *
   * @param[in]		index
   * @param[in]		nth field
   * @param[in/out]	field length
   *
   * @retval	field value address. */
  virtual byte *get_nth_field(const dict_index_t *index, ulint n,
                              ulint *len) override;

  /** Validate slice according to compressed rec format. */
  virtual bool validate() const override;

  /** Overwrite nth field value, only suitable for dslice.
   *
   * @param[in]		index
   * @param[in]		nth field
   * @param[in]		data source
   * @param[in]		data length
   * */
  virtual void set_nth_field(const dict_index_t *index, ulint n,
                             const void *data, ulint len) override;

  /** Get nth-field, maybe instant column. */
  const byte *get_nth_field_instant(const dict_index_t *index, ulint n,
                                    ulint *len) override;

 private:
  /** Set slice attributes through slice context -> get_offsets() */
  void set_offsets(creg_t reg, ulint *offsets, const dict_index_t *index,
                   mem_heap_t **heap);

  /** Set slice attributes through slice context -> get_offsets(),
   *  here special for Inheap_slice_ctx.
   * */
  void set_offsets(creg_t reg, const page_t *page, ulint *offsets,
                   const dict_index_t *index, mem_heap_t **heap);

  /** Setter of pslice record offsets.
   *
   * @param[in]		rec_t
   * @param[in]		offsets of rec_t
   * @param[in]		index
   * @param[in]		spc rec pack info.
   * @param[in]		page dict record address.
   * @param[in]		heap */
  void set_offsets_low(creg_t reg, ulint *offsets, const dict_index_t *index,
                       const rec_pack_t rec_pack, const dict_rec_t *drec,
                       mem_heap_t **heap);

 private:
  /** Slice pack info on record. */
  rec_pack_t m_rec_pack;
  /** Heap. */
  mem_heap_t **m_heap;
  /** Page dict rec pointer. */
  const dict_rec_t *m_drec;

  friend class Slice_ctx;
  friend class Inheap_slice_ctx;
};

/* Correspond to ddl::mrec_t, which doesn't have REC_N_NEW_EXTRA_BYTES.  */
class MSlice : public DSlice {
 private:
  MSlice() : DSlice() {}
  virtual ~MSlice() = default;

 public:
  bool validate() const override;

 private:
  friend Rlog_slice_ctx;
  friend DDL_slice_ctx;
};

/* Slice Context Interface which disabled the copy and assign construct */
class Slice_context_iface {
 public:
  Slice_context_iface() {}
  virtual ~Slice_context_iface() {}

 private:
  Slice_context_iface(const Slice_context_iface &);
  Slice_context_iface(const Slice_context_iface &&);
  Slice_context_iface &operator=(const Slice_context_iface &);
  Slice_context_iface &operator=(Slice_context_iface &&);
};

#endif
