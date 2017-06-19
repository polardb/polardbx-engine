/* Copyright (c) 2000, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file include/m_ctype.h
  A better implementation of the UNIX ctype(3) library.
*/

#ifndef _m_ctype_h
#define _m_ctype_h

#include <stdarg.h>
#include <stddef.h>
#include <sys/types.h>
#include <stdbool.h>

#include "my_byteorder.h"
#include "my_compiler.h"
#include "my_inttypes.h"
#include "my_loglevel.h"
#include "my_sharedlib.h"
#include "str_uca_type.h"

#ifdef	__cplusplus
extern "C" {
#endif

#define MY_CS_NAME_SIZE			32
#define MY_CS_CTYPE_TABLE_SIZE		257
#define MY_CS_TO_LOWER_TABLE_SIZE	256
#define MY_CS_TO_UPPER_TABLE_SIZE	256
#define MY_CS_SORT_ORDER_TABLE_SIZE	256
#define MY_CS_TO_UNI_TABLE_SIZE		256

#define CHARSET_DIR	"charsets/"

/**
  Our own version of wchar_t, ie., a type that holds a single Unicode code point
  ("wide character"). ulong is always big enough to hold any character
  in the BMP.
*/
typedef ulong my_wc_t;

#define MY_CS_REPLACEMENT_CHARACTER 0xFFFD

/*
  On i386 we store Unicode->CS conversion tables for
  some character sets using Big-endian order,
  to copy two bytes at onces.
  This gives some performance improvement.
*/
#if defined(__i386__) || defined(__x86_64__)
#define MB2(x)                (((x) >> 8) + (((x) & 0xFF) << 8))
static inline void MY_PUT_MB2(unsigned char *s, uint16 code)
{
  int2store(s, code);
}
#else
#define MB2(x)                (x)
static inline void MY_PUT_MB2(unsigned char *s, uint16 code)
{
  s[0]= code >> 8;
  s[1]= code & 0xFF;
}
#endif



typedef struct unicase_info_char_st
{
  uint32 toupper;
  uint32 tolower;
  uint32 sort;
} MY_UNICASE_CHARACTER;


typedef struct unicase_info_st
{
  my_wc_t maxchar;
  const MY_UNICASE_CHARACTER **page;
} MY_UNICASE_INFO;


extern MY_UNICASE_INFO my_unicase_default;
extern MY_UNICASE_INFO my_unicase_turkish;
extern MY_UNICASE_INFO my_unicase_mysql500;
extern MY_UNICASE_INFO my_unicase_unicode520;

/*
  NOTE: If you change MY_UCA_MAX_CONTRACTION, be sure to update the comment on
  MY_UCA_CNT_MID1 in strings/uca_data.h, as it might cause us to run out of
  bits in a byte flag.
*/
#define MY_UCA_MAX_CONTRACTION 6
#define MY_UCA_MAX_WEIGHT_SIZE 25
#define MY_UCA_WEIGHT_LEVELS   1

typedef struct my_contraction_t
{
  my_wc_t ch[MY_UCA_MAX_CONTRACTION];   /* Character sequence              */
  uint16 weight[MY_UCA_MAX_WEIGHT_SIZE];/* Its weight string, 0-terminated */
  bool with_context;
} MY_CONTRACTION;



typedef struct my_contraction_list_t
{
  bool has_contractions;
  /*
    Contractions are split by their length. The first two elements in nitems
    and item are meaningless because contraction must consist of at least two
    code points.
  */

  /* Number of contractions of same length. */
  size_t nitems[MY_UCA_MAX_CONTRACTION + 1];
  /* Lists of contractions of same length. */
  MY_CONTRACTION* item[MY_UCA_MAX_CONTRACTION + 1];

  /*
    Character flags for each character, e.g. "is contraction head"
    (MY_UCA_CNT_HEAD). These are set per-character, but to conserve space,
    only the lowest few bits of each character are used (so there could be
    false positives). Specifically, the code point is ANDed with
    MY_UCA_CNT_FLAG_MASK to index into this array.
  */
  char *flags;
} MY_CONTRACTIONS;


bool my_uca_can_be_contraction_head(const MY_CONTRACTIONS *c, my_wc_t wc);
bool my_uca_can_be_contraction_tail(const MY_CONTRACTIONS *c, my_wc_t wc);
uint16 *my_uca_contraction2_weight(const MY_CONTRACTIONS *c,
                                   my_wc_t wc1, my_wc_t wc2);


typedef struct uca_info_st
{
  enum enum_uca_ver   version;

  // Collation weights.
  my_wc_t maxchar;
  uchar   *lengths;
  uint16  **weights;
  MY_CONTRACTIONS contractions;

  /* Logical positions */
  my_wc_t first_non_ignorable;
  my_wc_t last_non_ignorable;
  my_wc_t first_primary_ignorable;
  my_wc_t last_primary_ignorable;
  my_wc_t first_secondary_ignorable;
  my_wc_t last_secondary_ignorable;
  my_wc_t first_tertiary_ignorable;
  my_wc_t last_tertiary_ignorable;
  my_wc_t first_trailing;
  my_wc_t last_trailing;
  my_wc_t first_variable;
  my_wc_t last_variable;

} MY_UCA_INFO;



extern MY_UCA_INFO my_uca_v400;


typedef struct uni_ctype_st
{
  uchar  pctype;
  uchar  *ctype;
} MY_UNI_CTYPE;

extern MY_UNI_CTYPE my_uni_ctype[256];

/* wm_wc and wc_mb return codes */
#define MY_CS_ILSEQ	0     /* Wrong by sequence: wb_wc                   */
#define MY_CS_ILUNI	0     /* Cannot encode Unicode to charset: wc_mb    */
#define MY_CS_TOOSMALL  -101  /* Need at least one byte:    wc_mb and mb_wc */
#define MY_CS_TOOSMALL2 -102  /* Need at least two bytes:   wc_mb and mb_wc */
#define MY_CS_TOOSMALL3 -103  /* Need at least three bytes: wc_mb and mb_wc */
/* These following three are currently not really used */
#define MY_CS_TOOSMALL4 -104  /* Need at least 4 bytes: wc_mb and mb_wc */
#define MY_CS_TOOSMALL5 -105  /* Need at least 5 bytes: wc_mb and mb_wc */
#define MY_CS_TOOSMALL6 -106  /* Need at least 6 bytes: wc_mb and mb_wc */
/* A helper macros for "need at least n bytes" */
#define MY_CS_TOOSMALLN(n)    (-100-(n))

#define MY_SEQ_INTTAIL	1
#define MY_SEQ_SPACES	2

        /* My charsets_list flags */
#define MY_CS_COMPILED  1      /* compiled-in sets               */
#define MY_CS_CONFIG    2      /* sets that have a *.conf file   */
#define MY_CS_INDEX     4      /* sets listed in the Index file  */
#define MY_CS_LOADED    8      /* sets that are currently loaded */
#define MY_CS_BINSORT	16     /* if binary sort order           */
#define MY_CS_PRIMARY	32     /* if primary collation           */
#define MY_CS_STRNXFRM	64     /*
                                 if _not_ set, sort_order will
                                 give same result as strnxfrm --
                                 all new collations should have this
                                 flag set, do not check it in new code
                               */
#define MY_CS_UNICODE	128    /* is a charset is BMP Unicode    */
#define MY_CS_READY	256    /* if a charset is initialized    */
#define MY_CS_AVAILABLE	512    /* If either compiled-in or loaded*/
#define MY_CS_CSSORT	1024   /* if case sensitive sort order   */	
#define MY_CS_HIDDEN	2048   /* don't display in SHOW          */	
#define MY_CS_PUREASCII 4096   /* if a charset is pure ascii     */
#define MY_CS_NONASCII  8192   /* if not ASCII-compatible        */
#define MY_CS_UNICODE_SUPPLEMENT 16384 /* Non-BMP Unicode characters */
#define MY_CS_LOWER_SORT 32768 /* If use lower case as weight   */
#define MY_CHARSET_UNDEFINED 0

/* Character repertoire flags */
#define MY_REPERTOIRE_ASCII      1 /* Pure ASCII            U+0000..U+007F */
#define MY_REPERTOIRE_EXTENDED   2 /* Extended characters:  U+0080..U+FFFF */
#define MY_REPERTOIRE_UNICODE30  3 /* ASCII | EXTENDED:     U+0000..U+FFFF */

/* Flags for strxfrm */
#define MY_STRXFRM_PAD_TO_MAXLEN   0x00000080 /* if pad tail(for filesort) */


typedef struct my_uni_idx_st
{
  uint16      from;
  uint16      to;
  const uchar *tab;
} MY_UNI_IDX;

typedef struct
{
  uint beg;
  uint end;
  uint mb_len;
} my_match_t;

struct charset_info_st;

typedef struct my_charset_loader_st
{
  char error[128];
  void *(*once_alloc)(size_t);
  void *(*mem_malloc)(size_t);
  void *(*mem_realloc)(void *, size_t);
  void (*mem_free)(void *);
  void (*reporter)(enum loglevel, const char *format, ...)
    MY_ATTRIBUTE((format(printf, 2, 3)));
  int  (*add_collation)(struct charset_info_st *cs);
} MY_CHARSET_LOADER;


extern int (*my_string_stack_guard)(int);

enum Pad_attribute { PAD_SPACE, NO_PAD };

/* See strings/CHARSET_INFO.txt for information about this structure  */
typedef struct my_collation_handler_st
{
  bool (*init)(struct charset_info_st *, MY_CHARSET_LOADER *);
  /* Collation routines */
  int     (*strnncoll)(const struct charset_info_st *,
		       const uchar *, size_t, const uchar *, size_t, bool);
  /**
    Compare the two strings under the pad rules given by the collation.

    Thus, for NO PAD collations, this is identical to strnncoll with is_prefix
    set to false. For PAD SPACE collations, the two strings are conceptually
    extended infinitely at the end using space characters (0x20) and then
    compared under the collation's normal comparison rules, so that e.g 'a' is
    equal to 'a '.
  */
  int     (*strnncollsp)(const struct charset_info_st *,
                         const uchar *, size_t, const uchar *, size_t);
  /**
    Transform the string into a form such that memcmp() between transformed
    strings yields the correct collation order.

    @param [out] dst Buffer for the transformed string.
    @param [out] dstlen Number of bytes available in dstlen.
      Must be even.
    @param num_codepoints Treat the string as if it were of type
      CHAR(num_codepoints). In particular, this means that if the
      collation is a pad collation (pad_attribute is PAD_SPACE) and
      string has fewer than "num_codepoints" codepoints, the string
      will be transformed as if it ended in (num_codepoints-n) extra spaces.
      If the string has more than "num_codepoints" codepoints,
      behavior is undefined; may truncate, may crash, or do something
      else entirely. Note that MY_STRXFRM_PAD_TO_MAXLEN overrides this;
      if it is given for a PAD SPACE collation, this value is taken to be
      effectively infinity.
    @param src The source string, in the required character set
      for the collation.
    @param srclen Number of bytes in src.
    @param flags ORed bitmask of MY_STRXFRM_* flags.

    @return Number of bytes written to dst.
  */
  size_t  (*strnxfrm)(const struct charset_info_st *,
                      uchar *dst, size_t dstlen, uint num_codepoints,
                      const uchar *src, size_t srclen, uint flags);

  /**
    Return the maximum number of output bytes needed for strnxfrm()
    to output all weights for any string of the given input length.
    You can use this to e.g. size buffers for sort keys.

    @param num_bytes Number of bytes in the input string. Note that for
      multibyte character sets, this _must_ be a pessimistic estimate,
      ie., one that's cs->mbmaxlen * max_num_codepoints. So for e.g.
      the utf8mb4 string "foo", you will need to give in 12, not 3.
  */
  size_t    (*strnxfrmlen)(const struct charset_info_st *, size_t num_bytes);
  bool (*like_range)(const struct charset_info_st *,
		     const char *s, size_t s_length,
		     char w_prefix, char w_one, char w_many,
		     size_t res_length,
		     char *min_str, char *max_str,
		     size_t *min_len, size_t *max_len);
  int     (*wildcmp)(const struct charset_info_st *,
  		     const char *str,const char *str_end,
                     const char *wildstr,const char *wildend,
                     int escape,int w_one, int w_many);

  int  (*strcasecmp)(const struct charset_info_st *, const char *,
                     const char *);
  
  uint (*strstr)(const struct charset_info_st *,
                 const char *b, size_t b_length,
                 const char *s, size_t s_length,
                 my_match_t *match, uint nmatch);
  
  /**
    Compute a sort hash for the given key. This hash must preserve equality
    under the given collation, so that a=b => H(a)=H(b). Note that this hash
    is used for hash-based partitioning (PARTITION KEY), so you cannot change
    it except when writing a new collation; it needs to be unchanged across
    releases, so that the on-disk format does not change. (It is also used
    for testing equality in the MEMORY storage engine.)

    nr1 and nr2 are both in/out parameters. nr1 is the actual hash value;
    nr2 holds extra state between invocations.
  */
  void (*hash_sort)(const struct charset_info_st *cs, const uchar *key,
                    size_t len, ulong *nr1, ulong *nr2);
  bool (*propagate)(const struct charset_info_st *cs, const uchar *str,
                    size_t len);
} MY_COLLATION_HANDLER;

extern MY_COLLATION_HANDLER my_collation_mb_bin_handler;
extern MY_COLLATION_HANDLER my_collation_8bit_bin_handler;
extern MY_COLLATION_HANDLER my_collation_8bit_simple_ci_handler;
extern MY_COLLATION_HANDLER my_collation_ucs2_uca_handler;

/* Some typedef to make it easy for C++ to make function pointers */
typedef int (*my_charset_conv_mb_wc)(const struct charset_info_st *,
                                     my_wc_t *, const uchar *, const uchar *);
typedef int (*my_charset_conv_wc_mb)(const struct charset_info_st *, my_wc_t,
                                     uchar *, uchar *);
typedef size_t (*my_charset_conv_case)(const struct charset_info_st *,
                                       char *, size_t, char *, size_t);


/* See strings/CHARSET_INFO.txt about information on this structure  */
typedef struct my_charset_handler_st
{
  bool (*init)(struct charset_info_st *, MY_CHARSET_LOADER *loader);
  /* Multibyte routines */
  uint    (*ismbchar)(const struct charset_info_st *, const char *,
                      const char *);
  uint    (*mbcharlen)(const struct charset_info_st *, uint c);
  size_t  (*numchars)(const struct charset_info_st *, const char *b,
                      const char *e);

  /**
    Return at which byte codepoint number "pos" begins, relative to
    the start of the string. If the string is shorter than or is
    exactly "pos" codepoints long, returns a value equal or greater to
    (e-b).
  */
  size_t  (*charpos)(const struct charset_info_st *, const char *b,
                     const char *e, size_t pos);
  size_t  (*well_formed_len)(const struct charset_info_st *,
                             const char *b,const char *e,
                             size_t nchars, int *error);
  /**
    Given a pointer and a length in bytes, returns a new length in bytes where
    all trailing space characters are stripped. This holds even for NO PAD
    collations.

    Exception: The "binary" collation, which is used behind-the-scenes to implement
    the BINARY type (by mapping it to CHAR(n) COLLATE "binary"), returns just the
    length back with no stripping. It's done that way so that Field_string
    (implementing CHAR(n)) returns the full padded width on read (as opposed to
    a normal CHAR, where we usually strip the spaces on read), but it's
    suboptimal, since lengthsp() is also used in a number of other places,
    e.g. stripping trailing spaces from enum values given in by the user.
    If you call this function, be aware of this special exception and consider
    the implications.
  */
  size_t  (*lengthsp)(const struct charset_info_st *, const char *ptr,
                      size_t length);
  size_t  (*numcells)(const struct charset_info_st *, const char *b,
                      const char *e);
  
  /* Unicode conversion */
  my_charset_conv_mb_wc mb_wc;
  my_charset_conv_wc_mb wc_mb;

  /* CTYPE scanner */
  int (*ctype)(const struct charset_info_st *cs, int *ctype,
               const uchar *s, const uchar *e);
  
  /* Functions for case and sort conversion */
  size_t  (*caseup_str)(const struct charset_info_st *, char *);
  size_t  (*casedn_str)(const struct charset_info_st *, char *);

  my_charset_conv_case caseup;
  my_charset_conv_case casedn;

  /* Charset dependant snprintf() */
  size_t (*snprintf)(const struct charset_info_st *, char *to, size_t n,
                     const char *fmt,
                     ...) MY_ATTRIBUTE((format(printf, 4, 5)));
  size_t (*long10_to_str)(const struct charset_info_st *, char *to, size_t n,
                          int radix, long int val);
  size_t (*longlong10_to_str)(const struct charset_info_st *, char *to,
                              size_t n, int radix, longlong val);
  
  void (*fill)(const struct charset_info_st *, char *to, size_t len,
               int fill);
  
  /* String-to-number conversion routines */
  long        (*strntol)(const struct charset_info_st *, const char *s,
                         size_t l, int base, char **e, int *err);
  ulong      (*strntoul)(const struct charset_info_st *, const char *s,
                         size_t l, int base, char **e, int *err);
  longlong   (*strntoll)(const struct charset_info_st *, const char *s,
                         size_t l, int base, char **e, int *err);
  ulonglong (*strntoull)(const struct charset_info_st *, const char *s,
                         size_t l, int base, char **e, int *err);
  double      (*strntod)(const struct charset_info_st *, char *s,
                         size_t l, char **e, int *err);
  longlong    (*strtoll10)(const struct charset_info_st *cs,
                           const char *nptr, char **endptr, int *error);
  ulonglong   (*strntoull10rnd)(const struct charset_info_st *cs,
                                const char *str, size_t length,
                                int unsigned_fl,
                                char **endptr, int *error);
  size_t        (*scan)(const struct charset_info_st *, const char *b,
                        const char *e, int sq);
} MY_CHARSET_HANDLER;

extern MY_CHARSET_HANDLER my_charset_8bit_handler;
extern MY_CHARSET_HANDLER my_charset_ascii_handler;
extern MY_CHARSET_HANDLER my_charset_ucs2_handler;

/* See strings/CHARSET_INFO.txt about information on this structure  */
typedef struct charset_info_st
{
  uint      number;
  uint      primary_number;
  uint      binary_number;
  uint      state;
  const char *csname;
  const char *name;
  const char *comment;
  const char *tailoring;
  struct Coll_param *coll_param;
  const uchar *ctype;
  const uchar *to_lower;
  const uchar *to_upper;
  const uchar *sort_order;
  MY_UCA_INFO *uca; /* This can be changed in apply_one_rule() */
  const uint16     *tab_to_uni;
  const MY_UNI_IDX *tab_from_uni;
  const MY_UNICASE_INFO *caseinfo;
  const struct lex_state_maps_st *state_maps; /* parser internal data */
  const uchar *ident_map; /* parser internal data */
  uint      strxfrm_multiply;
  uchar     caseup_multiply;
  uchar     casedn_multiply;
  uint      mbminlen;
  uint      mbmaxlen;
  uint      mbmaxlenlen;
  my_wc_t   min_sort_char;
  my_wc_t   max_sort_char; /* For LIKE optimization */
  uchar     pad_char;
  bool      escape_with_backslash_is_dangerous;
  uchar     levels_for_compare;
  
  MY_CHARSET_HANDLER *cset;
  MY_COLLATION_HANDLER *coll;
  
  /**
    If this collation is PAD_SPACE, it collates as if all inputs were
    padded with a given number of spaces at the end (see the "num_codepoints"
    flag to strnxfrm). NO_PAD simply compares unextended strings.

    Note that this is fundamentally about the behavior of coll->strnxfrm.
  */
  enum Pad_attribute pad_attribute;
} CHARSET_INFO;
#define ILLEGAL_CHARSET_INFO_NUMBER (~0U)


/*
  NOTE: You cannot use a CHARSET_INFO without it having been initialized first.
  In particular, they are not initialized when a unit test starts; do not use
  these globals indiscriminately from there, and do not add more. Instead,
  load them through a MY_CHARSET_LOADER, using my_collation_get_by_name().
*/

extern MYSQL_PLUGIN_IMPORT CHARSET_INFO my_charset_bin;
extern MYSQL_PLUGIN_IMPORT CHARSET_INFO my_charset_latin1;
extern MYSQL_PLUGIN_IMPORT CHARSET_INFO my_charset_filename;
extern MYSQL_PLUGIN_IMPORT CHARSET_INFO my_charset_utf8mb4_0900_ai_ci;

extern CHARSET_INFO my_charset_latin1_bin;
extern CHARSET_INFO my_charset_utf32_unicode_ci;
extern MYSQL_PLUGIN_IMPORT CHARSET_INFO my_charset_utf8_general_ci;
extern CHARSET_INFO my_charset_utf8_tolower_ci;
extern CHARSET_INFO my_charset_utf8_unicode_ci;
extern CHARSET_INFO my_charset_utf8_bin;
extern CHARSET_INFO my_charset_utf8mb4_bin;
extern MYSQL_PLUGIN_IMPORT CHARSET_INFO my_charset_utf8mb4_general_ci;

#define MY_UTF8MB3                 "utf8"
#define MY_UTF8MB4                 "utf8mb4"


/* declarations for simple charsets */
extern size_t my_strnxfrm_simple(const CHARSET_INFO *,
                                 uchar *dst, size_t dstlen, uint nweights,
                                 const uchar *src, size_t srclen, uint flags);
size_t  my_strnxfrmlen_simple(const CHARSET_INFO *, size_t); 
extern int  my_strnncoll_simple(const CHARSET_INFO *, const uchar *, size_t,
				const uchar *, size_t, bool);

extern int  my_strnncollsp_simple(const CHARSET_INFO *, const uchar *, size_t,
                                  const uchar *, size_t);

extern void my_hash_sort_simple(const CHARSET_INFO *cs,
				const uchar *key, size_t len,
				ulong *nr1, ulong *nr2); 

extern size_t my_lengthsp_8bit(const CHARSET_INFO *cs, const char *ptr,
                               size_t length);

extern uint my_instr_simple(const struct charset_info_st *,
                            const char *b, size_t b_length,
                            const char *s, size_t s_length,
                            my_match_t *match, uint nmatch);


/* Functions for 8bit */
extern size_t my_caseup_str_8bit(const CHARSET_INFO *, char *);
extern size_t my_casedn_str_8bit(const CHARSET_INFO *, char *);
extern size_t my_caseup_8bit(const CHARSET_INFO *, char *src, size_t srclen,
                             char *dst, size_t dstlen);
extern size_t my_casedn_8bit(const CHARSET_INFO *, char *src, size_t srclen,
                             char *dst, size_t dstlen);

extern int my_strcasecmp_8bit(const CHARSET_INFO * cs, const char *,
                              const char *);

int my_mb_wc_8bit(const CHARSET_INFO *cs,my_wc_t *wc, const uchar *s,
                  const uchar *e);
int my_wc_mb_8bit(const CHARSET_INFO *cs,my_wc_t wc, uchar *s, uchar *e);

int my_mb_ctype_8bit(const CHARSET_INFO *,int *, const uchar *,const uchar *);
int my_mb_ctype_mb(const CHARSET_INFO *,int *, const uchar *,const uchar *);

size_t my_scan_8bit(const CHARSET_INFO *cs, const char *b, const char *e,
                    int sq);

size_t my_snprintf_8bit(const struct charset_info_st *, char *to, size_t n,
                        const char *fmt, ...)
  MY_ATTRIBUTE((format(printf, 4, 5)));

long       my_strntol_8bit(const CHARSET_INFO *, const char *s, size_t l,
                           int base, char **e, int *err);
ulong      my_strntoul_8bit(const CHARSET_INFO *, const char *s, size_t l,
                            int base, char **e, int *err);
longlong   my_strntoll_8bit(const CHARSET_INFO *, const char *s, size_t l,
                            int base, char **e, int *err);
ulonglong my_strntoull_8bit(const CHARSET_INFO *, const char *s, size_t l,
                            int base, char **e, int *err);
double      my_strntod_8bit(const CHARSET_INFO *, char *s, size_t l, char **e,
			    int *err);
size_t my_long10_to_str_8bit(const CHARSET_INFO *, char *to, size_t l,
                             int radix, long int val);
size_t my_longlong10_to_str_8bit(const CHARSET_INFO *, char *to, size_t l,
                                 int radix, longlong val);

longlong my_strtoll10_8bit(const CHARSET_INFO *cs,
                           const char *nptr, char **endptr, int *error);
longlong my_strtoll10_ucs2(const CHARSET_INFO *cs, 
                           const char *nptr, char **endptr, int *error);

ulonglong my_strntoull10rnd_8bit(const CHARSET_INFO *cs,
                                 const char *str, size_t length, int
                                 unsigned_fl, char **endptr, int *error);
ulonglong my_strntoull10rnd_ucs2(const CHARSET_INFO *cs, 
                                 const char *str, size_t length,
                                 int unsigned_fl, char **endptr, int *error);

void my_fill_8bit(const CHARSET_INFO *cs, char* to, size_t l, int fill);

/* For 8-bit character set */
bool  my_like_range_simple(const CHARSET_INFO *cs,
                           const char *ptr, size_t ptr_length,
                           char escape, char w_one, char w_many,
                           size_t res_length,
                           char *min_str, char *max_str,
                           size_t *min_length, size_t *max_length);

/* For ASCII-based multi-byte character sets with mbminlen=1 */
bool  my_like_range_mb(const CHARSET_INFO *cs,
                       const char *ptr, size_t ptr_length,
                       char escape, char w_one, char w_many,
                       size_t res_length,
                       char *min_str, char *max_str,
                       size_t *min_length, size_t *max_length);

/* For other character sets, with arbitrary mbminlen and mbmaxlen numbers */
bool  my_like_range_generic(const CHARSET_INFO *cs,
                            const char *ptr, size_t ptr_length,
                            char escape, char w_one, char w_many,
                            size_t res_length,
                            char *min_str, char *max_str,
                            size_t *min_length, size_t *max_length);

int my_wildcmp_8bit(const CHARSET_INFO *,
		    const char *str,const char *str_end,
		    const char *wildstr,const char *wildend,
		    int escape, int w_one, int w_many);

int my_wildcmp_bin(const CHARSET_INFO *,
		   const char *str,const char *str_end,
		   const char *wildstr,const char *wildend,
		   int escape, int w_one, int w_many);

size_t my_numchars_8bit(const CHARSET_INFO *, const char *b, const char *e);
size_t my_numcells_8bit(const CHARSET_INFO *, const char *b, const char *e);
size_t my_charpos_8bit(const CHARSET_INFO *, const char *b, const char *e,
                       size_t pos);
size_t my_well_formed_len_8bit(const CHARSET_INFO *, const char *b,
                               const char *e, size_t pos, int *error);
uint my_mbcharlen_8bit(const CHARSET_INFO *, uint c);


/* Functions for multibyte charsets */
extern size_t my_caseup_str_mb(const CHARSET_INFO *, char *);
extern size_t my_casedn_str_mb(const CHARSET_INFO *, char *);
extern size_t my_caseup_mb(const CHARSET_INFO *, char *src, size_t srclen,
                                         char *dst, size_t dstlen);
extern size_t my_casedn_mb(const CHARSET_INFO *, char *src, size_t srclen,
                                         char *dst, size_t dstlen);
extern size_t my_caseup_mb_varlen(const CHARSET_INFO *, char *src,
                                  size_t srclen, char *dst, size_t dstlen);
extern size_t my_casedn_mb_varlen(const CHARSET_INFO *, char *src,
                                  size_t srclen, char *dst, size_t dstlen);
extern size_t my_caseup_ujis(const CHARSET_INFO *, char *src, size_t srclen,
                             char *dst, size_t dstlen);
extern size_t my_casedn_ujis(const CHARSET_INFO *, char *src, size_t srclen,
                             char *dst, size_t dstlen);
extern int my_strcasecmp_mb(const CHARSET_INFO * cs,const char *,
                            const char *);

int my_wildcmp_mb(const CHARSET_INFO *,
		  const char *str,const char *str_end,
		  const char *wildstr,const char *wildend,
		  int escape, int w_one, int w_many);
size_t my_numchars_mb(const CHARSET_INFO *, const char *b, const char *e);
size_t my_numcells_mb(const CHARSET_INFO *, const char *b, const char *e);
size_t my_charpos_mb(const CHARSET_INFO *, const char *b, const char *e,
                     size_t pos);
size_t my_well_formed_len_mb(const CHARSET_INFO *, const char *b,
                             const char *e, size_t pos, int *error);
uint my_instr_mb(const struct charset_info_st *,
                 const char *b, size_t b_length,
                 const char *s, size_t s_length,
                 my_match_t *match, uint nmatch);

int my_strnncoll_mb_bin(const CHARSET_INFO * cs,
                        const uchar *s, size_t slen,
                        const uchar *t, size_t tlen,
                        bool t_is_prefix);

int my_strnncollsp_mb_bin(const CHARSET_INFO *cs,
                          const uchar *a, size_t a_length,
                          const uchar *b, size_t b_length);

int my_wildcmp_mb_bin(const CHARSET_INFO *cs,
                      const char *str,const char *str_end,
                      const char *wildstr,const char *wildend,
                      int escape, int w_one, int w_many);

int my_strcasecmp_mb_bin(const CHARSET_INFO * cs MY_ATTRIBUTE((unused)),
                         const char *s, const char *t);

void my_hash_sort_mb_bin(const CHARSET_INFO *cs MY_ATTRIBUTE((unused)),
                         const uchar *key, size_t len,ulong *nr1, ulong *nr2);

size_t my_strnxfrm_mb(const CHARSET_INFO *,
                      uchar *dst, size_t dstlen, uint nweights,
                      const uchar *src, size_t srclen, uint flags);

size_t my_strnxfrm_unicode(const CHARSET_INFO *,
                           uchar *dst, size_t dstlen, uint nweights,
                           const uchar *src, size_t srclen, uint flags);

size_t my_strnxfrm_unicode_full_bin(const CHARSET_INFO *,
                                    uchar *dst, size_t dstlen, uint nweights,
                                    const uchar *src, size_t srclen, uint flags);
size_t  my_strnxfrmlen_unicode_full_bin(const CHARSET_INFO *, size_t); 

int my_wildcmp_unicode(const CHARSET_INFO *cs,
                       const char *str, const char *str_end,
                       const char *wildstr, const char *wildend,
                       int escape, int w_one, int w_many,
                       const MY_UNICASE_INFO *weights);

extern bool my_parse_charset_xml(MY_CHARSET_LOADER *loader,
                                 const char *buf, size_t buflen);
extern char *my_strchr(const CHARSET_INFO *cs, const char *str,
                       const char *end, char c);
extern size_t my_strcspn(const CHARSET_INFO *cs, const char *str,
                         const char *end, const char *reject,
                         size_t reject_length);

bool my_propagate_simple(const CHARSET_INFO *cs, const uchar *str,
                         size_t len);
bool my_propagate_complex(const CHARSET_INFO *cs, const uchar *str,
                          size_t len);


uint my_string_repertoire(const CHARSET_INFO *cs, const char *str, size_t len);
bool my_charset_is_ascii_based(const CHARSET_INFO *cs);
bool my_charset_is_8bit_pure_ascii(const CHARSET_INFO *cs);
uint my_charset_repertoire(const CHARSET_INFO *cs);


uint my_strxfrm_flag_normalize(uint flags);
size_t my_strxfrm_pad(const CHARSET_INFO *cs,
                      uchar *str, uchar *frmend, uchar *strend,
                      uint nweights, uint flags);

bool my_charset_is_ascii_compatible(const CHARSET_INFO *cs);

const MY_CONTRACTIONS *my_charset_get_contractions(const CHARSET_INFO *cs);

extern size_t my_vsnprintf_ex(const CHARSET_INFO *cs, char *to, size_t n,
                              const char* fmt, va_list ap);

size_t my_convert(char *to, size_t to_length, const CHARSET_INFO *to_cs,
                  const char *from, size_t from_length,
                  const CHARSET_INFO *from_cs, uint *errors);

uint my_mbcharlen_ptr(const CHARSET_INFO *cs, const char *s, const char *e);

#define	_MY_U	01	/* Upper case */
#define	_MY_L	02	/* Lower case */
#define	_MY_NMR	04	/* Numeral (digit) */
#define	_MY_SPC	010	/* Spacing character */
#define	_MY_PNT	020	/* Punctuation */
#define	_MY_CTR	040	/* Control character */
#define	_MY_B	0100	/* Blank */
#define	_MY_X	0200	/* heXadecimal digit */


#define	my_isascii(c)	(!((c) & ~0177))
#define	my_toascii(c)	((c) & 0177)
#define my_tocntrl(c)	((c) & 31)
#define my_toprint(c)	((c) | 64)
#define my_toupper(s,c)	(char) ((s)->to_upper[(uchar) (c)])
#define my_tolower(s,c)	(char) ((s)->to_lower[(uchar) (c)])
#define	my_isalpha(s, c)  (((s)->ctype+1)[(uchar) (c)] & (_MY_U | _MY_L))
#define	my_isupper(s, c)  (((s)->ctype+1)[(uchar) (c)] & _MY_U)
#define	my_islower(s, c)  (((s)->ctype+1)[(uchar) (c)] & _MY_L)
#define	my_isdigit(s, c)  (((s)->ctype+1)[(uchar) (c)] & _MY_NMR)
#define	my_isxdigit(s, c) (((s)->ctype+1)[(uchar) (c)] & _MY_X)
#define	my_isalnum(s, c)  (((s)->ctype+1)[(uchar) (c)] & (_MY_U | _MY_L | _MY_NMR))
#define	my_isspace(s, c)  (((s)->ctype+1)[(uchar) (c)] & _MY_SPC)
#define	my_ispunct(s, c)  (((s)->ctype+1)[(uchar) (c)] & _MY_PNT)
#define	my_isprint(s, c)  (((s)->ctype+1)[(uchar) (c)] & (_MY_PNT | _MY_U | _MY_L | _MY_NMR | _MY_B))
#define	my_isgraph(s, c)  (((s)->ctype+1)[(uchar) (c)] & (_MY_PNT | _MY_U | _MY_L | _MY_NMR))
#define	my_iscntrl(s, c)  (((s)->ctype+1)[(uchar) (c)] & _MY_CTR)

/* Some macros that should be cleaned up a little */
#define my_isvar(s,c)                 (my_isalnum(s,c) || (c) == '_')
#define my_isvar_start(s,c)           (my_isalpha(s,c) || (c) == '_')

#define my_binary_compare(s)	      ((s)->state  & MY_CS_BINSORT)
#define use_strnxfrm(s)               ((s)->state  & MY_CS_STRNXFRM)
#define my_strnxfrm(cs, d, dl, s, sl) \
   ((cs)->coll->strnxfrm((cs), (d), (dl), (dl), (s), (sl), 0))
#define my_strnncoll(s, a, b, c, d) ((s)->coll->strnncoll((s), (a), (b), (c), (d), 0))
#define my_like_range(s, a, b, c, d, e, f, g, h, i, j) \
   ((s)->coll->like_range((s), (a), (b), (c), (d), (e), (f), (g), (h), (i), (j)))
#define my_wildcmp(cs,s,se,w,we,e,o,m) ((cs)->coll->wildcmp((cs),(s),(se),(w),(we),(e),(o),(m)))
#define my_strcasecmp(s, a, b)        ((s)->coll->strcasecmp((s), (a), (b)))
#define my_charpos(cs, b, e, num)     (cs)->cset->charpos((cs), (const char*) (b), (const char *)(e), (num))


#define use_mb(s)                     ((s)->cset->ismbchar != NULL)
#define my_ismbchar(s, a, b)          ((s)->cset->ismbchar((s), (a), (b)))
#define my_mbcharlen(s, a)            ((s)->cset->mbcharlen((s),(a)))
/**
  Get the length of gb18030 code by the given two leading bytes

  @param[in] s charset_info
  @param[in] a first byte of gb18030 code
  @param[in] b second byte of gb18030 code
  @return    the length of gb18030 code starting with given two bytes,
             the length would be 2 or 4 for valid gb18030 code,
             or 0 for invalid gb18030 code
*/
#define my_mbcharlen_2(s, a, b)       ((s)->cset->mbcharlen((s),((((a) & 0xFF) << 8) + ((b) & 0xFF))))
/**
  Get the maximum length of leading bytes needed to determine the length of a
  multi-byte gb18030 code

  @param[in] s charset_info
  @return    number of leading bytes we need, would be 2 for gb18030
             and 1 for all other charsets
*/
#define my_mbmaxlenlen(s)             ((s)->mbmaxlenlen)
/**
  Judge if the given byte is a possible leading byte for a charset.
  For gb18030 whose mbmaxlenlen is 2, we can't determine the length of
  a multi-byte character by looking at the first byte only

  @param[in] s charset_info
  @param[in] i possible leading byte
  @return    true if it is, otherwise false
*/
#define my_ismb1st(s, i)                                           \
   (my_mbcharlen((s), (i)) > 1 ||                                  \
    (my_mbmaxlenlen((s)) == 2 && my_mbcharlen((s), (i)) == 0))

#define my_caseup_str(s, a)           ((s)->cset->caseup_str((s), (a)))
#define my_casedn_str(s, a)           ((s)->cset->casedn_str((s), (a)))
#define my_strntol(s, a, b, c, d, e)  ((s)->cset->strntol((s),(a),(b),(c),(d),(e)))
#define my_strntoul(s, a, b, c, d, e) ((s)->cset->strntoul((s),(a),(b),(c),(d),(e)))
#define my_strntoll(s, a, b, c, d, e) ((s)->cset->strntoll((s),(a),(b),(c),(d),(e)))
#define my_strntoull(s, a, b, c,d, e) ((s)->cset->strntoull((s),(a),(b),(c),(d),(e)))
#define my_strntod(s, a, b, c, d)     ((s)->cset->strntod((s),(a),(b),(c),(d)))

#ifdef	__cplusplus
}
#endif

#endif /* _m_ctype_h */
