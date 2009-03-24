/*****************************************************************************

Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/**********************************************************************
Utilities for converting data from the database file
to the machine format.

Created 11/28/1995 Heikki Tuuri
***********************************************************************/

#ifndef mach0data_h
#define mach0data_h

#include "univ.i"
#include "ut0byte.h"

/* The data and all fields are always stored in a database file
in the same format: ascii, big-endian, ... .
All data in the files MUST be accessed using the functions in this
module. */

/***********************************************************
The following function is used to store data in one byte. */
UNIV_INLINE
void
mach_write_to_1(
/*============*/
	byte*	b,	/* in: pointer to byte where to store */
	ulint	n);	 /* in: ulint integer to be stored, >= 0, < 256 */
/************************************************************
The following function is used to fetch data from one byte. */
UNIV_INLINE
ulint
mach_read_from_1(
/*=============*/
				/* out: ulint integer, >= 0, < 256 */
	const byte*	b)	/* in: pointer to byte */
	__attribute__((nonnull, pure));
/***********************************************************
The following function is used to store data in two consecutive
bytes. We store the most significant byte to the lower address. */
UNIV_INLINE
void
mach_write_to_2(
/*============*/
	byte*	b,	/* in: pointer to two bytes where to store */
	ulint	n);	 /* in: ulint integer to be stored, >= 0, < 64k */
/************************************************************
The following function is used to fetch data from two consecutive
bytes. The most significant byte is at the lowest address. */
UNIV_INLINE
ulint
mach_read_from_2(
/*=============*/
				/* out: ulint integer, >= 0, < 64k */
	const byte*	b)	/* in: pointer to two bytes */
	__attribute__((nonnull, pure));

/************************************************************
The following function is used to convert a 16-bit data item
to the canonical format, for fast bytewise equality test
against memory. */
UNIV_INLINE
uint16
mach_encode_2(
/*==========*/
			/* out: 16-bit integer in canonical format */
	ulint	n)	/* in: integer in machine-dependent format */
	__attribute__((const));
/************************************************************
The following function is used to convert a 16-bit data item
from the canonical format, for fast bytewise equality test
against memory. */
UNIV_INLINE
ulint
mach_decode_2(
/*==========*/
			/* out: integer in machine-dependent format */
	uint16	n)	/* in: 16-bit integer in canonical format */
	__attribute__((const));
/***********************************************************
The following function is used to store data in 3 consecutive
bytes. We store the most significant byte to the lowest address. */
UNIV_INLINE
void
mach_write_to_3(
/*============*/
	byte*	b,	/* in: pointer to 3 bytes where to store */
	ulint	n);	 /* in: ulint integer to be stored */
/************************************************************
The following function is used to fetch data from 3 consecutive
bytes. The most significant byte is at the lowest address. */
UNIV_INLINE
ulint
mach_read_from_3(
/*=============*/
				/* out: ulint integer */
	const byte*	b)	/* in: pointer to 3 bytes */
	__attribute__((nonnull, pure));
/***********************************************************
The following function is used to store data in four consecutive
bytes. We store the most significant byte to the lowest address. */
UNIV_INLINE
void
mach_write_to_4(
/*============*/
	byte*	b,	/* in: pointer to four bytes where to store */
	ulint	n);	 /* in: ulint integer to be stored */
/************************************************************
The following function is used to fetch data from 4 consecutive
bytes. The most significant byte is at the lowest address. */
UNIV_INLINE
ulint
mach_read_from_4(
/*=============*/
				/* out: ulint integer */
	const byte*	b)	/* in: pointer to four bytes */
	__attribute__((nonnull, pure));
/*************************************************************
Writes a ulint in a compressed form (1..5 bytes). */
UNIV_INLINE
ulint
mach_write_compressed(
/*==================*/
			/* out: stored size in bytes */
	byte*	b,	/* in: pointer to memory where to store */
	ulint	n);	/* in: ulint integer to be stored */
/*************************************************************
Returns the size of an ulint when written in the compressed form. */
UNIV_INLINE
ulint
mach_get_compressed_size(
/*=====================*/
			/* out: compressed size in bytes */
	ulint	n)	/* in: ulint integer to be stored */
	__attribute__((const));
/*************************************************************
Reads a ulint in a compressed form. */
UNIV_INLINE
ulint
mach_read_compressed(
/*=================*/
				/* out: read integer */
	const byte*	b)	/* in: pointer to memory from where to read */
	__attribute__((nonnull, pure));
/***********************************************************
The following function is used to store data in 6 consecutive
bytes. We store the most significant byte to the lowest address. */
UNIV_INLINE
void
mach_write_to_6(
/*============*/
	byte*	b,	/* in: pointer to 6 bytes where to store */
	dulint	n);	 /* in: dulint integer to be stored */
/************************************************************
The following function is used to fetch data from 6 consecutive
bytes. The most significant byte is at the lowest address. */
UNIV_INLINE
dulint
mach_read_from_6(
/*=============*/
				/* out: dulint integer */
	const byte*	b)	/* in: pointer to 6 bytes */
	__attribute__((nonnull, pure));
/***********************************************************
The following function is used to store data in 7 consecutive
bytes. We store the most significant byte to the lowest address. */
UNIV_INLINE
void
mach_write_to_7(
/*============*/
	byte*	b,	/* in: pointer to 7 bytes where to store */
	dulint	n);	 /* in: dulint integer to be stored */
/************************************************************
The following function is used to fetch data from 7 consecutive
bytes. The most significant byte is at the lowest address. */
UNIV_INLINE
dulint
mach_read_from_7(
/*=============*/
				/* out: dulint integer */
	const byte*	b)	/* in: pointer to 7 bytes */
	__attribute__((nonnull, pure));
/***********************************************************
The following function is used to store data in 8 consecutive
bytes. We store the most significant byte to the lowest address. */
UNIV_INLINE
void
mach_write_to_8(
/*============*/
	byte*	b,	/* in: pointer to 8 bytes where to store */
	dulint	n);	/* in: dulint integer to be stored */
/***********************************************************
The following function is used to store data in 8 consecutive
bytes. We store the most significant byte to the lowest address. */
UNIV_INLINE
void
mach_write_ull(
/*===========*/
	byte*		b,	/* in: pointer to 8 bytes where to store */
	ib_uint64_t	n);	/* in: 64-bit integer to be stored */
/************************************************************
The following function is used to fetch data from 8 consecutive
bytes. The most significant byte is at the lowest address. */
UNIV_INLINE
dulint
mach_read_from_8(
/*=============*/
				/* out: dulint integer */
	const byte*	b)	/* in: pointer to 8 bytes */
	__attribute__((nonnull, pure));
/************************************************************
The following function is used to fetch data from 8 consecutive
bytes. The most significant byte is at the lowest address. */
UNIV_INLINE
ib_uint64_t
mach_read_ull(
/*==========*/
				/* out: 64-bit integer */
	const byte*	b)	/* in: pointer to 8 bytes */
	__attribute__((nonnull, pure));
/*************************************************************
Writes a dulint in a compressed form (5..9 bytes). */
UNIV_INLINE
ulint
mach_dulint_write_compressed(
/*=========================*/
			/* out: size in bytes */
	byte*	b,	/* in: pointer to memory where to store */
	dulint	n);	/* in: dulint integer to be stored */
/*************************************************************
Returns the size of a dulint when written in the compressed form. */
UNIV_INLINE
ulint
mach_dulint_get_compressed_size(
/*============================*/
			/* out: compressed size in bytes */
	dulint	 n);	/* in: dulint integer to be stored */
/*************************************************************
Reads a dulint in a compressed form. */
UNIV_INLINE
dulint
mach_dulint_read_compressed(
/*========================*/
				/* out: read dulint */
	const byte*	b)	/* in: pointer to memory from where to read */
	__attribute__((nonnull, pure));
/*************************************************************
Writes a dulint in a compressed form (1..11 bytes). */
UNIV_INLINE
ulint
mach_dulint_write_much_compressed(
/*==============================*/
			/* out: size in bytes */
	byte*	b,	/* in: pointer to memory where to store */
	dulint	n);	/* in: dulint integer to be stored */
/*************************************************************
Returns the size of a dulint when written in the compressed form. */
UNIV_INLINE
ulint
mach_dulint_get_much_compressed_size(
/*=================================*/
			/* out: compressed size in bytes */
	dulint	 n)	 /* in: dulint integer to be stored */
	__attribute__((const));
/*************************************************************
Reads a dulint in a compressed form. */
UNIV_INLINE
dulint
mach_dulint_read_much_compressed(
/*=============================*/
				/* out: read dulint */
	const byte*	b)	/* in: pointer to memory from where to read */
	__attribute__((nonnull, pure));
/*************************************************************
Reads a ulint in a compressed form if the log record fully contains it. */
UNIV_INTERN
byte*
mach_parse_compressed(
/*==================*/
			/* out: pointer to end of the stored field, NULL if
			not complete */
	byte*	ptr,	/* in: pointer to buffer from where to read */
	byte*	end_ptr,/* in: pointer to end of the buffer */
	ulint*	val);	/* out: read value */
/*************************************************************
Reads a dulint in a compressed form if the log record fully contains it. */
UNIV_INTERN
byte*
mach_dulint_parse_compressed(
/*=========================*/
			/* out: pointer to end of the stored field, NULL if
			not complete */
	byte*	ptr,	/* in: pointer to buffer from where to read */
	byte*	end_ptr,/* in: pointer to end of the buffer */
	dulint*	val);	/* out: read value */
#ifndef UNIV_HOTBACKUP
/*************************************************************
Reads a double. It is stored in a little-endian format. */
UNIV_INLINE
double
mach_double_read(
/*=============*/
				/* out: double read */
	const byte*	b)	/* in: pointer to memory from where to read */
	__attribute__((nonnull, pure));
/*************************************************************
Writes a double. It is stored in a little-endian format. */
UNIV_INLINE
void
mach_double_write(
/*==============*/
	byte*	b,	/* in: pointer to memory where to write */
	double	d);	/* in: double */
/*************************************************************
Reads a float. It is stored in a little-endian format. */
UNIV_INLINE
float
mach_float_read(
/*============*/
				/* out: float read */
	const byte*	b)	/* in: pointer to memory from where to read */
	__attribute__((nonnull, pure));
/*************************************************************
Writes a float. It is stored in a little-endian format. */
UNIV_INLINE
void
mach_float_write(
/*=============*/
	byte*	b,	/* in: pointer to memory where to write */
	float	d);	/* in: float */
/*************************************************************
Reads a ulint stored in the little-endian format. */
UNIV_INLINE
ulint
mach_read_from_n_little_endian(
/*===========================*/
					/* out: unsigned long int */
	const byte*	buf,		/* in: from where to read */
	ulint		buf_size)	/* in: from how many bytes to read */
	__attribute__((nonnull, pure));
/*************************************************************
Writes a ulint in the little-endian format. */
UNIV_INLINE
void
mach_write_to_n_little_endian(
/*==========================*/
	byte*	dest,		/* in: where to write */
	ulint	dest_size,	/* in: into how many bytes to write */
	ulint	n);		/* in: unsigned long int to write */
/*************************************************************
Reads a ulint stored in the little-endian format. */
UNIV_INLINE
ulint
mach_read_from_2_little_endian(
/*===========================*/
					/* out: unsigned long int */
	const byte*	buf)		/* in: from where to read */
	__attribute__((nonnull, pure));
/*************************************************************
Writes a ulint in the little-endian format. */
UNIV_INLINE
void
mach_write_to_2_little_endian(
/*==========================*/
	byte*	dest,		/* in: where to write */
	ulint	n);		/* in: unsigned long int to write */

/*************************************************************
Convert integral type from storage byte order (big endian) to
host byte order. */
UNIV_INLINE
ullint
mach_read_int_type(
/*===============*/
					/* out: integer value */
	const byte*	src,		/* in: where to read from */
	ulint		len,		/* in: length of src */
	ibool		unsigned_type);	/* in: signed or unsigned flag */
#endif /* !UNIV_HOTBACKUP */

#ifndef UNIV_NONINL
#include "mach0data.ic"
#endif

#endif
