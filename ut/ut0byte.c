/*******************************************************************
Byte utilities

(c) 1994, 1995 Innobase Oy

Created 5/11/1994 Heikki Tuuri
********************************************************************/

#include "ut0byte.h"

#ifdef UNIV_NONINL
#include "ut0byte.ic"
#endif

/* Zero value for a dulint */
dulint	ut_dulint_zero		= {0, 0};

/* Maximum value for a dulint */
dulint	ut_dulint_max		= {0xFFFFFFFFUL, 0xFFFFFFFFUL};

#ifdef notdefined /* unused code */
#include "ut0sort.h"

/****************************************************************
Sort function for dulint arrays. */
void
ut_dulint_sort(dulint* arr, dulint* aux_arr, ulint low, ulint high)
/*===============================================================*/
{
#define ut_dulint_sort_ctx(c,a,aux,lo,hi) ut_dulint_sort(a,aux,lo,hi)
#define ut_dulint_cmp_ctx(c,a,b) ut_dulint_cmp(a,b)

	UT_SORT_FUNCTION_BODY(ut_dulint_sort_ctx,, arr, aux_arr, low, high,
			      ut_dulint_cmp_ctx);
}
#endif /* notdefined */
