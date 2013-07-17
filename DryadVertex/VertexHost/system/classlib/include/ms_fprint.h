/*
Copyright (c) Microsoft Corporation

All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
compliance with the License.  You may obtain a copy of the License 
at http://www.apache.org/licenses/LICENSE-2.0   


THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER 
EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF 
TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  


See the Apache Version 2.0 License for specific language governing permissions and 
limitations under the License. 

*/

/* (c) Microsoft Corporation.  All rights reserved. */

#if !defined(_MS_FPRINT_H_)
#define _MS_FPRINT_H_

#if defined(__GNUC__)
typedef unsigned long long ms_fprint_t;
#endif

#if defined(_MSC_VER)
typedef unsigned __int64 ms_fprint_t;
#pragma warning(disable:4127)
#include <stddef.h>
#endif

typedef struct ms_fprint_data_s *ms_fprint_data_t;
/* an opaque type used to keep the data structures need to compute
   fingerprints.  */

ms_fprint_data_t ms_fprint_new ();
/* Computes the tables needed for fingerprint manipulations. */

ms_fprint_data_t ms_fprint_new (ms_fprint_t poly);
/* Computes the tables needed for fingerprint manipulations. */

ms_fprint_t ms_fprint_of (ms_fprint_data_t fp,
			    void *data, size_t len);
/* if fp was generated with polynomial P, and bytes
   "data[0, ..., len-1]" contain string B,
   return the fingerprint under P of the concatenation of B.
   Strings are treated as polynomials.  The low-order bit in the first
   byte is the highest degree coefficient in the polynomial.*/

void ms_fprint_destroy (ms_fprint_data_t fp);
/* discard the data associated with "fp" */

#endif
