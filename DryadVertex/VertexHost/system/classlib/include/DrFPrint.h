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
#pragma once
#include <string.h>
#include "basic_types.h"

#undef IndexAssert
#define IndexAssert(expr) LogAssert(expr)
typedef __int64 Dryad_dupelim_fprint_int64_t;
typedef UInt64 Dryad_dupelim_fprint_uint64_t;

/* the type of a 64-bit fingerprint */
typedef Dryad_dupelim_fprint_uint64_t Dryad_dupelim_fprint_t;

/* an opaque type used to keep the data structures need to compute
   fingerprints.  */

typedef struct Dryad_dupelim_fprint_data_s *Dryad_dupelim_fprint_data_t;
typedef const struct Dryad_dupelim_fprint_data_s *Dryad_dupelim_fprint_data_tc;

/* hash lengths */
enum HashPolyLength{
    Poly8bit, 
    Poly16bit,
    Poly32bit,
    Poly64bit} ;

/* Allocate and return a new Rabin fingerprint function.
   Rabin fingerprint belongs to the family of CRC hashes
   Its collusion is bounded by a very small number
   Since it employs polynomials in a galois field, it is very 
   efficient in calculating recursive hashes

   
   for straight-forward applications, use Dryad_dupelim_rabinhash_create() and 
   Dryad_dupelim_rabinhash_process() in your applications. unless you 
   understand what the other functions exactly do, refrain from using them.

   Dryad_dupelim_rabinhash_new()----------------------
   returns true if a hash function is created, 
   fprint data structure should already been allocated
   returns a pointer to the hash function created on pHashfunction
   hashLen is the order of polynomials to be used for the hash function
   seed is the index of the polynomial to be used in the hash function
   seed has to be less than or equal to cbPolysN {N = 8 , 16, 32, 64}
        otherwise, Dryad_dupelim_rabinhash_new will return false
*/
bool Dryad_dupelim_rabinhash_init (Dryad_dupelim_fprint_data_s* pHashData, 
                                HashPolyLength hashLen,
				UInt32 seed);
/* if fp was generated with polynomial P,bytes "data[0, ..., len-1]" 
   contain string A, return the fingerprint under P of A.
   Strings are treated as polynomials.  The low-order bit in the first
   byte is the highest degree coefficient in the polynomial. 
*/
Dryad_dupelim_fprint_t Dryad_dupelim_rabinhash_process(Dryad_dupelim_fprint_data_s* pHashData,
                                                   const unsigned char *data, unsigned len);

/* if fp was generated with polynomial P,bytes "data[0, ..., len-1]" 
   contain string B, and initialHash contains the hash value for string A
   return the fingerprint under P of A added to initialHash.
   the output value is merely the hash of string A concat string B.
   Strings are treated as polynomials.  The low-order bit in the first
   byte is the highest degree coefficient in the polynomial. 
*/

Dryad_dupelim_fprint_t Dryad_dupelim_rabinhash_add(Dryad_dupelim_fprint_data_s* pHashFunction, Dryad_dupelim_fprint_t initialHash,
                                               const unsigned char *data, unsigned len);


/* Allocate and return a new fingerprint function.

   Computes the tables needed for fingerprint manipulations.
   Requires that "poly" be the binary representation
   of an irreducible polynomial in GF(2) of degree 64.   The X^64 term
   is not represented.  The X^0 term is the high order bit, and the
   X^63 term is the low-order bit.

   span is used in later calls to Dryad_dupelim_fprint_slide_word().
   If Dryad_dupelim_fprint_slide_word() is not to be called, span
   should be set to zero. */
Dryad_dupelim_fprint_data_t Dryad_dupelim_fprint_new (Dryad_dupelim_fprint_t poly,
						  unsigned span);

/* Like "new" above, except that the degree can be any value between 1
   and 64.  Return 0 if that's not true.

   The X^(degree-1) term is in the low-order bit of poly.
*/
Dryad_dupelim_fprint_data_t Dryad_dupelim_fprint_new2 (Dryad_dupelim_fprint_t poly,
						   unsigned span, int degree);

/* returns the seeded polynomial ie. fingerprint of an empty element under this fp */
Dryad_dupelim_fprint_t Dryad_dupelim_fprint_empty (Dryad_dupelim_fprint_data_tc fp);

/* if fp was generated with polynomial P, "a" is the fingerprint under
   P of string A, and bytes "data[0, ..., len-1]" contain string B,
   return the fingerprint under P of the concatenation of A and B.
   Strings are treated as polynomials.  The low-order bit in the first
   byte is the highest degree coefficient in the polynomial. This
   routine differs from Dryad_dupelim_fprint_extend_word() in that it
   will read bytes in increasing address order, regardless of the
   endianness of the  machine.  
   data's length is the number of unsigned chars
*/
Dryad_dupelim_fprint_t Dryad_dupelim_fprint_extend (Dryad_dupelim_fprint_data_tc fp,
						Dryad_dupelim_fprint_t a,
                                                const unsigned char *data, unsigned len);

/* If fp was generated with polynomial P, "a" is the fingerprint under
   P of string A, and 64-bit words "data[0, ..., len-1]" contain
   string B, return the fingerprint under P of the concatenation of A
   and B.  Arrays of words are treated as polynomials.  The low-order
   bit in the first word is the highest degree coefficient in the
   polynomial. This routine differs from Dryad_dupelim_fprint_extend()
   on bigendian machines, where the byte order within each word is
   backwards. */
Dryad_dupelim_fprint_t
Dryad_dupelim_fprint_extend_word (Dryad_dupelim_fprint_data_tc fp,
				Dryad_dupelim_fprint_t a,
                const Dryad_dupelim_fprint_uint64_t *data,
				unsigned len);

/* if fp was generated with polynomial P, "a" is the fingerprint under
   P of string A, and "b" is the fingerprint under P of string B,
   which has length "blen" bytes, return the fingerprint under P of
   the concatenation of A and B */
Dryad_dupelim_fprint_t
Dryad_dupelim_fprint_concat(Dryad_dupelim_fprint_data_tc fp,
			  Dryad_dupelim_fprint_t a, Dryad_dupelim_fprint_t
			  b, Dryad_dupelim_fprint_t blen);


/* Turn fingerprint "f" into a hexadecimal, ascii-zero-filled
   printable string S of length 16, and place the characters in
   buf[0,...,15].  No null terminator is written by the routine. */
void Dryad_dupelim_fprint_toascii (Dryad_dupelim_fprint_t f, char *buf);

/* if "fp" was generated with polynomial P, X is some string of length
   "(span-1)*sizeof (Dryad_dupelim_fprint_uint64_t)" bytes  (see
   Dryad_dupelim_fprint_new()), "f" is the fingerprint under P of word
   "a" concatenated with X, return the fingerprint under P of X
   concatenated with word "b".  The words "a" and "b" represent
   polynomials whose X^0 term is in the high-order bit, and whose X^63
   term is in the low order bit.  */
Dryad_dupelim_fprint_t
Dryad_dupelim_fprint_slideword (Dryad_dupelim_fprint_data_tc fp,
			      Dryad_dupelim_fprint_t f,
			      Dryad_dupelim_fprint_uint64_t a,
			      Dryad_dupelim_fprint_uint64_t b);

/* discard the data associated with "fp" */
void Dryad_dupelim_fprint_close (Dryad_dupelim_fprint_data_t fp);

/* fprint struct */
struct Dryad_dupelim_fprint_data_s {
    Dryad_dupelim_fprint_t poly[2];
    /* poly[0] = 0; poly[1] = polynomial */
    Dryad_dupelim_fprint_t empty;
    /* fingerprint of the empty string */
    Dryad_dupelim_fprint_t bybyte[8][256];
    /* bybyte[b][i] is i*X^(degree+8*b) mod poly[1] */
    Dryad_dupelim_fprint_t powers[64];
    /* extend[i] is X^(8*2^i) mod poly[1] */
    static const UInt32 LOGZEROBLOCK = 8;
    static const UInt32 ZEROBLOCK = (1 << LOGZEROBLOCK); 
    union {
        double align;
        unsigned char zeroes[ZEROBLOCK];
    } zeroes;
    Dryad_dupelim_fprint_t bybyte_out[8][256];
    /* bybyte_out[b][i] is i*X^(degree+8*(b+span)) mod poly[1] */
    unsigned span;
};
