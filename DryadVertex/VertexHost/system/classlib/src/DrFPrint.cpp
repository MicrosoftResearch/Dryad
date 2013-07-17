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
#include <stdlib.h>
#include "DrFPrint.h"
#include "DrFPrint_polynomials.h"

#pragma unmanaged

static void initbybyte (Dryad_dupelim_fprint_data_t fp,
			Dryad_dupelim_fprint_t bybyte[][256],
			Dryad_dupelim_fprint_t f) {
    unsigned b;
    for (b = 0; b < 8; b++) {
        unsigned i, i2;
        bybyte[b][0] = 0;
        for (i = 0x80, i2 = 0x100; i > 0; i >>= 1) {
            unsigned j;
            bybyte[b][i] = f;
            for (j = i2; i + j < 256; j += i2)
                bybyte[b][i+j] = f ^ bybyte[b][j];
            i2 = i;
            f = fp->poly[f & 1] ^ (f >> 1);
        }
    }
}

void Dryad_dupelim_fprint_init (Dryad_dupelim_fprint_data_t fp,
			      Dryad_dupelim_fprint_t poly, unsigned span,
			      int degree) {
    Dryad_dupelim_fprint_t l;
    int i;
    fp->poly[0] = 0;
    fp->poly[1] = poly;	/*This must be initialized early on */
    fp->empty = poly;
    fp->span = span;
    initbybyte (fp, fp->bybyte, poly);
    memset (&fp->zeroes, 0, sizeof (fp->zeroes));
    /* The initialization of powers[] must happen after bybyte[][]
       and zeroes are initialized because concat uses all of
       bybyte[][], zeroes and the prefix of powers[] internally. */
    if (degree > 8)
        fp->powers[0] = ((Dryad_dupelim_fprint_t) 1) << (degree - 9);
    else
        fp->powers[0] = fp->bybyte[0][((size_t)0x1) << (8-degree)];
    for (i = 1, l = 1;
         i != sizeof (fp->powers) / sizeof (fp->powers[0]);
         i++, l <<= 1) {
        fp->powers[i] =
            Dryad_dupelim_fprint_concat (fp, fp->powers[i-1] ^ poly, 0, l);
    }
    if (span != 0) {
        initbybyte (fp, fp->bybyte_out,
                    Dryad_dupelim_fprint_concat (fp, 0, 0,
                                               (span-1) * 8));
    }
}

Dryad_dupelim_fprint_data_t Dryad_dupelim_fprint_new (Dryad_dupelim_fprint_t poly,
						  unsigned span ) {
    Dryad_dupelim_fprint_data_t fp = (Dryad_dupelim_fprint_data_t) malloc (sizeof (*fp));
    Dryad_dupelim_fprint_init(fp, poly, span, 64);
    return fp;
}

Dryad_dupelim_fprint_data_t Dryad_dupelim_fprint_new2 (Dryad_dupelim_fprint_t poly,
						   unsigned span, int degree) {
    Dryad_dupelim_fprint_data_t fp;
    if ((degree > 64) || (degree < 1))
        return 0; /* bad choice for degree */
    fp = (Dryad_dupelim_fprint_data_t) malloc (sizeof (*fp));
    Dryad_dupelim_fprint_init(fp, poly, span, degree);
    return fp;
}

Dryad_dupelim_fprint_t Dryad_dupelim_fprint_empty (Dryad_dupelim_fprint_data_tc fp) {
    return (fp->empty);
}

Dryad_dupelim_fprint_t
Dryad_dupelim_fprint_slideword (Dryad_dupelim_fprint_data_tc fp,
			      Dryad_dupelim_fprint_t f,
			      Dryad_dupelim_fprint_uint64_t a,
			      Dryad_dupelim_fprint_uint64_t b ) {
    a ^= fp->poly[1] ^ (((Dryad_dupelim_fprint_t) 1) << 63);
    /* a now also gets rid of the old leading 1, and adds a new
       one */
    f ^=fp->bybyte_out[7][a & 0xff] ^
        fp->bybyte_out[6][(a >> 8) & 0xff] ^
        fp->bybyte_out[5][(a >> 16) & 0xff] ^
        fp->bybyte_out[4][(a >> 24) & 0xff] ^
        fp->bybyte_out[3][(a >> 32) & 0xff] ^
        fp->bybyte_out[2][(a >> 40) & 0xff] ^
        fp->bybyte_out[1][(a >> 48) & 0xff] ^
        fp->bybyte_out[0][a >> 56];
    f ^= b;
    f = fp->bybyte[7][f & 0xff] ^
        fp->bybyte[6][(f >> 8) & 0xff] ^
        fp->bybyte[5][(f >> 16) & 0xff] ^
        fp->bybyte[4][(f >> 24) & 0xff] ^
        fp->bybyte[3][(f >> 32) & 0xff] ^
        fp->bybyte[2][(f >> 40) & 0xff] ^
        fp->bybyte[1][(f >> 48) & 0xff] ^
        fp->bybyte[0][f >> 56];
    return (f);
}

Dryad_dupelim_fprint_t
Dryad_dupelim_fprint_extend_word (Dryad_dupelim_fprint_data_tc fp,
				Dryad_dupelim_fprint_t init,
                                const Dryad_dupelim_fprint_uint64_t *data,
				unsigned len ) {
    unsigned i;
    for (i = 0; i != len; i++) {
        init ^= data[i];
        init =  fp->bybyte[7][init & 0xff] ^
            fp->bybyte[6][(init >> 8) & 0xff] ^
            fp->bybyte[5][(init >> 16) & 0xff] ^
            fp->bybyte[4][(init >> 24) & 0xff] ^
            fp->bybyte[3][(init >> 32) & 0xff] ^
            fp->bybyte[2][(init >> 40) & 0xff] ^
            fp->bybyte[1][(init >> 48) & 0xff] ^
            fp->bybyte[0][init >> 56];
    }
    return (init);
}

Dryad_dupelim_fprint_t
Dryad_dupelim_fprint_extend (Dryad_dupelim_fprint_data_tc fp,
                           Dryad_dupelim_fprint_t init, const unsigned char *data,
			   unsigned len ) {
    unsigned char *p = (unsigned char*) data;
    unsigned char *e = p+len;
    while (p != e && (((Dryad_dupelim_fprint_uint64_t) p) & 7L) != 0) {
        init = (init >> 8) ^ fp->bybyte[0][(init & 0xff) ^ *p++];
    }
    while (p+8 <= e) {
        init ^= *(Dryad_dupelim_fprint_t *)p;
        init =  fp->bybyte[7][init & 0xff] ^
            fp->bybyte[6][(init >> 8) & 0xff] ^
            fp->bybyte[5][(init >> 16) & 0xff] ^
            fp->bybyte[4][(init >> 24) & 0xff] ^
            fp->bybyte[3][(init >> 32) & 0xff] ^
            fp->bybyte[2][(init >> 40) & 0xff] ^
            fp->bybyte[1][(init >> 48) & 0xff] ^
            fp->bybyte[0][init >> 56];
        p += 8;
    }
    
    while (p != e) {
        init = (init >> 8) ^ fp->bybyte[0][(init & 0xff) ^ *p++];
    }
    return (init);
}


Dryad_dupelim_fprint_t
Dryad_dupelim_fprint_concat (Dryad_dupelim_fprint_data_tc fp,
			   Dryad_dupelim_fprint_t a,
			   Dryad_dupelim_fprint_t b,
			   Dryad_dupelim_fprint_t blen) {
    int i;
    Dryad_dupelim_fprint_t x = blen;
    unsigned low = (unsigned)x & ((1 << fp->LOGZEROBLOCK)-1);
    a ^= fp->poly[1];
    if (low != 0) {
        a = Dryad_dupelim_fprint_extend (fp, a, fp->zeroes.zeroes, low);
    }
    x >>= fp->LOGZEROBLOCK;
    i = fp->LOGZEROBLOCK;
    while (x != 0) {
        if (x & 1) {
	    Dryad_dupelim_fprint_t m = 0;
	    Dryad_dupelim_fprint_t bit;
	    Dryad_dupelim_fprint_t e = fp->powers[i];
	    for (bit = ((Dryad_dupelim_fprint_t) 1) << 63; bit != 0; bit >>= 1) {
                if (e & bit) {
                    m ^= a;
                }
                a = (a >> 1) ^ fp->poly[a & 1];
	    }
	    a = m;
        }
        x >>= 1;
        i++;
    }
    return (a ^ b);
}


void Dryad_dupelim_fprint_toascii (Dryad_dupelim_fprint_t f, char *buf) {
    int i;
    for (i = 60; i != -4; i -= 4) {
        *buf++ = "0123456789abcdef"[(f >> i) & 0xf];
    }
}


void Dryad_dupelim_fprint_close (Dryad_dupelim_fprint_data_t fp) {
    free (fp);
}


// rabin hash functions
bool Dryad_dupelim_rabinhash_init (Dryad_dupelim_fprint_data_s* pHashData, 
                                HashPolyLength hashLen,
				UInt32 seed)
{
    UInt64 polySize = 0;
    int degree = 0;
    const Dryad_dupelim_fprint_t* pPolys = NULL;
    switch(hashLen) 
    {
    
    case Poly8bit:
        polySize = cbPolys8;
        pPolys = polys8;
        degree = 8;
        break;
    case Poly16bit:
        polySize = cbPolys16;
        pPolys = polys16;
        degree = 16;
        break;
    case Poly32bit:
        polySize = cbPolys32;
        pPolys = polys32;
        degree = 32;
        break;
    case Poly64bit:
        polySize = cbPolys64;
        pPolys = polys64;
        degree = 64;
        break;
    default:
        return false;
    }

    if (pHashData == NULL)
    {
        return false;
    }

    if (polySize<=seed)
    {
        return false;
    }

    Dryad_dupelim_fprint_init(pHashData,pPolys[seed], 0, degree);
    return true;
}
 
Dryad_dupelim_fprint_t Dryad_dupelim_rabinhash_process(Dryad_dupelim_fprint_data_s* pHashFunction,
                                                   const unsigned char *data, unsigned len)
{
    return Dryad_dupelim_fprint_extend( pHashFunction, Dryad_dupelim_fprint_empty(pHashFunction), data, len);
}

Dryad_dupelim_fprint_t Dryad_dupelim_rabinhash_add(Dryad_dupelim_fprint_data_s* pHashFunction, Dryad_dupelim_fprint_t initialHash,
                                               const unsigned char *data, unsigned len)
{
    return Dryad_dupelim_fprint_extend( pHashFunction, initialHash, data, len);
}


