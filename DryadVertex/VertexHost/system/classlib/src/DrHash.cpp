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

#include "DrHash.h"

#pragma unmanaged

/*
 * Copied and modified from http://burtleburtle.net/bob/c/lookup3.c,
 * where it is Public Domain.
 */

#define UPPER(c) ((UInt32)((((c) >= 'a') && ((c) <= 'z')) ? (c) - ('a' - 'A') : (c)))


// Compute2: Compute two hashes of an array of bytes.
// The first hash is slightly better mixed than the second hash.
void DrHash32::Compute2 (
    const void *pData,  // byte array to hash; may be null if uSize==0
    Size_t      uSize,  // length of pData
    UInt32      uSeed1, // first seed
    UInt32      uSeed2, // second seed
    UInt32     *uHash1, // OUT: first hash (may not be null)
    UInt32     *uHash2) // OUT: second hash (may not be null)
{
    UInt32 a,b,c;

    // Set up the internal state
    a = b = c = 0xdeadbeef + ((UInt32)uSize) + uSeed1;
    c += uSeed2;

    if (((((UInt8 *)pData)-(UInt8 *)0) & 0x3) == 0) {
        const UInt32 *k = (const UInt32 *)pData;       // read 32-bit chunks
        const UInt8  *k1;

        // all but last block: aligned reads and affect 32 bits of (a,b,c)
        while (uSize > 12)
        {
            a += k[0];
            b += k[1];
            c += k[2];
            Mix(a,b,c);
            uSize -= 12;
            k += 3;
        }

        // handle the last (probably partial) block
        k1 = (const UInt8 *)k;
        switch(uSize)
        {
        case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
        case 11: c+=((UInt32)k1[10])<<16;  // fall through
        case 10: c+=((UInt32)k1[9])<<8;    // fall through
        case 9 : c+=(UInt32)k1[8];         // fall through
        case 8 : b+=k[1]; a+=k[0]; break;
        case 7 : b+=((UInt32)k1[6])<<16;   // fall through
        case 6 : b+=((UInt32)k1[5])<<8;    // fall through
        case 5 : b+=((UInt32)k1[4]);       // fall through
        case 4 : a+=k[0]; break;
        case 3 : a+=((UInt32)k1[2])<<16;   // fall through
        case 2 : a+=((UInt32)k1[1])<<8;    // fall through
        case 1 : a+=k1[0]; break;
        case 0 : 
            *uHash1 = c;
            *uHash2 = b;
            return;
        }

    } else if (((((UInt8 *)pData)-(UInt8 *)0) & 0x1) == 0) {
        const UInt16 *k = (const UInt16 *) pData;   // read 16-bit chunks
        const UInt8  *k1;

        // all but last block: aligned reads and different mixing
        while (uSize > 12)
        {
            a += k[0] + (((UInt32)k[1])<<16);
            b += k[2] + (((UInt32)k[3])<<16);
            c += k[4] + (((UInt32)k[5])<<16);
            Mix(a,b,c);
            uSize -= 12;
            k += 6;
        }

        // handle the last (probably partial) block
        k1 = (const UInt8 *)k;
        switch(uSize)
        {
        case 12: c+=k[4]+(((UInt32)k[5])<<16);
            b+=k[2]+(((UInt32)k[3])<<16);
            a+=k[0]+(((UInt32)k[1])<<16);
            break;
        case 11: c+=((UInt32)k1[10])<<16;        // fall through
        case 10: c+=k[4];
            b+=k[2]+(((UInt32)k[3])<<16);
            a+=k[0]+(((UInt32)k[1])<<16);
            break;
        case 9 : c+=k1[8];                       // fall through
        case 8 : b+=k[2]+(((UInt32)k[3])<<16);
            a+=k[0]+(((UInt32)k[1])<<16);
            break;
        case 7 : b+=((UInt32)k1[6])<<16;         // fall through
        case 6 : b+=k[2];
            a+=k[0]+(((UInt32)k[1])<<16);
            break;
        case 5 : b+=k1[4];                       // fall through
        case 4 : a+=k[0]+(((UInt32)k[1])<<16);
            break;
        case 3 : a+=((UInt32)k1[2])<<16;         // fall through
        case 2 : a+=k[0];
            break;
        case 1 : a+=k1[0];
            break;
        case 0 :
            *uHash1 = c;
            *uHash2 = b;
            return;
        }

    } else {                      // need to read the key one byte at a time
        const UInt8 *k = (const UInt8 *)pData;

        // all but the last block: affect some 32 bits of (a,b,c)
        while (uSize > 12)
        {
            a += k[0];
            a += ((UInt32)k[1])<<8;
            a += ((UInt32)k[2])<<16;
            a += ((UInt32)k[3])<<24;
            b += k[4];
            b += ((UInt32)k[5])<<8;
            b += ((UInt32)k[6])<<16;
            b += ((UInt32)k[7])<<24;
            c += k[8];
            c += ((UInt32)k[9])<<8;
            c += ((UInt32)k[10])<<16;
            c += ((UInt32)k[11])<<24;
            Mix(a,b,c);
            uSize -= 12;
            k += 12;
        }

        // last block: affect all 32 bits of (c)
        switch(uSize)                // all the case statements fall through
        {
        case 12: c+=((UInt32)k[11])<<24;
        case 11: c+=((UInt32)k[10])<<16;
        case 10: c+=((UInt32)k[9])<<8;
        case 9 : c+=k[8];
        case 8 : b+=((UInt32)k[7])<<24;
        case 7 : b+=((UInt32)k[6])<<16;
        case 6 : b+=((UInt32)k[5])<<8;
        case 5 : b+=k[4];
        case 4 : a+=((UInt32)k[3])<<24;
        case 3 : a+=((UInt32)k[2])<<16;
        case 2 : a+=((UInt32)k[1])<<8;
        case 1 : a+=k[0];
            break;
        case 0 :
            *uHash1 = c;
            *uHash2 = b;
            return;
        }
    }

    Final(a,b,c);
    *uHash1 = c;
    *uHash2 = b;
    return;
}


// Hash a string of unknown length case sensitive.  I can't just call
// Compute() without allocating a copy of the string, which could have
// complications because there's no max length for strings.
void DrHash32::StringI2 (
    const char *pString,
    Size_t      uSize,
    UInt32      uSeed1,
    UInt32      uSeed2,
    UInt32     *uHash1,
    UInt32     *uHash2)
{
    UInt32       a,b,c;
    const UInt8 *k;
    
    k = (const UInt8 *) pString;

    // Set up the internal state
    a = b = c = 0xdeadbeef + ((UInt32)uSize) + uSeed1;
    c += uSeed2;

    // all but the last block: affect some 32 bits of (a,b,c)
    while (uSize > 12)
    {
        a += UPPER(k[0]);
        a += UPPER(k[1])<<8;
        a += UPPER(k[2])<<16;
        a += UPPER(k[3])<<24;
        b += UPPER(k[4]);
        b += UPPER(k[5])<<8;
        b += UPPER(k[6])<<16;
        b += UPPER(k[7])<<24;
        c += UPPER(k[8]);
        c += UPPER(k[9])<<8;
        c += UPPER(k[10])<<16;
        c += UPPER(k[11])<<24;
        Mix(a,b,c);
        uSize -= 12;
        k += 12;
    }
    
    // last block: affect all 32 bits of (c)
    switch(uSize)                    // all the case statements fall through
    {
    case 12: c+=UPPER(k[11])<<24;
    case 11: c+=UPPER(k[10])<<16;
    case 10: c+=UPPER(k[9])<<8;
    case 9 : c+=UPPER(k[8]);
    case 8 : b+=UPPER(k[7])<<24;
    case 7 : b+=UPPER(k[6])<<16;
    case 6 : b+=UPPER(k[5])<<8;
    case 5 : b+=UPPER(k[4]);
    case 4 : a+=UPPER(k[3])<<24;
    case 3 : a+=UPPER(k[2])<<16;
    case 2 : a+=UPPER(k[1])<<8;
    case 1 : a+=UPPER(k[0]);
        break;
    case 0 :
        *uHash1 = c;
        *uHash2 = b;
        return;
    }
    
    Final(a,b,c);
    *uHash1 = c;
    *uHash2 = b;
    return;
}



//
// Self-test to check that the hash behaves as advertized
// Or you can plug your favorite hash in here and see how it fares!
//
#if 0

#include <time.h>

// used for timings
static void driver1()
{
  UInt8 buf[256];
  UInt32 i;
  UInt64 h=0;
  time_t a,z;

  time(&a);
  for (i=0; i<256; ++i) buf[i] = 'x';

  // increase the loop size until you can measure wall-clock time taken
  for (i=0; i<1; ++i) 
  {
      h = DrHash64::Compute(&buf[0],1,h);
  }
  time(&z);
  if (z-a > 0) printf("time %ld %.8x\n", z-a, h);
}

// check that every input bit changes every output bit half the time
#define HASHSTATE 1
#define HASHLEN   1
#define MAXPAIR 60
#define MAXLEN  70
static void driver2()
{
    UInt8 qa[MAXLEN+1], qb[MAXLEN+2], *a = &qa[0], *b = &qb[1];
    UInt64 c[HASHSTATE], d[HASHSTATE];
    UInt32 i=0, j=0, k, l, m=0, z;
    UInt64 e[HASHSTATE],f[HASHSTATE],g[HASHSTATE],h[HASHSTATE];
    UInt64 x[HASHSTATE],y[HASHSTATE];
    Size_t hlen;
    
    printf("No more than %d trials should ever be needed \n",MAXPAIR/2);
    for (hlen=0; hlen < MAXLEN; ++hlen)
    {
        z=0;
        for (i=0; i<hlen; ++i)  // for each input byte,
        {
            for (j=0; j<8; ++j)   // for each input bit,
            {
                for (m=1; m<8; ++m) // for serveral possible initvals,
                {
                    for (l=0; l<HASHSTATE; ++l)
                        e[l]=f[l]=g[l]=h[l]=x[l]=y[l]=~((UInt32)0);
                    
                    // check that every output bit is affected by that input bit
                    for (k=0; k<MAXPAIR; k+=2)
                    { 
                        UInt32 finished=1;

                        // keys have one bit different
                        for (l=0; l<hlen+1; ++l) {a[l] = b[l] = (UInt8)0;}

                        // have a and b be two keys differing in only one bit
                        a[i] ^= (k<<j);
                        a[i] ^= (k>>(8-j));
                        c[0] = DrHash64::Compute(a, hlen, m);
                        b[i] ^= ((k+1)<<j);
                        b[i] ^= ((k+1)>>(8-j));
                        d[0] = DrHash64::Compute(b, hlen, m);
                        // check every bit is 1, 0, set, and not set at least once
                        for (l=0; l<HASHSTATE; ++l)
                        {
                            e[l] &= (c[l]^d[l]);
                            f[l] &= ~(c[l]^d[l]);
                            g[l] &= c[l];
                            h[l] &= ~c[l];
                            x[l] &= d[l];
                            y[l] &= ~d[l];
                            if (e[l]|f[l]|g[l]|h[l]|x[l]|y[l]) finished=0;
                        }
                        if (finished) break;
                    }
                    if (k>z) z=k;
                    if (k==MAXPAIR) 
                    {
                        printf("Some bit didn't change: ");
                        printf("%.8x.8x %.8x.8x %.8x.8x %.8x.8x %.8x.8x %.8x.8x  ",
                               (UInt32)(e[0] >> 32), (UInt32)e[0],
                               (UInt32)(f[0] >> 32), (UInt32)f[0],
                               (UInt32)(g[0] >> 32), (UInt32)g[0],
                               (UInt32)(h[0] >> 32), (UInt32)h[0],
                               (UInt32)(x[0] >> 32), (UInt32)x[0],
                               (UInt32)(y[0] >> 32), (UInt32)y[0]);
                        printf("i %ld j %ld m %ld len %ld\n",i,j,m,hlen);
                    }
                    if (z==MAXPAIR) goto done;
                }
            }
        }
    done:
        if (z < MAXPAIR)
        {
            printf("Mix success  %2ld bytes  %2ld initvals  ",i,m);
            printf("required  %ld  trials\n",z/2);
        }
    }
    printf("\n");
}

// Check for reading beyond the end of the buffer and alignment problems
static void driver3()
{
    UInt8 buf[MAXLEN+20], *b;
    UInt32 len;
    UInt8 q[] = "This is the time for all good men to come to the aid of their country...";
    UInt32 h;
    UInt8 qq[] = "xThis is the time for all good men to come to the aid of their country...";
    UInt32 i;
    UInt8 qqq[] = "xxThis is the time for all good men to come to the aid of their country...";
    UInt32 j;
    UInt8 qqqq[] = "xxxThis is the time for all good men to come to the aid of their country...";
    UInt64 ref,x,y;
    UInt8 *p;
    
    printf("Endianness.  These lines should all be the same (for values filled in):\n");
    p = q;
    printf("%.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x\n",
           DrHash32::Compute(p, sizeof(q)-1, 13), 
           DrHash32::Compute(p, sizeof(q)-2, 13),
           DrHash32::Compute(p, sizeof(q)-3, 13), 
           DrHash32::Compute(p, sizeof(q)-4, 13),
           DrHash32::Compute(p, sizeof(q)-5, 13),
           DrHash32::Compute(p, sizeof(q)-6, 13),
           DrHash32::Compute(p, sizeof(q)-7, 13), 
           DrHash32::Compute(p, sizeof(q)-8, 13),
           DrHash32::Compute(p, sizeof(q)-9, 13), 
           DrHash32::Compute(p, sizeof(q)-10, 13),
           DrHash32::Compute(p, sizeof(q)-11, 13), 
           DrHash32::Compute(p, sizeof(q)-12, 13));
    p = &qq[1];
    printf("%.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x\n",
           DrHash32::Compute(p, sizeof(q)-1, 13), 
           DrHash32::Compute(p, sizeof(q)-2, 13),
           DrHash32::Compute(p, sizeof(q)-3, 13), 
           DrHash32::Compute(p, sizeof(q)-4, 13),
           DrHash32::Compute(p, sizeof(q)-5, 13), 
           DrHash32::Compute(p, sizeof(q)-6, 13),
           DrHash32::Compute(p, sizeof(q)-7, 13),
           DrHash32::Compute(p, sizeof(q)-8, 13),
           DrHash32::Compute(p, sizeof(q)-9, 13), 
           DrHash32::Compute(p, sizeof(q)-10, 13),
           DrHash32::Compute(p, sizeof(q)-11, 13),
           DrHash32::Compute(p, sizeof(q)-12, 13));
    p = &qqq[2];
    printf("%.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x\n",
           DrHash32::Compute(p, sizeof(q)-1, 13), 
           DrHash32::Compute(p, sizeof(q)-2, 13),
           DrHash32::Compute(p, sizeof(q)-3, 13), 
           DrHash32::Compute(p, sizeof(q)-4, 13),
           DrHash32::Compute(p, sizeof(q)-5, 13), 
           DrHash32::Compute(p, sizeof(q)-6, 13),
           DrHash32::Compute(p, sizeof(q)-7, 13),
           DrHash32::Compute(p, sizeof(q)-8, 13),
           DrHash32::Compute(p, sizeof(q)-9, 13), 
           DrHash32::Compute(p, sizeof(q)-10, 13),
           DrHash32::Compute(p, sizeof(q)-11, 13),
           DrHash32::Compute(p, sizeof(q)-12, 13));
    p = &qqqq[3];
    printf("%.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x %.8x\n",
           DrHash32::Compute(p, sizeof(q)-1, 13),
           DrHash32::Compute(p, sizeof(q)-2, 13),
           DrHash32::Compute(p, sizeof(q)-3, 13),
           DrHash32::Compute(p, sizeof(q)-4, 13),
           DrHash32::Compute(p, sizeof(q)-5, 13),
           DrHash32::Compute(p, sizeof(q)-6, 13),
           DrHash32::Compute(p, sizeof(q)-7, 13),
           DrHash32::Compute(p, sizeof(q)-8, 13),
           DrHash32::Compute(p, sizeof(q)-9, 13),
           DrHash32::Compute(p, sizeof(q)-10, 13),
           DrHash32::Compute(p, sizeof(q)-11, 13),
           DrHash32::Compute(p, sizeof(q)-12, 13));
    printf("\n");
    for (h=0, b=buf+1; h<8; ++h, ++b)
    {
        for (i=0; i<MAXLEN; ++i)
        {
            len = i;
            for (j=0; j<i; ++j) *(b+j)=0;
            
            // these should all be equal
            ref = DrHash64::Compute(b, len, 1);
            *(b+i)=(UInt8)~0;
            *(b-1)=(UInt8)~0;
            x = DrHash64::Compute(b, len, 1);
            y = DrHash64::Compute(b, len, 1);
            if ((ref != x) || (ref != y)) 
            {
                printf("alignment error: %.8x.8x  %.8x.8x  %.8x.8x %ld %ld\n",
                       (UInt32)(ref >> 32), (UInt32)ref,
                       (UInt32)(x >> 32), (UInt32)x,
                       (UInt32)(y >> 32), (UInt32)y,
                       h,i);
            }
        }
    }
}

// check for problems with nulls
static void driver4()
{
    UInt32 i;
    UInt64 h,state[HASHSTATE];
    
    
    for (i=0; i<HASHSTATE; ++i) state[i] = 1;
    printf("These should all be different\n");
    for (i=0, h=0; i<8; ++i)
    {
        h = DrHash64::Compute((const void *)0, 0, h);
        printf("%2ld  0-byte strings, hash is  %.8x%.8x\n", 
               i, (UInt32)(h >> 32), (UInt32)h);
    }
}

// Check that StringI really is case insensitive 
// and equivalent to Compute on an uppercased string
static void driver5()
{
    const char x1[] = "mares eat oats and does eat oats and little lambs eat ivy\n";
    const char x2[] = "Mares Eat Oats And Does Eat Oats And Little Lambs Eat Ivy\n";
    const char x3[] = "MARES EAT OATS AND DOES EAT OATS AND LITTLE LAMBS EAT IVY\n";
    const char y1[] = "bob";
    const char y2[] = "Bob";
    const char y3[] = "BOB";
    printf("\nStringI: Columns are the same, rows are different\n");
    printf("%.8x%.8x  %.8x%.8x\n",
           (UInt32)(DrHash64::StringI( x1, strlen(x1), 666) >> 32),
           (UInt32)(DrHash64::StringI( x1, strlen(x1), 666)),
           (UInt32)(DrHash64::StringI( y1, strlen(y1), 666) >> 32),
           (UInt32)(DrHash64::StringI( y1, strlen(y1), 666)));
    printf("%.8x%.8x  %.8x%.8x\n",
           (UInt32)(DrHash64::StringI( x2, strlen(x2), 666) >> 32),
           (UInt32)(DrHash64::StringI( x2, strlen(x2), 666)),
           (UInt32)(DrHash64::StringI( y2, strlen(y2), 666) >> 32),
           (UInt32)(DrHash64::StringI( y2, strlen(y2), 666)));
    printf("%.8x%.8x  %.8x%.8x\n",
           (UInt32)(DrHash64::StringI( x3, strlen(x3), 666) >> 32),
           (UInt32)(DrHash64::StringI( x3, strlen(x3), 666)),
           (UInt32)(DrHash64::StringI( y3, strlen(y3), 666) >> 32),
           (UInt32)(DrHash64::StringI( y3, strlen(y3), 666)));
    printf("%.8x%.8x  %.8x%.8x\n",
           (UInt32)(DrHash64::Compute( (const void *)x3, strlen(x3), 666) >> 32),
           (UInt32)(DrHash64::Compute( (const void *)x3, strlen(x3), 666)),
           (UInt32)(DrHash64::Compute( (const void *)y3, strlen(y3), 666) >> 32),
           (UInt32)(DrHash64::Compute( (const void *)y3, strlen(y3), 666)));
}

int __cdecl main(int argc, char **argv)
{
    driver1();   // test that the key is hashed: used for timings
    driver2();   // test that whole key is hashed thoroughly
    driver3();   // test that nothing but the key is hashed
    driver4();   // test hashing multiple buffers (all buffers are null)
    driver5();   // test that StringI really is case insensitive
    return 0;
}


#endif
