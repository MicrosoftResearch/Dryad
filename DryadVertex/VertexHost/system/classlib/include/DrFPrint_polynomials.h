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

// This file defines 4 sets of GF[2] irreducible polynomials
// These polynomials are of the order 8, 16, 32 and 64
// You can use these to initialize fprint_data 

static const Dryad_dupelim_fprint_t polys8[] = {
  (Dryad_dupelim_fprint_t)0x00d4UL,
  (Dryad_dupelim_fprint_t)0x00b4UL,
  (Dryad_dupelim_fprint_t)0x00b1UL,
  (Dryad_dupelim_fprint_t)0x00b2UL,
  (Dryad_dupelim_fprint_t)0x0095UL,
  (Dryad_dupelim_fprint_t)0x00afUL,
  (Dryad_dupelim_fprint_t)0x00b2UL,
  (Dryad_dupelim_fprint_t)0x00a6UL,
  (Dryad_dupelim_fprint_t)0x0096UL
};

static const Dryad_dupelim_fprint_t polys16[] = {
  (Dryad_dupelim_fprint_t)0x00009ee6UL,
  (Dryad_dupelim_fprint_t)0x0000dfb5UL,
  (Dryad_dupelim_fprint_t)0x0000e95dUL,
  (Dryad_dupelim_fprint_t)0x0000ab23UL,
  (Dryad_dupelim_fprint_t)0x00009566UL,
  (Dryad_dupelim_fprint_t)0x0000d5e9UL,
  (Dryad_dupelim_fprint_t)0x000086c1UL,
  (Dryad_dupelim_fprint_t)0x000082c3UL,
  (Dryad_dupelim_fprint_t)0x0000a485UL,
  (Dryad_dupelim_fprint_t)0x00008b55UL,
  (Dryad_dupelim_fprint_t)0x00008b4dUL,
  (Dryad_dupelim_fprint_t)0x0000883fUL,
  (Dryad_dupelim_fprint_t)0x0000bd5cUL,
  (Dryad_dupelim_fprint_t)0x0000e87dUL,
  (Dryad_dupelim_fprint_t)0x000082b4UL,
  (Dryad_dupelim_fprint_t)0x0000c036UL,
  (Dryad_dupelim_fprint_t)0x000097e9UL,
  (Dryad_dupelim_fprint_t)0x00009e98UL,
  (Dryad_dupelim_fprint_t)0x000099f9UL,
  (Dryad_dupelim_fprint_t)0x0000fa93UL,
};

static const Dryad_dupelim_fprint_t polys32[] = {
  (Dryad_dupelim_fprint_t)0x8b950699UL,
  (Dryad_dupelim_fprint_t)0xf8c45e2aUL,
  (Dryad_dupelim_fprint_t)0xdfdac578UL,
  (Dryad_dupelim_fprint_t)0x896f6717UL,
  (Dryad_dupelim_fprint_t)0xb2ab5f5dUL,
  (Dryad_dupelim_fprint_t)0xece51013UL,
  (Dryad_dupelim_fprint_t)0xc9ed9c7bUL,
  (Dryad_dupelim_fprint_t)0xb2b28a80UL,
  (Dryad_dupelim_fprint_t)0xb03c9ed2UL,
  (Dryad_dupelim_fprint_t)0x85cd5087UL,
  (Dryad_dupelim_fprint_t)0xcb7d544eUL,
  (Dryad_dupelim_fprint_t)0xf090b664UL,
  (Dryad_dupelim_fprint_t)0xfe442fe2UL,
  (Dryad_dupelim_fprint_t)0x80a0adc0UL,
  (Dryad_dupelim_fprint_t)0x9132521fUL,
  (Dryad_dupelim_fprint_t)0xeca10123UL,
  (Dryad_dupelim_fprint_t)0xf06b52c3UL,
  (Dryad_dupelim_fprint_t)0x87b146b5UL,
  (Dryad_dupelim_fprint_t)0xc6b63122UL,
  (Dryad_dupelim_fprint_t)0xaa109fabUL
};

const Dryad_dupelim_fprint_t polys64[] = {
  (((Dryad_dupelim_fprint_t)0xb40ab24eUL) << 32) | (Dryad_dupelim_fprint_t)0x49737109UL, 
  (((Dryad_dupelim_fprint_t)0xc0398760UL) << 32) | (Dryad_dupelim_fprint_t)0xd3108fd6UL, 
  (((Dryad_dupelim_fprint_t)0xd869093fUL) << 32) | (Dryad_dupelim_fprint_t)0x2ebec587UL, 
  (((Dryad_dupelim_fprint_t)0xa6ab08f8UL) << 32) | (Dryad_dupelim_fprint_t)0x00c128c9UL, 
  (((Dryad_dupelim_fprint_t)0xa629a9c4UL) << 32) | (Dryad_dupelim_fprint_t)0x60a8edfbUL, 
  (((Dryad_dupelim_fprint_t)0xd422e286UL) << 32) | (Dryad_dupelim_fprint_t)0x78b47614UL, 
  (((Dryad_dupelim_fprint_t)0x93facdf9UL) << 32) | (Dryad_dupelim_fprint_t)0xbc1363a2UL, 
  (((Dryad_dupelim_fprint_t)0x93caa3c5UL) << 32) | (Dryad_dupelim_fprint_t)0xdd40d768UL, 
  (((Dryad_dupelim_fprint_t)0xaa53204aUL) << 32) | (Dryad_dupelim_fprint_t)0x7969914eUL, 
  (((Dryad_dupelim_fprint_t)0xe2415fb3UL) << 32) | (Dryad_dupelim_fprint_t)0x440a16bbUL, 
  (((Dryad_dupelim_fprint_t)0xa05f3d02UL) << 32) | (Dryad_dupelim_fprint_t)0x95be208fUL, 
  (((Dryad_dupelim_fprint_t)0xb1e61188UL) << 32) | (Dryad_dupelim_fprint_t)0x6ec27c88UL, 
  (((Dryad_dupelim_fprint_t)0xd6d2bc63UL) << 32) | (Dryad_dupelim_fprint_t)0xc91d290eUL, 
  (((Dryad_dupelim_fprint_t)0xf80f25b8UL) << 32) | (Dryad_dupelim_fprint_t)0xc1930eccUL, 
  (((Dryad_dupelim_fprint_t)0x97dc1fd1UL) << 32) | (Dryad_dupelim_fprint_t)0x15e0e70eUL, 
  (((Dryad_dupelim_fprint_t)0xe17f23cdUL) << 32) | (Dryad_dupelim_fprint_t)0x55fe08aeUL, 
  (((Dryad_dupelim_fprint_t)0xd309c54aUL) << 32) | (Dryad_dupelim_fprint_t)0xe0d66600UL, 
  (((Dryad_dupelim_fprint_t)0xb55bd691UL) << 32) | (Dryad_dupelim_fprint_t)0x17e20f21UL, 
  (((Dryad_dupelim_fprint_t)0x9b19a5d4UL) << 32) | (Dryad_dupelim_fprint_t)0xd4f5ccbeUL, 
  (((Dryad_dupelim_fprint_t)0xcbca35d9UL) << 32) | (Dryad_dupelim_fprint_t)0xab901b9bUL, 
  (((Dryad_dupelim_fprint_t)0x889417edUL) << 32) | (Dryad_dupelim_fprint_t)0x965534ddUL, 
  (((Dryad_dupelim_fprint_t)0x8f27c100UL) << 32) | (Dryad_dupelim_fprint_t)0xbd898837UL, 
  (((Dryad_dupelim_fprint_t)0x930fc2d3UL) << 32) | (Dryad_dupelim_fprint_t)0x4cc207e3UL, 
  (((Dryad_dupelim_fprint_t)0xba0920c3UL) << 32) | (Dryad_dupelim_fprint_t)0xf1c7b364UL, 
  (((Dryad_dupelim_fprint_t)0x80d46b49UL) << 32) | (Dryad_dupelim_fprint_t)0xcfadf5ccUL, 
  (((Dryad_dupelim_fprint_t)0xb45b9d25UL) << 32) | (Dryad_dupelim_fprint_t)0x2b5d6071UL, 
  (((Dryad_dupelim_fprint_t)0x9fe4d82fUL) << 32) | (Dryad_dupelim_fprint_t)0x5fd432d2UL, 
  (((Dryad_dupelim_fprint_t)0xa97d6763UL) << 32) | (Dryad_dupelim_fprint_t)0xd5f818b3UL, 
  (((Dryad_dupelim_fprint_t)0xe8d6b0beUL) << 32) | (Dryad_dupelim_fprint_t)0x7c43649dUL, 
  (((Dryad_dupelim_fprint_t)0xbc673c33UL) << 32) | (Dryad_dupelim_fprint_t)0xfbe55129UL, 
  (((Dryad_dupelim_fprint_t)0xec03ce27UL) << 32) | (Dryad_dupelim_fprint_t)0xf7509ae5UL, 
  (((Dryad_dupelim_fprint_t)0x808401d4UL) << 32) | (Dryad_dupelim_fprint_t)0x40abf627UL, 
  (((Dryad_dupelim_fprint_t)0x95c51b3dUL) << 32) | (Dryad_dupelim_fprint_t)0x387ce64bUL, 
  (((Dryad_dupelim_fprint_t)0xa5a59bd2UL) << 32) | (Dryad_dupelim_fprint_t)0x7d3f452dUL, 
  (((Dryad_dupelim_fprint_t)0xe429f8beUL) << 32) | (Dryad_dupelim_fprint_t)0x22291027UL, 
  (((Dryad_dupelim_fprint_t)0xe4764c26UL) << 32) | (Dryad_dupelim_fprint_t)0x913308e0UL, 
  (((Dryad_dupelim_fprint_t)0xafd52ea1UL) << 32) | (Dryad_dupelim_fprint_t)0x35797bdaUL, 
  (((Dryad_dupelim_fprint_t)0xeb04bdfeUL) << 32) | (Dryad_dupelim_fprint_t)0xa0163482UL, 
  (((Dryad_dupelim_fprint_t)0x9e81f8b8UL) << 32) | (Dryad_dupelim_fprint_t)0xd63a6b87UL, 
  (((Dryad_dupelim_fprint_t)0xd320f803UL) << 32) | (Dryad_dupelim_fprint_t)0x485563aeUL, 
  (((Dryad_dupelim_fprint_t)0x8af88fe4UL) << 32) | (Dryad_dupelim_fprint_t)0x09983363UL, 
  (((Dryad_dupelim_fprint_t)0xd66102feUL) << 32) | (Dryad_dupelim_fprint_t)0xf6ccfe37UL, 
  (((Dryad_dupelim_fprint_t)0xa93e4704UL) << 32) | (Dryad_dupelim_fprint_t)0x3985cda0UL, 
  (((Dryad_dupelim_fprint_t)0x88bf43afUL) << 32) | (Dryad_dupelim_fprint_t)0x43565fa7UL, 
  (((Dryad_dupelim_fprint_t)0xbebb7241UL) << 32) | (Dryad_dupelim_fprint_t)0x360adb47UL, 
  (((Dryad_dupelim_fprint_t)0xd399e12dUL) << 32) | (Dryad_dupelim_fprint_t)0xea25d131UL, 
  (((Dryad_dupelim_fprint_t)0xd03a3d3cUL) << 32) | (Dryad_dupelim_fprint_t)0x20aa87f4UL, 
  (((Dryad_dupelim_fprint_t)0x8111202dUL) << 32) | (Dryad_dupelim_fprint_t)0x77c4b0a8UL, 
  (((Dryad_dupelim_fprint_t)0xc62d960fUL) << 32) | (Dryad_dupelim_fprint_t)0xccc5ba7fUL, 
  (((Dryad_dupelim_fprint_t)0x9d94edd9UL) << 32) | (Dryad_dupelim_fprint_t)0xe31c0833UL, 
  (((Dryad_dupelim_fprint_t)0xa926bc80UL) << 32) | (Dryad_dupelim_fprint_t)0x10d838e0UL, 
  (((Dryad_dupelim_fprint_t)0xf3c8b809UL) << 32) | (Dryad_dupelim_fprint_t)0x89f6395aUL, 
  (((Dryad_dupelim_fprint_t)0x99824e83UL) << 32) | (Dryad_dupelim_fprint_t)0xb5562fbaUL, 
  (((Dryad_dupelim_fprint_t)0xd87d11f3UL) << 32) | (Dryad_dupelim_fprint_t)0xa1ae7f31UL, 
  (((Dryad_dupelim_fprint_t)0xadb9b99eUL) << 32) | (Dryad_dupelim_fprint_t)0x5d44d4eaUL, 
  (((Dryad_dupelim_fprint_t)0xaef654bbUL) << 32) | (Dryad_dupelim_fprint_t)0x644fe26aUL, 
  (((Dryad_dupelim_fprint_t)0xcbf16d7aUL) << 32) | (Dryad_dupelim_fprint_t)0xc4a259e8UL, 
  (((Dryad_dupelim_fprint_t)0x8a1a38ceUL) << 32) | (Dryad_dupelim_fprint_t)0x068a8e79UL, 
  (((Dryad_dupelim_fprint_t)0xfc5207dcUL) << 32) | (Dryad_dupelim_fprint_t)0x711c0a9fUL, 
  (((Dryad_dupelim_fprint_t)0xd30ddb1bUL) << 32) | (Dryad_dupelim_fprint_t)0xa0f02884UL, 
  (((Dryad_dupelim_fprint_t)0xd48fc688UL) << 32) | (Dryad_dupelim_fprint_t)0x376f2998UL, 
  (((Dryad_dupelim_fprint_t)0xa79f0024UL) << 32) | (Dryad_dupelim_fprint_t)0xe168fb6eUL, 
  (((Dryad_dupelim_fprint_t)0x80709fe6UL) << 32) | (Dryad_dupelim_fprint_t)0xa7dd8d6fUL, 
  (((Dryad_dupelim_fprint_t)0xc8771453UL) << 32) | (Dryad_dupelim_fprint_t)0xabb9e8e3UL, 
  (((Dryad_dupelim_fprint_t)0xc9e8268eUL) << 32) | (Dryad_dupelim_fprint_t)0xfb9fd8a3UL, 
  (((Dryad_dupelim_fprint_t)0xc994dbf7UL) << 32) | (Dryad_dupelim_fprint_t)0xc566278eUL, 
  (((Dryad_dupelim_fprint_t)0xddd80109UL) << 32) | (Dryad_dupelim_fprint_t)0xc37bd67bUL, 
  (((Dryad_dupelim_fprint_t)0xa9cc5534UL) << 32) | (Dryad_dupelim_fprint_t)0x8f13c673UL, 
  (((Dryad_dupelim_fprint_t)0xa36d7a45UL) << 32) | (Dryad_dupelim_fprint_t)0xd27bc907UL, 
  (((Dryad_dupelim_fprint_t)0xd7e2a78cUL) << 32) | (Dryad_dupelim_fprint_t)0x66663257UL, 
  (((Dryad_dupelim_fprint_t)0xdd426ee6UL) << 32) | (Dryad_dupelim_fprint_t)0x7c908039UL, 
  (((Dryad_dupelim_fprint_t)0xc80996c7UL) << 32) | (Dryad_dupelim_fprint_t)0x916f5fc8UL, 
  (((Dryad_dupelim_fprint_t)0xf9a6c515UL) << 32) | (Dryad_dupelim_fprint_t)0x3d62dc96UL, 
  (((Dryad_dupelim_fprint_t)0x8267aaa0UL) << 32) | (Dryad_dupelim_fprint_t)0xc80b20a6UL, 
  (((Dryad_dupelim_fprint_t)0xdeb59e2dUL) << 32) | (Dryad_dupelim_fprint_t)0xb3e430a8UL, 
  (((Dryad_dupelim_fprint_t)0xa03fa280UL) << 32) | (Dryad_dupelim_fprint_t)0x2d0318a9UL, 
  (((Dryad_dupelim_fprint_t)0x83b7afb5UL) << 32) | (Dryad_dupelim_fprint_t)0xc47e0dfcUL, 
  (((Dryad_dupelim_fprint_t)0x8752b710UL) << 32) | (Dryad_dupelim_fprint_t)0xe740bfa9UL, 
  (((Dryad_dupelim_fprint_t)0xa6ee843cUL) << 32) | (Dryad_dupelim_fprint_t)0x1df1006eUL, 
  (((Dryad_dupelim_fprint_t)0x814705bfUL) << 32) | (Dryad_dupelim_fprint_t)0x21a7a80eUL, 
  (((Dryad_dupelim_fprint_t)0xf3feedbaUL) << 32) | (Dryad_dupelim_fprint_t)0x611a554dUL, 
  (((Dryad_dupelim_fprint_t)0xdbe78addUL) << 32) | (Dryad_dupelim_fprint_t)0xf2daa748UL, 
  (((Dryad_dupelim_fprint_t)0x961e7a41UL) << 32) | (Dryad_dupelim_fprint_t)0x615851ccUL, 
  (((Dryad_dupelim_fprint_t)0xdb85afd5UL) << 32) | (Dryad_dupelim_fprint_t)0x496a1c1dUL, 
  (((Dryad_dupelim_fprint_t)0xbadd6e78UL) << 32) | (Dryad_dupelim_fprint_t)0x2e2ba8ceUL, 
  (((Dryad_dupelim_fprint_t)0xaf93ef6dUL) << 32) | (Dryad_dupelim_fprint_t)0x2abed356UL, 
  (((Dryad_dupelim_fprint_t)0xc645141aUL) << 32) | (Dryad_dupelim_fprint_t)0xd5794d6cUL, 
  (((Dryad_dupelim_fprint_t)0xd86e9600UL) << 32) | (Dryad_dupelim_fprint_t)0x582cb555UL, 
  (((Dryad_dupelim_fprint_t)0xc39d12b4UL) << 32) | (Dryad_dupelim_fprint_t)0x25fe98a3UL, 
  (((Dryad_dupelim_fprint_t)0x8c346762UL) << 32) | (Dryad_dupelim_fprint_t)0x9a5f7296UL, 
  (((Dryad_dupelim_fprint_t)0x9f373e3cUL) << 32) | (Dryad_dupelim_fprint_t)0x90100d71UL, 
  (((Dryad_dupelim_fprint_t)0xb00c9e7bUL) << 32) | (Dryad_dupelim_fprint_t)0x68d20287UL, 
  (((Dryad_dupelim_fprint_t)0x9f6f838bUL) << 32) | (Dryad_dupelim_fprint_t)0x293b2e4aUL, 
  (((Dryad_dupelim_fprint_t)0xcbd55e6bUL) << 32) | (Dryad_dupelim_fprint_t)0xb5990fdcUL, 
  (((Dryad_dupelim_fprint_t)0xc9ca494cUL) << 32) | (Dryad_dupelim_fprint_t)0x50fcc7c8UL, 
  (((Dryad_dupelim_fprint_t)0xe7e36ad9UL) << 32) | (Dryad_dupelim_fprint_t)0x68b357d0UL, 
  (((Dryad_dupelim_fprint_t)0x88f27f83UL) << 32) | (Dryad_dupelim_fprint_t)0xc0204576UL, 
  (((Dryad_dupelim_fprint_t)0x9b17ad6fUL) << 32) | (Dryad_dupelim_fprint_t)0x4c8a74b2UL, 
  (((Dryad_dupelim_fprint_t)0xe0cfbf08UL) << 32) | (Dryad_dupelim_fprint_t)0x5660db1cUL, 
  (((Dryad_dupelim_fprint_t)0x982f1507UL) << 32) | (Dryad_dupelim_fprint_t)0x9f214ce0UL
}; 
  
const UInt64 cbPolys8 =(sizeof(polys8) / sizeof(polys8[0]));
const UInt64 cbPolys16 =(sizeof(polys16) / sizeof(polys16[0]));
const UInt64 cbPolys32 =(sizeof(polys32) / sizeof(polys32[0]));
const UInt64 cbPolys64 = sizeof (polys64) / sizeof (polys64[0]);

