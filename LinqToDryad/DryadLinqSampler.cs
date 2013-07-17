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

//
// � Microsoft Corporation.  All rights reserved.
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Linq.Expressions;
using System.Linq;
using Microsoft.Research.DryadLinq;
using System.Diagnostics;

namespace Microsoft.Research.DryadLinq.Internal
{
    public static class HpcLinqSampler
    {
        internal const double SAMPLE_RATE = 0.001;  
        private const int MAX_SECOND_PHASE_SAMPLES = 1024*1024;

        [Resource(IsStateful=false)]
        public static IEnumerable<K> Phase1Sampling<T, K>(IEnumerable<T> source,
                                                          Func<T, K> keySelector,
                                                          HpcLinqVertexEnv denv)
        {
            // note: vertexID is constant for each repetition of a specific vertex (eg in fail-and-retry scenarios)
            //       this is very good as it ensure the sampling is idempotent w.r.t. retries.

            long vertexID = HpcLinqNative.GetVertexId(denv.NativeHandle);
            int seed = unchecked((int)(vertexID));
            long nEmitted = 0;

            Random rdm = new Random(seed);
            
            List<K> allSoFar = new List<K>();
            List<K> samples = new List<K>();
            
            // try to collect 10 samples, but keep all the records just in case
            IEnumerator<T> sourceEnumerator = source.GetEnumerator();
            while (sourceEnumerator.MoveNext())
            {
                T elem = sourceEnumerator.Current;
                K key = keySelector(elem);
                allSoFar.Add(key);
                if (rdm.NextDouble() < SAMPLE_RATE)
                {
                    samples.Add(key);
                    if (samples.Count >= 10)
                        break;
                }
            }

            if (samples.Count >= 10)
            {
                // we have lots of samples.. emit them and continue sampling
                allSoFar = null; // not needed.
                foreach (K key in samples)
                {
                    yield return key;
                    nEmitted++;
                }
                while (sourceEnumerator.MoveNext())
                {
                    T elem = sourceEnumerator.Current;
                    if (rdm.NextDouble() < SAMPLE_RATE)
                    {
                        yield return keySelector(elem);
                        nEmitted++;
                    }
                }
            }
            else
            {
                // sampling didn't produce much, so emit all the records instead.
                DryadLinqLog.Add("Sampling produced only {0} records.  Emitting all records instead.", samples.Count());
                Debug.Assert(sourceEnumerator.MoveNext() == false, "The source enumerator wasn't finished");
                samples = null; // the samples list is not needed.
                foreach (K key in allSoFar)
                {
                    yield return key;
                    nEmitted++;
                }
            }

            DryadLinqLog.Add("Stage1 sampling: num keys emitted = {0}", nEmitted);
        }

        //------------------------------------
        //Range-sampler
        // 1. Secondary sampling
        // 2. sort, and select separator values.

        //This method is only used for dynamic inputs. Not required in RTM
        //public static IEnumerable<K> RangeSampler_Dynamic<T, K>(IEnumerable<T> source,
        //                                                Func<T, K> keySelector,
        //                                                IComparer<K> comparer,
        //                                                bool isDescending,
        //                                                HpcLinqVertexEnv denv)
        //{
        //    if (denv.NumberOfArguments < 2)
        //    {
        //        throw new HpcLinqException(SR.Sampler_NotEnoughArgumentsForVertex);
        //    }
        //    Int32 pcount = Int32.Parse(denv.GetArgument(denv.NumberOfArguments-1));
        //    return RangeSamplerCore(source, keySelector, comparer, isDescending, pcount);
        //}

        // used for static plan (ie pcount is determined on client-side and baked into vertex code)
        public static IEnumerable<K>
            RangeSampler_Static<K>(IEnumerable<K> firstPhaseSamples,
                                   IComparer<K> comparer,
                                   bool isDescending,
                                   int pcount)
        {
            return RangeSamplerCore(firstPhaseSamples, comparer, isDescending, pcount);
        }

        public static IEnumerable<K>
            RangeSamplerCore<K>(IEnumerable<K> firstPhaseSamples,
                                IComparer<K> comparer,
                                bool isDescending,
                                int pcount)
        {
            //Reservoir sampling to produce at most MAX_SECOND_PHASE_SAMPLES records.
            K[] samples = new K[MAX_SECOND_PHASE_SAMPLES];
            int inputCount = 0;
            int reservoirCount = 0;

            // fixed-seed is ok here as second-phase-sampler is a singleton vertex. Idempotency is important.
            Random r = new Random(314159);
            
            foreach (K key in firstPhaseSamples)  // this completely enumerates each source in turn.
            {
                if (inputCount < MAX_SECOND_PHASE_SAMPLES)
                {
                    samples[reservoirCount] = key;
                    inputCount++;
                    reservoirCount++;
                }
                else
                {
                    int idx = r.Next(inputCount); // ie a number between 0..inputCount-1 inclusive.
                    if (idx < MAX_SECOND_PHASE_SAMPLES)
                    {
                        samples[idx] = key;
                    }
                    inputCount++;
                }
                
            }

            // Sort and Emit the keys
            Array.Sort(samples, 0, reservoirCount, comparer);

            DryadLinqLog.Add("Range-partition separator keys: ");
            DryadLinqLog.Add("samples: {0}", reservoirCount);
            DryadLinqLog.Add("pCount:  {0}", pcount);

            if (reservoirCount == 0)
            {
                //DryadLinqLog.Add("  case: cnt==0.  No separators produced.");
                yield break;
            }

            if (reservoirCount < pcount)
            {
                //DryadLinqLog.Add("  case: cnt < pcount");
                if (isDescending)
                {
                    //DryadLinqLog.Add("  case: isDescending=true");
                    for (int i = reservoirCount - 1; i >= 0; i--)
                    {
                        //DryadLinqLog.Add("  [{0}]", samples[i]);
                        yield return samples[i];
                    }
                    K first = samples[0];
                    for (int i = reservoirCount; i < pcount - 1; i++)
                    {
                        //DryadLinqLog.Add("  [{0}]", first);
                        yield return first;
                    }
                }
                else
                {
                    //DryadLinqLog.Add("  case: isDescending=false");
                    for (int i = 0; i < reservoirCount; i++)
                    {
                        //DryadLinqLog.Add("  [{0}]", samples[i]);
                        yield return samples[i];
                    }
                    K last = samples[reservoirCount - 1];
                    for (int i = reservoirCount; i < pcount - 1; i++)
                    {
                        //DryadLinqLog.Add("  [{0}]", last);
                        yield return last;
                    }
                }
            }
            else
            {
                //DryadLinqLog.Add("  case: cnt >= pcount");
                int intv = reservoirCount / pcount;
                if (isDescending)
                {
                    //DryadLinqLog.Add("  case: isDescending=true");
                    int idx = reservoirCount - intv;
                    for (int i = 0; i < pcount-1; i++)
                    {
                        //DryadLinqLog.Add("  [{0}]", samples[idx]);
                        yield return samples[idx];
                        idx -= intv;
                    }                        
                }
                else
                {
                    //DryadLinqLog.Add("  case: isDescending=false");
                    int idx = intv;
                    for (int i = 0; i < pcount-1; i++)
                    {
                        //DryadLinqLog.Add("  [{0}]", samples[idx]);
                        yield return samples[idx];
                        idx += intv;
                    }
                }
            }
        }
    }
}
