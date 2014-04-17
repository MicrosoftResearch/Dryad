///-------------------------------------------------------------------------------------------------
// file:	Validate.cs
//
// summary:	Implements the validate class
///-------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

namespace BenchmarkFramework {

    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Validation utils </summary>
    ///-------------------------------------------------------------------------------------------------

    public class Validate {

        public static void 
        Check<T>(
            IEnumerable<T>[] ss,
            IComparer<T> comparer = null,
            bool sort = true,
            bool verbose = false,
            IComparer<T> sortcomparer = null
            ) {

            if(ss.Length == 0) return;

            if(comparer == null) {
                comparer = Comparer<T>.Default;
                if(comparer == null) {
                    throw new ArgumentNullException("Can't not be null.");
                }
            }
            if(sortcomparer == null)
                sortcomparer = comparer;

            T[][] aa = new T[ss.Length][];
            for(int i = 0; i < aa.Length; i++) {
                aa[i] = ss[i].ToArray();
                if(sort) Array.Sort(aa[i], sortcomparer);
            }
            int len = aa[0].Length;
            for(int i = 1; i < aa.Length; i++) {
                if(aa[i].Length != len) {
                    throw new Exception("Wrong number of elements.");
                }
            }
            for(int i = 0; i < len; i++) {
                T elem = aa[0][i];
                for(int j = 1; j < aa.Length; j++) {
                    if(verbose) {
                        //TestOutput.WriteLine("Comparing {0} to {1}", elem.ToString(), aa[j][i].ToString());
                    }
                    if(comparer.Compare(elem, aa[j][i]) != 0) {
                        throw new Exception("Elements failed to match: " + elem + " != " + aa[j][i]);
                    }
                }
            }
        }

        //public static void 
        //GroupCheck<K, V>(
        //    IEnumerable<IGrouping<K, V>>[] ss,
        //    IComparer<K> kComparer = null,
        //    IComparer<V> vComparer = null
        //    ) {

        //    if(ss.Length == 0) return;

        //    if(kComparer == null) {
        //        kComparer = Comparer<K>.Default;
        //        if(kComparer == null) {
        //            throw new ArgumentNullException("Can't not be null.");
        //        }
        //    }
        //    if(vComparer == null) {
        //        vComparer = Comparer<V>.Default;
        //        if(vComparer == null) {
        //            throw new ArgumentNullException("Can't not be null.");
        //        }
        //    }
        //    IGrouping<K, V>[][] aa = new IGrouping<K, V>[ss.Length][];
        //    for(int i = 0; i < aa.Length; i++) {
        //        aa[i] = ss[i].ToArray();
        //        K[] keys = aa[i].Select(x => x.Key).ToArray();
        //        Array.Sort(keys, aa[i], kComparer);
        //    }
        //    int len = aa[0].Length;
        //    for(int i = 1; i < aa.Length; i++) {
        //        if(aa[i].Length != len) {
        //            throw new Exception("Wrong number of elements.");
        //        }
        //    }
        //    for(int i = 0; i < len; i++) {
        //        IEnumerable<V> elem = aa[0][i];
        //        for(int j = 1; j < aa.Length; j++) {
        //            Check(new IEnumerable<V>[] { elem, aa[j][i] }, vComparer);
        //        }
        //    }
        //}
    }

    /*
    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Tolerant float comparer. Floating point differences between
    ///             GPU and CPU cause the default comparer to fail sometimes even when
    ///             the result is correct. Use this comparer to introduce some tolerance
    ///             for this
    ///             </summary>
    ///
    /// <remarks>   Crossbac, 2/19/2013. </remarks>
    ///-------------------------------------------------------------------------------------------------

    public class TolerantDoubleComparer : IComparer<double> {
        Double EPSILON;
        public TolerantDoubleComparer(Double _epsilon = 0.000001f) {
            EPSILON = _epsilon;
        }
        public int Compare(Double a, Double b) {
            Double delta = a - b;
            if(Math.Abs(delta) <= EPSILON)
                return 0;
            return delta < 0.0f ? -1 : 1;
        }
    }

    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Tolerant float comparer. Floating point differences between
    ///             GPU and CPU cause the default comparer to fail sometimes even when
    ///             the result is correct. Use this comparer to introduce some tolerance
    ///             for this
    ///             </summary>
    ///
    /// <remarks>   Crossbac, 2/19/2013. </remarks>
    ///-------------------------------------------------------------------------------------------------

    public class TolerantFloatComparer : IComparer<float> {
        float EPSILON;
        public TolerantFloatComparer(float _epsilon = 0.000001f) {
            EPSILON = _epsilon;
        }
        public int Compare(float a, float b) {
            float delta = a - b;
            if(Math.Abs(delta) <= EPSILON)
                return 0;
            return delta < 0.0f ? -1 : 1;
        }
    }

    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Tolerant float comparer. Floating point differences between
    ///             GPU and CPU cause the default comparer to fail sometimes even when
    ///             the result is correct. Use this comparer to introduce some tolerance
    ///             for this
    ///             </summary>
    ///
    /// <remarks>   Crossbac, 2/19/2013. </remarks>
    ///-------------------------------------------------------------------------------------------------

    public class TolerantVectorComparer : IComparer<Vector> {
        float EPSILON;
        public TolerantVectorComparer(float _epsilon = 0.0001f) {
            EPSILON = _epsilon;
        }
        public int Compare(Vector a, Vector b) {
            for(int i = 0; i < a.m_elems.Length; i++) {
                float delta = a.m_elems[i] - b.m_elems[i];
                if(Math.Abs(delta) > EPSILON)
                    return delta < 0.0f ? -1 : 1;
            }
            return 0;
        }
    }

    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Interface for epsilon comparable single. </summary>
    ///
    /// <remarks>   Crossbac, 1/16/2014. </remarks>
    ///
    /// <typeparam name="T">    Generic type parameter. </typeparam>
    ///-------------------------------------------------------------------------------------------------

    public interface IEpsilonComparableSingle<T> {
        int EpsilonCompare(T a, T b, float epsilon);
    }

    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Interface for epsilon comparable double. </summary>
    ///
    /// <remarks>   Crossbac, 1/16/2014. </remarks>
    ///
    /// <typeparam name="T">    Generic type parameter. </typeparam>
    ///-------------------------------------------------------------------------------------------------

    public interface IEpsilonComparableDouble<T> {
        int EpsilonCompare(T a, T b, double epsilon);
    }


    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Tolerant float comparer. Floating point differences between
    ///             GPU and CPU cause the default comparer to fail sometimes even when
    ///             the result is correct. Use this comparer to introduce some tolerance
    ///             for this
    ///             </summary>
    ///
    /// <remarks>   Crossbac, 2/19/2013. </remarks>
    ///-------------------------------------------------------------------------------------------------

    public class EpsilonComparer<T> : IComparer<T> where T : IEpsilonComparableSingle<T> {
        float EPSILON;
        public EpsilonComparer(float _epsilon = 0.0001f) {
            EPSILON = _epsilon;
        }
        public int Compare(T a, T b) {
            return a.EpsilonCompare(a, b, EPSILON);
        }
    }


    ///-------------------------------------------------------------------------------------------------
    /// <summary>   Tolerant float comparer for images. Floating point differences between
    ///             GPU and CPU cause the default comparer to fail sometimes even when
    ///             the result is correct. Use this comparer to introduce some tolerance
    ///             for this
    ///             </summary>
    ///
    /// <remarks>   Crossbac, 2/19/2013. </remarks>
    ///-------------------------------------------------------------------------------------------------

    public class TolerantImageComparer : IComparer<Image> {
        float EPSILON;
        public TolerantImageComparer(float _epsilon = 0.0001f) {
            EPSILON = _epsilon;
        }
        public int Compare(Image a, Image b) {
            for(int i = 0; i < a.m_elems.Length; i++) {
                float delta = a.m_elems[i] - b.m_elems[i];
                if(Math.Abs(delta) > EPSILON)
                    return delta < 0.0f ? -1 : 1;
            }
            return 0;
        }
    }

    /// <summary>
    /// Compare two instances of Pair<int,int>.
    /// Implement for concrete type as Pair<T1,T2> cannot straighforwardly implement IComparable.
    /// </summary>
    ///
    /// <remarks>   jcurrey, 3/11/2013. </remarks>
    public class PairIntIntComparer : IComparer<Pair<int, int>> {
        public int Compare(Pair<int, int> a, Pair<int, int> b) {
            int keyComparison = a.Key.CompareTo(b.Key);
            if(keyComparison == 0) {
                return a.Value.CompareTo(b.Value);
            } else {
                return keyComparison;
            }
        }
    }

*/

}
