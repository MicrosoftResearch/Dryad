using Microsoft.Research.DryadLinq;
using Microsoft.Research.DryadLinq.Internal;
using Microsoft.Research.Peloponnese.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;


namespace DryadLinqTests
{
    public class Config
    {
        public static string accountName = @"";
        public static string storageKey = @"";
        public static string containerName = @"";
        public static string cluster = @""; 

        public static string testLogPath = @"";

        public Config(string clusterName, string container, string logPath)
        {
            cluster = clusterName;

            AzureSubscriptions azs = new AzureSubscriptions();
            Task<AzureCluster> clusterTask = azs.GetClusterAsync(clusterName);
            clusterTask.Wait();

            accountName = clusterTask.Result.StorageAccount;
            storageKey = clusterTask.Result.StorageKey;
            containerName = container;
            testLogPath = logPath;
        }
    }

    public class DataGenerator
    {
        public DataGenerator()
        {
        }

        public static IQueryable<int> CreateSimpleFileSetsEx()
        {
            int[] input = { 0, 1, 2 };
            IEnumerable<int> range = input.Apply(x => Enumerable.Range(0, 3)); // {0, 1, 2}
            IEnumerable<int> partitions = range.HashPartition(x => x, 3); // create 3 partitions
            IEnumerable<int> rangePartition = partitions.SelectMany(x => Enumerable.Range(x * 4, 4));

            return rangePartition.AsQueryable();
        }

        public static IEnumerable<IEnumerable<int>> CreateSimpleFileSets()
        {
            IEnumerable<IEnumerable<int>> data = new int[][]
                    { 
                        new[] { 1, 2, 3, 4 }, 
                        new[] { 5, 6, 7, 8 }, 
                        new[] { 9, 10, 11, 12 }, 
                    };
            return data;
        }

        public static IEnumerable<IEnumerable<int>> CreateGroupByReduceDataSet()
        {
            // we need quite a few elements to ensure the combiner will be activated in Stage#1 groupBy.
            // 33 elements per partition should suffice, but 100 per partition is safer.
            IEnumerable<IEnumerable<int>> data = new int[][]
                    { 
                        Enumerable.Range(1,100).ToArray(), 
                        Enumerable.Range(101,100).ToArray(),         
                    };
            return data;
        }

        public static IEnumerable<IEnumerable<int>> CreateRangePartitionDataSet()
        {
            // we need a lot of data to ensure sampler will get some data. 
            // A few thousand should suffice.
            IEnumerable<IEnumerable<int>> data = new int[][]
                    { 
                        Enumerable.Range(1,1000).ToArray(), 
                        Enumerable.Range(20000,2000).ToArray(),         
                        Enumerable.Range(40000,5000).ToArray(),
                    };
            return data;
        }

        public static IQueryable<int> GetSimpleFileSets(DryadLinqContext context)
        {
            //IEnumerable<IEnumerable<int>> data = new int[][]
            //        { 
            //            new[] { 0, 1, 2, 3 }, 
            //            new[] { 4, 5, 6, 7 }, 
            //            new[] { 8, 9, 10, 11}, 
            //        };

            //IQueryable<LineRecord> input = context.FromStore<LineRecord>(AzureUtils.ToAzureUri(Config.accountName, Config.containerName,
            //                                            "unittest/inputdata/SimpleFile.txt"));
            IQueryable<int> input = context.FromEnumerable(new int[1]);
            IQueryable<int> range = input.Apply(x => Enumerable.Range(0, 3)); // {0, 1, 2}
            IQueryable<int> partitions = range.HashPartition(x => x, 3); // create 3 partitions
            IQueryable<int> rangePartition = partitions.SelectMany(x => Enumerable.Range(x * 4, 4));
            //IQueryable<int> store = rangePartition.ToStore(@"unittest/inputdata/SimpleFile.txt");
            return rangePartition;
        }

        public static IQueryable<int> GetGroupByReduceDataSet(DryadLinqContext context)
        {
            //IEnumerable<IEnumerable<int>> data = new int[][] { 
            //            Enumerable.Range(1,100).ToArray(), 
            //            Enumerable.Range(101,100).ToArray(),         
            //    };

            IQueryable<int> input = context.FromEnumerable(new int[1]);
            IQueryable<int> range = input.Apply(x => Enumerable.Range(0, 2)); // {0, 1}
            IQueryable<int> partitions = range.HashPartition(x => x, 2); // create 2 partitions
            IQueryable<int> rangePartition = partitions.SelectMany(x => Enumerable.Range(x * 100 + 1, 100));
            return rangePartition;
        }

        public static IQueryable<int> GetRangePartitionDataSet(DryadLinqContext context)
        {
            // we need a lot of data to ensure sampler will get some data. 
            // A few thousand should suffice.
            //IEnumerable<IEnumerable<int>> data = new int[][] { 
            //            Enumerable.Range(1,1000).ToArray(), 
            //            Enumerable.Range(20000,2000).ToArray(),         
            //            Enumerable.Range(40000,5000).ToArray(),
            //    };

            IQueryable<int> input = context.FromEnumerable(new int[1]);
            IQueryable<int> range = input.Apply(x => Enumerable.Range(0, 3)); // {0, 1, 2}
            IQueryable<int> partitions = range.HashPartition(x => x, 3); // create 3 partitions
            IQueryable<int> rangePartition = partitions.SelectMany(x => Enumerable.Range(x * 20000 + 1, 1000));
            return rangePartition;
        }
    }

    [Serializable]
    public class ReverseComparer<T> : IComparer<T>
    {
        IComparer<T> _originalComparer;
        public ReverseComparer(IComparer<T> originalComparer)
        {
            _originalComparer = originalComparer ?? Comparer<T>.Default;
        }

        public int Compare(T x, T y)
        {
            return (_originalComparer.Compare(y, x)); //note reversed order of operands
        }

        public override bool Equals(object obj)
        {
            ReverseComparer<T> objTyped = obj as ReverseComparer<T>;

            return objTyped != null && _originalComparer.Equals(objTyped._originalComparer);
        }

        public override int GetHashCode()
        {
            // Modify the hash code so that it differs from the hash code for the underlying comparer.
            // It would also probably be good enough to just return _originalComparer.GetHashCode().
            return unchecked((_originalComparer.GetHashCode() + 123457) * 10007);
        }
    }


    public class Utils
    {
        public static bool DeleteFile(string accountName, string accountKey, string containerName, string fileName, bool delSubDirs)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=http;AccountName=" + accountName + ";AccountKey=" + accountKey);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                container.CreateIfNotExists();
                BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
                containerPermissions.PublicAccess = BlobContainerPublicAccessType.Blob;
                container.SetPermissions(containerPermissions);

                if (false == delSubDirs)
                {
                    CloudBlockBlob remoteFile = container.GetBlockBlobReference(fileName);
                    remoteFile.DeleteIfExists();
                }

                if (true == delSubDirs)
                {
                    foreach (IListBlobItem item in container.ListBlobs(fileName, true))
                    {
                        CloudBlockBlob blob = (CloudBlockBlob)item;
                        blob.DeleteIfExists();
                    }
                }
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }
        public static bool FileExists(string accountName, string accountKey, string containerName, string fileName)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=http;AccountName=" + accountName + ";AccountKey=" + accountKey);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                container.CreateIfNotExists();
                BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
                containerPermissions.PublicAccess = BlobContainerPublicAccessType.Blob;
                container.SetPermissions(containerPermissions);

                IEnumerable<IListBlobItem> files = container.ListBlobs(fileName, true);
                return (files.Count() > 0);
            }
            catch (Exception)
            {
                return false;
            }
        }

        internal static DryadLinqContext MakeBasicConfig(string cluster) //???
        {
            var context = new DryadLinqContext(cluster);
            try
            {
                context.JobFriendlyName = "DryadLinq_DevUnitTests";
                context.CompileForVertexDebugging = true;
                context.JobEnvironmentVariables.Add("DummyEnvVar", "hello"); //note: this is consumed by a unit-test.

                if (File.Exists("Microsoft.Hpc.Linq.pdb")) // TODO: fix references
                {
                    context.ResourcesToAdd.Add("Microsoft.Hpc.Linq.pdb");
                }

                if (File.Exists("Microsoft.Hpc.Dsc.Client.pdb"))  // TODO: fix references
                {
                    context.ResourcesToAdd.Add("Microsoft.Hpc.Dsc.Client.pdb");
                }

                // To prevent job from running forever, and blocking other test
                context.JobRuntimeLimit = (int)TimeSpan.FromMinutes(30).TotalSeconds;


                //config.AllowConcurrentUserDelegatesInSingleProcess = false;

                // If we are on Azure, we have to set the nodeGroup to "NodeRole" so that the default of "ComputeNodes" is not used
                // This fixes "FromEnumerableTests" on Azure which queries the active node-group.
                // Note also, the headnode for an azure deployment defaults to "HPCCluster" (at least from James' script)
                int onAzureInt = 0;
                string onAzureString = Environment.GetEnvironmentVariable("CCP_SCHEDULERONAZURE");
                if (onAzureString != null)
                {
                    int.TryParse(onAzureString, out onAzureInt);
                }

                if (onAzureInt == 1)
                {
                    context.NodeGroup = "NodeRole";
                }

            }
            catch (DryadLinqException)
            {
            }
            return context;
        }

        internal static DryadLinqRecordReader<TRecord> MakeDryadRecordReader<TRecord>(DryadLinqContext context, string readPath)
        {
            DryadLinqFactory<TRecord> factory = (DryadLinqFactory<TRecord>)DryadLinqCodeGen.GetFactory(context, typeof(TRecord));
            NativeBlockStream nativeStream = ReflectionHelper.CreateDryadLinqFileStream(readPath, FileMode.Open, FileAccess.Read);
            // ??? NativeBlockStream nativeStream = ReflectionHelper.CreateDryadLinqFileStream(readPath, FileMode.Open, FileAccess.Read, DscCompressionScheme.None); 
            DryadLinqRecordReader<TRecord> reader = factory.MakeReader(nativeStream);
            return reader;
        }
    }

    public class Validate
    {
        public static void
        Check<T>(
            IEnumerable<T>[] ss,
            IComparer<T> comparer = null,
            bool sort = true,
            bool verbose = false,
            IComparer<T> sortcomparer = null
            )
        {

            if (ss.Length == 0) return;

            if (comparer == null)
            {
                comparer = Comparer<T>.Default;
                if (comparer == null)
                {
                    throw new ArgumentNullException("Can't not be null.");
                }
            }
            if (sortcomparer == null)
                sortcomparer = comparer;

            T[][] aa = new T[ss.Length][];
            for (int i = 0; i < aa.Length; i++)
            {
                aa[i] = ss[i].ToArray();
                if (sort) Array.Sort(aa[i], sortcomparer);
            }
            int len = aa[0].Length;
            for (int i = 1; i < aa.Length; i++)
            {
                if (aa[i].Length != len)
                {
                    throw new Exception("Wrong number of elements.");
                }
            }
            for (int i = 0; i < len; i++)
            {
                T elem = aa[0][i];
                for (int j = 1; j < aa.Length; j++)
                {
                    if (verbose)
                    {
                        //TestOutput.WriteLine("Comparing {0} to {1}", elem.ToString(), aa[j][i].ToString());
                    }
                    if (comparer.Compare(elem, aa[j][i]) != 0)
                    {
                        throw new Exception("Elements failed to match: " + elem + " != " + aa[j][i]);
                    }
                }
            }
        }

        internal static bool outFileExists(string outFile)
        {
            try
            {
                return Utils.FileExists(Config.accountName, Config.storageKey, Config.containerName, outFile);
            }
            catch (Exception)
            {
                return false;
            }
        }
    }



    public static class ReflectionHelper
    {
        /// <summary>
        /// Create DryadLinqFileStream object via reflection
        /// </summary>
        ///<param name="parameters"></param>
        /// <returns></returns>
        public static NativeBlockStream CreateDryadLinqFileStream(params object[] parameters)
        {
            return Assembly.LoadWithPartialName("Microsoft.Hpc.Linq").GetType("Microsoft.Hpc.Linq.Internal.HpcLinqFileStream") //???
                   .GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, parameters.Select(p => p.GetType()).ToArray(), null)
                   .Invoke(parameters) as NativeBlockStream;
        }

        private static Type s_errorCodeType = null;
        public static int GetDryadLinqErrorCode(string name)
        {
            if (s_errorCodeType == null)
            {
                Assembly asm = Assembly.Load("Microsoft.Research.DryadLinq");
                Type[] types = asm.GetTypes();
                foreach (var t in types)
                {
                    if (t.Name == "DryadLinqErrorCode")
                    {
                        s_errorCodeType = t;
                        break;
                    }
                }
            }
            var finfo = s_errorCodeType.GetField(name);
            return (int)finfo.GetValue(null);
        }
    }
}
