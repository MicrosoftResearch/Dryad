using Microsoft.Research.DryadLinq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;


namespace DryadLinqTests
{
    public static class Config
    {
        public static string accountName = @"MyAccountName";
        public static string storageKey = @"MyStorageKey";
        public static string containerName = @"MyContainerName";
        public static string cluster = "MyCcluster";
    }

    public class DataGenerator
    {
        public DataGenerator()
        {
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

                CloudBlockBlob remoteFile = container.GetBlockBlobReference(fileName);
                if (!remoteFile.Exists())
                    return false;
            }
            catch (Exception)
            {
                return false;
            }
            return true;
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
    }
}
