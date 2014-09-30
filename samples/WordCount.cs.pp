// define:
//   #define local (and not #define azure) means run on the local computer for local debugging
//   #define azure (and not #define local) means run on an Azure HDInsight 3.1 cluster
//   don't define either azure or local means run on a YARN cluster
#define local
//#define azure

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Research.DryadLinq;
#if azure
using Microsoft.Research.Peloponnese.Azure;
#else
using Microsoft.Research.Peloponnese.Yarn;
#endif

namespace $rootnamespace$
{
    public class WordCount
    {
        public static void WordCountExample()
        {
#if local
			// This overload runs the computation on your local computer using a single worker
            var config = new DryadLinqContext(1);

            var lines = new LineRecord[] { new LineRecord("This is a dummy line for a short job") };
            // You can create inputs from any IEnumerable source using this method
            var input = config.FromEnumerable(lines);
#else
#if azure
			string clusterName = "Replace with your HDInsight 3.1 cluster name";
            // to use the davinci.txt example input below, select your cluster's default
            // storage account and container, which automatically includes the sample text
			string accountName = "Replace with a storage account name";
			string containerName = "Replace with a storage container name";

			// This overload creates an Azure-based computation
            var config = new DryadLinqContext(clusterName);
            config.JobFriendlyName = "DryadLINQ Sample Wordcount";

            // plain text files should be read as type LineRecord
			var input = config.FromStore<LineRecord>(Utils.ToAzureUri(accountName, containerName,
					                                 "example/data/gutenberg/davinci.txt"));
#else
            // to use a yarn cluster, fill in the username, resource node machine name and port, and name node and hdfs port below (use -1 for the default hdfs port).
            string user = "Replace with your username";
            string resourceNode = "Replace with the name of the computer your resource node is running on";
            int rmPort = 8088;
            string nameNode = "Replace with the name of the computer your name node is running on";
            int hdfsPort = -1;
            // set the YARN queue to submit your job on below. Leave null to use the default queue
            string queue = null;
            // set the number of worker containers to start for the DryadLINQ job below
            int numberOfWorkers = 2;
            // set the amount of memory requested for the DryadLINQ job manager container below: 8GB should be enough for even the largest jobs, and 2GB will normally suffice
            int amMemoryMB = 2000;
            // set the amount of memory requested for the DryadLINQ worker containers below. The amount needed will depend on the code you are running
            int workerMemoryMB = 8000;
			// This overload runs the computation on your local computer using a single worker
            var cluster = new DryadLinqYarnCluster(user, numberOfWorkers, amMemoryMB, workerMemoryMB, queue, resourceNode, rmPort, nameNode, hdfsPort);

            var config = new DryadLinqContext(cluster);

            var lines = new LineRecord[] { new LineRecord("This is a dummy line for a short job") };
            // You can create inputs from any IEnumerable source using this method
            var input = config.FromEnumerable(lines);
#endif
#endif

            var words = input.SelectMany(x => x.Line.Split(' '));
            var groups = words.GroupBy(x => x);
            var counts = groups.Select(x => new KeyValuePair<string, int>(x.Key, x.Count()));
            var toOutput = counts.Select(x => new LineRecord(String.Format("{0}: {1}", x.Key, x.Value)));

#if azure
            // the 'true' parameter to ToStore means the output will be over-written if you run
            // the job more than once
            var info = toOutput.ToStore(Utils.ToAzureUri(accountName, containerName,
			           "wc-out.txt"), true).SubmitAndWait();
#else
            // any collection computed by the query can be materialized back at the client,
            // not just the 'output' collection. For large collections this is expensive!
            foreach (LineRecord line in toOutput)
            {
                Console.WriteLine(line.Line);
            }
#endif
        }
    }
}
