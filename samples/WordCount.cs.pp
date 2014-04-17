// comment the following line to run on Azure
#define local

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Research.DryadLinq;
using Microsoft.Research.Peloponnese.Storage;

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
			string clusterName = "Replace with your HDInsight 3.0 cluster name";
            // to use the davinci.txt example input below, select your cluster's default
            // storage account and container, which automatically includes the sample text
			string accountName = "Replace with a storage account name";
			string containerName = "Replace with a storage container name";

			// This overload creates an Azure-based computation
            var config = new DryadLinqContext(clusterName);
            config.JobFriendlyName = "DryadLINQ Sample Wordcount";

            // plain text files should be read as type LineRecord
			var input = config.FromStore<LineRecord>(AzureUtils.ToAzureUri(accountName, containerName,
					                                 "example/data/gutenberg/davinci.txt"));
#endif

            var words = input.SelectMany(x => x.Line.Split(' '));
            var groups = words.GroupBy(x => x);
            var counts = groups.Select(x => new KeyValuePair<string, int>(x.Key, x.Count()));
            var toOutput = counts.Select(x => new LineRecord(String.Format("{0}: {1}", x.Key, x.Value)));

#if local
            // any collection computed by the query can be materialized back at the client,
            // not just the 'output' collection. For large collections this is expensive!
            foreach (LineRecord line in toOutput)
            {
                Console.WriteLine(line.Line);
            }
#else
            // the 'true' parameter to ToStore means the output will be over-written if you run
            // the job more than once
            var info = toOutput.ToStore(AzureUtils.ToAzureUri(accountName, containerName,
			           "wc-out.txt"), true).SubmitAndWait();
#endif
        }
    }
}
