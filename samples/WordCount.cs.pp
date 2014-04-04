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
			string accountName = "Replace with your account name";
			string storageKey = "Replace with your storage key";
			string containerName = "Replace with the default container name for your HDInsight cluster";
			
			// If you have multiple HDInsight clusters, modify the DryadLinqContext to include the clusterName
            var config = new DryadLinqContext(accountName, storageKey, containerName);            
			
			// the LocalExecution flag determines if the computation is run locally or on a cluster
			config.LocalExecution = false;
			
			var input = config.FromStore<LineRecord>(AzureUtils.ToAzureUri(accountName, storageKey, containerName, 
					                                 "example/data/gutenberg/davinci.txt"));
            var words = input.SelectMany(x => x.Line.Split(' '));
            var groups = words.GroupBy(x => x);
            var counts = groups.Select(x => new KeyValuePair<string, int>(x.Key, x.Count()));
            var toOutput = counts.Select(x => new LineRecord(String.Format("{0}: {1}", x.Key, x.Value)));
            var info = toOutput.ToStore(AzureUtils.ToAzureUri(accountName, storageKey, containerName, 
			           "wc-out.txt")).SubmitAndWait();
        }
    }
}
