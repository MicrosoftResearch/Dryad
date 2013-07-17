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

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.Research.Dryad;

namespace linqtodryadjm_managed
{
    public class DryadLINQApp
    {
        public class OptionDescription
        {
            public enum OptionCategory
            {
                OC_General = 0,
                OC_Inputs,
                OC_Outputs,
                OC_VertexBehavior,
                OC_LAST
            };
            public OptionCategory m_optionCategory;
            
            public int m_optionIndex;  
            public string m_shortName;
            public string m_longName;
            public string m_arguments;
            public string m_descriptionText;
            
            public OptionDescription()
            {
            }

            public OptionDescription(OptionCategory optionCatagory, int index, string shortName, string longName, string args, string desc)
            {
                m_optionCategory = optionCatagory;
                m_optionIndex = index;
                m_shortName = shortName;
                m_longName = longName;
                m_arguments = args;
                m_descriptionText = desc;
            }            
        };

        enum DryadLINQAppOptions 
        {
            BDAO_EmbeddedResource,
            BDAO_ReferenceResource,
            BDJAO_AMD64,
            BDJAO_I386,
            BDJAO_Retail,
            BDJAO_Debug,
            DNAO_MaxAggregateInputs,
            DNAO_MaxAggregateFilterInputs,
            DNAO_AggregateThreshold,
            DNAO_NoClusterAffinity,
            DNAO_LAST
        };

        static OptionDescription []s_dryadLinqOptionArray =  new OptionDescription[] 
        {
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_VertexBehavior,
                     (int)DryadLINQAppOptions.BDJAO_AMD64,
                     "amd64",
                     "useamd64binary",
                     "",
                     "Dummy argument for legacy reasons"
                    ),            
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_VertexBehavior,
                     (int)DryadLINQAppOptions.BDJAO_I386,
                     "i386",
                     "usei386binary",
                     "",
                     "Dummy argument for legacy reasons"
                    ),
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_VertexBehavior,
                     (int)DryadLINQAppOptions.BDJAO_Retail,
                     "retail",
                     "useretailbinary",
                     "",
                     "Dummy argument for legacy reasons"
                    ),
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_VertexBehavior,
                     (int)DryadLINQAppOptions.BDJAO_Debug,
                     "debug",
                     "usedebugbinary",
                     "",
                     "Dummy argument for legacy reasons"
                    ),
            
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_General,
                     (int)DryadLINQAppOptions.DNAO_MaxAggregateInputs,
                     "mai",
                     "maxaggregateinputs",
                     "<maxInputs>",
                     "Only allow aggregate vertices to use up to maxInputs inputs."
                    ),
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_General,
                     (int)DryadLINQAppOptions.DNAO_MaxAggregateFilterInputs,
                     "mafi",
                     "maxaggregatefilterinputs",
                     "<maxInputs>",
                     "Only allow aggregate filter vertices to use up to maxInputs inputs."
                    ),
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_General,
                     (int)DryadLINQAppOptions.DNAO_AggregateThreshold,
                     "at",
                     "aggregatethreshold",
                     "<dataSize>",
                     "Only allow aggregate and aggregate filter vertices to use inputs up to a total of size dataSize. dataSize can be specified as a number of bytes or with the (case-insensitive) suffix KB, MB, GB, TB, PB, i.e. 12KB. If a suffix is present then fractions are allowed, e.g. 20.5MB."
                    ),
            new OptionDescription
                    (
                     OptionDescription.OptionCategory.OC_General,
                     (int)DryadLINQAppOptions.DNAO_NoClusterAffinity,
                     "nca",
                     "noclusteraffinity",
                     "",
                     "By default, LINQToHPC does not process DSC filesets from a different cluster. Specifying this flag overrides that"
                    )                    
        };

        SortedDictionary<string, OptionDescription> m_optionMap;
        private DrGraph m_graph;
        private bool m_clusterAffinity;
        private int m_maxAggregateInputs;
        private int m_maxAggregateFilterInputs;
        private UInt64 m_aggregateThreshold;
        private UInt64 m_startTime;
        private FileStream m_identityMapFile;

        public DryadLINQApp(DrGraph graph)
        {
            m_graph = graph;
            m_clusterAffinity = true;
            m_maxAggregateInputs = 150;
            m_maxAggregateFilterInputs = 32;
            m_aggregateThreshold = 1024*1024*1024;  // 1GB
            m_startTime = graph.GetXCompute().GetCurrentTimeStamp();            
            m_identityMapFile = null;    
            m_optionMap = new SortedDictionary<string,OptionDescription>();
            AddOptionsToMap(s_dryadLinqOptionArray);
        }
        
        public void AddOptionsToMap(OptionDescription[] optionArray)
        {
            foreach (OptionDescription option in optionArray)
            {
                m_optionMap.Add(option.m_shortName, option);
                m_optionMap.Add(option.m_longName, option);
            }            
        }
        
        public void PrintOptionUsage(OptionDescription option)
        {
            Console.WriteLine("    {-{0}|-{1}} {2}", option.m_shortName, option.m_longName, option.m_arguments);
            Console.WriteLine("    {0}", option.m_descriptionText);
        }

        static string[] s_optionCategoryName = new string[] 
        {
            "General options",
            "Options to specify job inputs",
            "Options to specify job outputs",
            "Options to control vertex behavior"
        };

        public void PrintCategoryUsage(OptionDescription.OptionCategory category)
        {
            Console.WriteLine(s_optionCategoryName[(int)category]);
            foreach (KeyValuePair<string, OptionDescription> kvp in m_optionMap)
            {
                OptionDescription option = kvp.Value;
                if (option.m_optionCategory == category && kvp.Key == option.m_shortName)
                {
                    PrintOptionUsage(option);
                }
            }
        }

        public void PrintUsage(string exeName)
        {
            string leafName = Path.GetFileName(exeName);
            Console.WriteLine("usage: {0} [--debugbreak] [--popup] <options> <appSpecificArguments>\n<options> fall into the following categories:", leafName);
            for (OptionDescription.OptionCategory i=0; i<OptionDescription.OptionCategory.OC_LAST; ++i)
            {
                PrintCategoryUsage(i);
            }
        }
        
        public bool ParseCommandLineFlags(string[] args)
        {
            bool retVal = true;

            for (int index=0; index < args.Length; index++)
            {
                string arg = args[index].Substring(args[index].IndexOf("-")+1);
                OptionDescription option = null;
                m_optionMap.TryGetValue(arg, out option);
                if (option == null)
                {
                    retVal = false;
                    break;
                }
                int optionIndex = option.m_optionIndex;

                switch (optionIndex)
                {
                    case (int)DryadLINQAppOptions.BDJAO_AMD64:
                    case (int)DryadLINQAppOptions.BDJAO_I386:
                    case (int)DryadLINQAppOptions.BDJAO_Retail:
                    case (int)DryadLINQAppOptions.BDJAO_Debug:
                        break;

                    case (int)DryadLINQAppOptions.DNAO_MaxAggregateInputs:
                        if ((index + 1) >= args.Length)
                        {
                            DryadLogger.LogCritical(0, null, "The argument for option '{0}' was missing.\n", args[index]);
                            retVal = false;
                        }
                        else
                        {
                            int maxInputs;
                            if (!Int32.TryParse(args[index+1], out maxInputs))
                            {
                                DryadLogger.LogCritical(0, null, "The argument '{0}' for option '{1}' could not be parsed as an integer.\n", args[index + 1], args[index]);
                                retVal = false;
                            }
                            else
                            {
                                m_maxAggregateInputs = maxInputs;
                                index++;
                            }
                        }
                        break;

                    case (int)DryadLINQAppOptions.DNAO_MaxAggregateFilterInputs:
                        if ((index + 1) >= args.Length)
                        {
                            DryadLogger.LogCritical(0, null, "The argument for option '{0}' was missing.\n", args[index]);
                            retVal = false;
                        }
                        else
                        {
                            int maxInputs;
                            if (!Int32.TryParse(args[index+1], out maxInputs))
                            {
                                DryadLogger.LogCritical(0, null, "The argument '{0}' for option '{1}' could not be parsed as an integer.\n", args[index + 1], args[index]);
                                retVal = false;
                            }
                            else
                            {
                                m_maxAggregateFilterInputs = maxInputs;
                                index++;
                            }
                        }
                        break;


                    case (int)DryadLINQAppOptions.DNAO_AggregateThreshold:
                        if ((index + 1) >= args.Length)
                        {
                            DryadLogger.LogCritical(0, null, "The argument for option '{0}' was missing.\n", args[index]);
                            retVal = false;
                        }
                        else
                        {
                            UInt64 threshold;
                            if (!UInt64.TryParse(args[index+1], out threshold))
                            {
                                DryadLogger.LogCritical(0, null, "The argument '{0}' for option '{1}' could not be parsed as a UIN64.\n", args[index + 1], args[index]);
                                retVal = false;
                            }
                            else
                            {
                                m_aggregateThreshold = threshold;
                                index++;
                            }
                        }
                        break;

                    case (int)DryadLINQAppOptions.DNAO_NoClusterAffinity:
                        m_clusterAffinity = false;
                        break;

                    default:
                        DryadLogger.LogCritical(0, null, "Unknown command-line option {0}\n", optionIndex);
                        retVal = false;
                        break;
                }

            }

            if (!retVal)
            {
                PrintUsage(args[0]);
            }
            return retVal;

        }

        internal bool ExtractReferenceResourceNameAndUri(string resourceSpec, ref string resourceUri, ref string resourceName)
        {
            // reference resourceSpec is of type name@uri
            string name = resourceSpec.Substring(0, resourceSpec.IndexOf('@'));
            string uri = resourceSpec.Substring(resourceSpec.IndexOf('@'));

            // resourceSpecCopy now is name\0uri with resourceName pointing to name and resourceUri pointing to uri
            resourceUri = uri;
            resourceName = name;

            return true;
        }
        
        public DrGraph GetGraph()
        {
            return m_graph;
        }

        public DrUniverse GetUniverse()
        {
            return m_graph.GetXCompute().GetUniverse();
        }

        public void SetClusterAffinity(bool flag)
        {
            m_clusterAffinity = flag;
        }

        public bool GetClusterAffinity()
        {
            return m_clusterAffinity;
        }

        public int GetMaxAggregateInputs()
        {
            return m_maxAggregateInputs;
        }

        public UInt64 GetAggregateThreshold()
        {
            return m_aggregateThreshold;
        }

        public int GetMaxAggregateFilterInputs()
        {
            return m_maxAggregateFilterInputs;
        }

        public UInt64 GetStartTime()
        {
            return m_startTime;
        }

        public void SetXmlFileName(string xml)
        {
            if (xml.Length >= 3)
            {
                string dup = xml.Substring(0, xml.Length-3);
                dup += "map";
                m_identityMapFile = File.Open(dup, FileMode.Create);
            }
        }

        public FileStream GetIdentityMapFile()
        {
            return m_identityMapFile;
        }
        
    }

} // namespace DryadLINQ
 