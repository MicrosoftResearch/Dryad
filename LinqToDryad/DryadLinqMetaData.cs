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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using Microsoft.Research.DryadLinq.Internal;
using System.Xml;
using System.Text;

namespace Microsoft.Research.DryadLinq
{
    public class DryadLinqMetaData
    {
        const int FLAG_ALLOW_NULL_RECORDS = 0x1;
        const int FLAG_ALLOW_NULL_FIELDS = 0x2;
        const int FLAG_ALLOW_NULL_ARRAY_ELEMENTS = 0x4;
        const int FLAG_ALLOW_AUTO_TYPE_INFERENCE = 0x8;

        private DryadLinqContext m_context;
        private Uri m_dataSetUri;
        private Type m_elemType;
        private CompressionScheme m_compressionScheme;
        //private Version m_version;
        //private int m_serializationFlags;
        //private UInt64 m_fp;
        //private DataSetInfo m_dataSetInfo;
        
        internal const string RECORD_TYPE_NAME = "__DLINQ__364C7B59-08C0-44ED-B8A2-7E224ED5B7ED";
        //NOTE: While Compression is a DSC attribute, it can only be retrieved via dscFileSet.CompressionScheme
        //      fs.GetMetaData("name") will only return the byte[] payload.
        //      Also, fs.CompressionScheme can only be set via dsc.CreateFileSet(.., scheme)

        private DryadLinqMetaData()
        {
        }

        internal DryadLinqMetaData(DryadLinqContext context,
                                   Type recordType,
                                   Uri dataSetUri,
                                   CompressionScheme compressionScheme)
        {
            this.m_context = context;
            this.m_dataSetUri = dataSetUri;
            this.m_elemType = recordType;
            this.m_compressionScheme = compressionScheme;
            //this.m_version = context.ClientVersion();
            //this.InitializeFlags();

            //this.m_fp = 0UL;
            //this.m_dataSetInfo = node.OutputDataSetInfo;
        }

        // create DryadLinqMetaData from a query OutputNode
        internal static DryadLinqMetaData Get(DryadLinqContext context, DLinqOutputNode node)
        {
            DryadLinqMetaData metaData = new DryadLinqMetaData();

            if (!DataPath.IsValidDataPath(node.OutputUri))
            {
                throw new InvalidOperationException();
            }

            metaData.m_context = context;
            metaData.m_dataSetUri = node.OutputUri;
            metaData.m_elemType = node.OutputTypes[0];
            metaData.m_compressionScheme = node.OutputCompressionScheme;
            //metaData.m_version = context.ClientVersion();
            //metaData.InitializeFlags();
            
            //metaData.m_fp = 0UL;
            //metaData.m_dataSetInfo = node.OutputDataSetInfo;

            return metaData;
        }

        internal static DryadLinqMetaData Get(DryadLinqContext context, Uri dataSetUri)
        {
            string scheme = DataPath.GetScheme(dataSetUri);
            DataProvider dataProvider = DataProvider.GetDataProvider(scheme);
            return dataProvider.GetMetaData(context, dataSetUri);
        }

        //private void InitializeFlags()
        //{
        //    this.m_serializationFlags = 0;

        //    if (StaticConfig.AllowNullRecords)
        //    {
        //        this.m_serializationFlags |= FLAG_ALLOW_NULL_RECORDS;
        //    }
        //    if (StaticConfig.AllowNullFields)
        //    {
        //        this.m_serializationFlags |= FLAG_ALLOW_NULL_FIELDS;
        //    }
        //    if (StaticConfig.AllowNullArrayElements)
        //    {
        //        this.m_serializationFlags |= FLAG_ALLOW_NULL_ARRAY_ELEMENTS;
        //    }
        //    if (StaticConfig.AllowAutoTypeInference)
        //    {
        //        this.m_serializationFlags |= FLAG_ALLOW_AUTO_TYPE_INFERENCE;
        //    }
        //}

        internal Uri DataSetUri
        {
            get { return this.m_dataSetUri; }
        }

        internal Type ElemType
        {
            get { return this.m_elemType; }
        }

        internal CompressionScheme CompressionScheme
        {
            get { return this.m_compressionScheme; }
        }

        //internal Version Version
        //{
        //    get { return this.m_version; }
        //}

        //internal bool AllowNullRecords
        //{
        //    get { return false; } //(this.m_serializationFlags & FLAG_ALLOW_NULL_RECORDS) != 0; 
        //}

        //internal bool AllowNullFields
        //{
        //    get { return false; } // (this.m_serializationFlags & FLAG_ALLOW_NULL_FIELDS) != 0; 
        //}

        //internal bool AllowNullArrayElements
        //{
        //    get { return false; } // (this.m_serializationFlags & FLAG_ALLOW_NULL_ARRAY_ELEMENTS) != 0;
        //}

        //internal bool AllowAutoTypeInference
        //{
        //    get { return false; }// (this.m_serializationFlags & FLAG_ALLOW_AUTO_TYPE_INFERENCE) != 0;
        //}

        //internal DataSetInfo DataSetInfo
        //{
        //    get { return this.m_dataSetInfo; }
        //}
    }
}
