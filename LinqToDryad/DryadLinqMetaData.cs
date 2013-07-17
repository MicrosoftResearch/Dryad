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
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using Microsoft.Research.DryadLinq.Internal;
using System.Xml;
using System.Text;

namespace Microsoft.Research.DryadLinq
{
    //@@TODO[P1]: read/write meta-data to the DSC stream attribute.
    //       use the query-plan to pass the meta-data to the JM which will set the attribute on each output stream.
    internal class DryadLinqMetaData
    {
        const int FLAG_ALLOW_NULL_RECORDS = 0x1;
        const int FLAG_ALLOW_NULL_FIELDS = 0x2;
        const int FLAG_ALLOW_NULL_ARRAY_ELEMENTS = 0x4;
        const int FLAG_ALLOW_AUTO_TYPE_INFERENCE = 0x8;

        private HpcLinqContext m_context;
        private string m_dscStreamName;
        private Type m_elemType;
        private DscCompressionScheme m_compressionScheme;
        //private Version m_version;
        //private int m_serializationFlags;
        //private UInt64 m_fp;
        //private DataSetInfo m_dataSetInfo;
        
        internal const string RECORD_TYPE_NAME = "__LinqToHPC__ 364C7B59-08C0-44ED-B8A2-7E224ED5B7ED";
        //internal const string COMPRESSION_SCHEME_NAME = "__LinqToHpc__8D22DD19-FA86-45DB-A0D7-C3C3A1440C90";
        //NOTE: although Compression is stored as a DSC attribute, it can only be retrieved via dscFileSet.CompressionScheme
        //      fs.GetMetaData("name") will only return the byte[] payload, for which there is none.
        //      Also, fs.CompressionScheme can only be set via dsc.CreateFileSet(.., scheme)
        private DryadLinqMetaData()
        {
        }

        internal static DryadLinqMetaData ForLocalDebug(HpcLinqContext context,
                                                        Type recordType,
                                                        string dscStreamName,
                                                        DscCompressionScheme compressionScheme)
        {
            DryadLinqMetaData metaData = new DryadLinqMetaData();
            
            metaData.m_context = context;
            metaData.m_dscStreamName = dscStreamName;
            metaData.m_elemType = recordType;
            metaData.m_compressionScheme = compressionScheme;
            //metaData.m_version = context.ClientVersion;
            //metaData.InitializeFlags();

            //metaData.m_fp = 0UL;
            //metaData.m_dataSetInfo = node.OutputDataSetInfo;

            return metaData;
        }

        // create DryadLinqMetaData from a query OutputNode
        internal static DryadLinqMetaData FromOutputNode(HpcLinqContext context, DryadOutputNode node)
        {
            DryadLinqMetaData metaData = new DryadLinqMetaData();
            
            if (! (DataPath.IsDsc(node.MetaDataUri) || DataPath.IsHdfs(node.MetaDataUri)) )
            {
                throw new InvalidOperationException();
            }

            metaData.m_context = context;
            metaData.m_dscStreamName = node.MetaDataUri;
            metaData.m_elemType = node.OutputTypes[0];
            metaData.m_compressionScheme = node.OutputCompressionScheme;
            //metaData.m_version = context.ClientVersion;
            //metaData.InitializeFlags();
            
            //metaData.m_fp = 0UL;
            //metaData.m_dataSetInfo = node.OutputDataSetInfo;

            return metaData;
        }

        // Load a DryadLinqMetaData from an existing dsc stream.
        internal static DryadLinqMetaData FromDscStream(HpcLinqContext context, string dscStreamName)
        {
            DryadLinqMetaData metaData;
            try
            {
                DscFileSet fs = context.DscService.GetFileSet(dscStreamName);
                metaData = new DryadLinqMetaData();
                metaData.m_context = context;
                metaData.m_dscStreamName = dscStreamName;
                //metaData.m_fp = 0L;
                //metaData.m_dataSetInfo = null;

                byte[] metaDataBytes; 
                
                //record-type
                metaDataBytes = fs.GetMetadata(DryadLinqMetaData.RECORD_TYPE_NAME);
                if (metaDataBytes != null)
                {
                    string recordTypeString = Encoding.UTF8.GetString(metaDataBytes);
                    metaData.m_elemType = Type.GetType(recordTypeString);
                }

                //Compression-scheme
                metaData.m_compressionScheme = fs.CompressionScheme;
            }
            catch (Exception e)
            {
                throw new DryadLinqException(HpcLinqErrorCode.ErrorReadingMetadata,
                                           String.Format(SR.ErrorReadingMetadata), e);
            }

            return metaData;
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

        internal string MetaDataUri
        {
            get { return this.m_dscStreamName; }
        }

        internal Type ElemType
        {
            get { return this.m_elemType; }
        }

        //internal Version Version
        //{
        //    get { return this.m_version; }
        //}

        internal DscCompressionScheme CompressionScheme
        {
            get { return this.m_compressionScheme; }
        }

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
