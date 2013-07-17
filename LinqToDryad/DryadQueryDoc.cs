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
using System.Collections.Generic;
using System.Text;
using System.Xml;
using Microsoft.Research.DryadLinq.Internal;

namespace Microsoft.Research.DryadLinq
{
    internal class DryadQueryDoc
    {
        // Create the XML element specifying the vertex code.
        internal static XmlElement CreateVertexEntryElem(XmlDocument queryDoc, string dllFileName, string vertexMethod)
        {
            XmlElement entryElem = queryDoc.CreateElement("Entry");

            XmlElement elem = queryDoc.CreateElement("AssemblyName");
            elem.InnerText = dllFileName;
            entryElem.AppendChild(elem);

            elem = queryDoc.CreateElement("ClassName");
            elem.InnerText = HpcLinqCodeGen.VertexClassFullName;
            entryElem.AppendChild(elem);

            elem = queryDoc.CreateElement("MethodName");
            elem.InnerText = vertexMethod;
            entryElem.AppendChild(elem);

            return entryElem;
        }
    }
}
