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
using System.Text;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;
using Microsoft.Research.DryadLinq;

namespace Microsoft.Research.DryadLinq.Internal
{
    // This class implements an object store that is used to store objects
    // needed for remote execution of managed vertex code. All objects put
    // in the store must have the .NET Serializable attribute.
    // Note: this class is not thread safe
    public sealed class HpcLinqObjectStore
    {
        private const string ObjectStoreFileName = "HpcLinqObjectStore.bin";

        internal static string GetClientSideObjectStorePath()
        {
            return HpcLinqCodeGen.GetPathForGeneratedFile(ObjectStoreFileName, null);            
        }

        private static ArrayList s_objectList = null;

        public static bool IsEmpty
        {
            get {
                return (s_objectList == null || s_objectList.Count == 0); 
            }
        }

        public static void Clear()
        {
            s_objectList = null;
        }
        
        // this method is only used by the generated vertex code, and always
        // assumes "HpcLinqObjectStore.bin" to be in the current directory
        public static object Get(int idx)
        {
            if (s_objectList == null)
            {
                // Try to open the object store. First look in the parent directory
                // (job directory when running normally), then try opening it from the path.
                FileStream fs;
                try
                {
                    fs = new FileStream(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).FullName, ObjectStoreFileName), FileMode.Open, FileAccess.Read, FileShare.Read);
                }
                catch (FileNotFoundException)
                {
                    fs = new FileStream(ObjectStoreFileName, FileMode.Open, FileAccess.Read, FileShare.Read);
                }

                BinaryFormatter bfm = new BinaryFormatter();            
                try
                {
                    s_objectList = (ArrayList)bfm.Deserialize(fs);
                }
                catch (SerializationException e)
                {
                    throw new DryadLinqException(HpcLinqErrorCode.FailedToDeserialize,
                                               SR.FailedToDeserialize, e);
                }
                finally
                {
                    if (fs != null) fs.Close();
                }
            }

            if (idx >= s_objectList.Count)
            {
                throw new DryadLinqException(HpcLinqErrorCode.IndexOutOfRange,
                                           SR.IndexOutOfRange);
            }
            return s_objectList[idx];
        }

        public static int Put(object obj)
        {
            if (s_objectList != null)
            {
                for (int idx = 0; idx < s_objectList.Count; idx++)
                {
                    if (Object.ReferenceEquals(obj, s_objectList[idx]))
                    {
                        return idx;
                    }
                }
            }

            if (s_objectList == null)
            {
                s_objectList = new ArrayList(4);
            }
            s_objectList.Add(obj);
            return (s_objectList.Count - 1);
        }

        // This method is only used by the client process to save the object store before submitting a job
        // Like other generated files we need to save this file in the temp directory
        public static void Save()
        {
            if (IsEmpty) return;
            string objectStorePath = GetClientSideObjectStorePath();

            FileStream fs = new FileStream(objectStorePath, FileMode.Create);
            BinaryFormatter bfm = new BinaryFormatter();
            try
            {
                bfm.Serialize(fs, s_objectList);
            }
            catch (SerializationException e)
            {
                foreach (object obj in s_objectList)
                {
                    Type badType = TypeSystem.GetNonserializable(obj);
                    if (badType != null)
                    {
                        if (badType.IsGenericType &&
                            badType.GetGenericTypeDefinition() == typeof(DryadLinqQuery<>))
                        {
                            throw new DryadLinqException(HpcLinqErrorCode.CannotSerializeHpcLinqQuery,
                                                       SR.CannotSerializeHpcLinqQuery);
                        }
                        else
                        {
                            throw new DryadLinqException(HpcLinqErrorCode.CannotSerializeObject,
                                                       string.Format(SR.CannotSerializeObject, obj));
                        }
                    }
                }

                throw new DryadLinqException(HpcLinqErrorCode.GeneralSerializeFailure,
                                           SR.GeneralSerializeFailure, e);
            }
            finally
            {
                if (fs != null) fs.Close();
            }
        }
    }

}
