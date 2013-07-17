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

//------------------------------------------------------------------------------
// <summary>
//      Provide access to native APIs in Netapi32.dll
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Hpc.Dsc.Internal
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Runtime.InteropServices;
    using System.Text;


    /// <summary>
    /// Return codes from the Netapi32.dll pinvoke calls
    /// </summary>
    internal enum NetShareError
    {
        /// <summary>
        /// Success
        /// </summary>
        NERR_Success = 0,

        /// <summary>
        /// The user does not have access to the requested information.
        /// </summary>
        ERROR_ACCESS_DENIED = 5,

        /// <summary>
        /// Not enough storage is available to process this command
        /// </summary>
        ERROR_NOT_ENOUGH_MEMORY = 8,

        /// <summary>
        /// The network path was not found
        /// </summary>
        ERROR_BAD_NETPATH = 53,

        /// <summary>
        /// The specified parameter is not valid.
        /// </summary>
        ERROR_INVALID_PARAMETER = 87,

        /// <summary>
        /// The value specified for the level parameter is not valid.
        /// </summary>
        ERROR_INVALID_LEVEL = 124,

        /// <summary>
        /// The provided buffer was too small to hold the entire value
        /// </summary>
        ERROR_MORE_DATA = 234,

        /// <summary>
        /// The filename, directory name, or volume label syntax is incorrect
        /// </summary>
        ERROR_INVALID_NAME = 123,

        /// <summary>
        /// The device or directory does not exist
        /// </summary>
        NERR_UnknownDevDir = 2116,

        /// <summary>
        /// The share name is already in use on this server
        /// </summary>
        NERR_DuplicateShare = 2118,

        /// <summary>
        /// The client request succeeded. More entries are available. The buffer size that is specified by PreferedMaximumLength was too small to fit even a single entry
        /// </summary>
        NERR_BufTooSmall = 2123,

        /// <summary>
        /// The share name does not exist
        /// </summary>
        NERR_NetNameNotFound = 2310,

        /// <summary>
        /// The operation is not valid for a redirected resource. The specified device name is assigned to a shared resource
        /// </summary>
        NERR_RedirectedPath = 2117
    }

    //
    // Provide access to native APIs in Netapi32.dll
    //
    internal static class NetShareWrapper
    {
        /// <summary>
        /// Specifies information about the shared resource, including the name of the resource, 
        /// type and permissions, and number of connections. 
        /// The buf parameter points to a SHARE_INFO_2 structure.
        /// </summary>
        private static UInt32 _InfoLevel = 2;

        /// <summary>
        /// Disk drive
        /// </summary>
        private static UInt32 _STYPE_DISKTREE = 0;

        /// <summary>
        /// Permission to read a resource and, by default, execute the resource.
        /// </summary>
        private static UInt32 _PERM_FILE_READ = 1;

        /// <summary>
        /// Permission to write to a resource.
        /// </summary>
        private static UInt32 _PERM_FILE_WRITE = 2;

        /// <summary>
        /// Permission to create a resource; data can be written when creating the resource.
        /// </summary>
        private static UInt32 _PERM_FILE_CREATE = 4;

        /// <summary>
        /// Contains information about the shared resource, including name of the resource, type and permissions, and the number of current connections
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        private struct _SHARE_INFO_2
        {
            /// <summary>
            /// Pointer to a Unicode string specifying the share name of a resource
            /// </summary>
            [MarshalAs(UnmanagedType.LPWStr)]
            public string shi2_netname;

            /// <summary>
            /// A bitmask of flags that specify the type of the shared resource
            /// </summary>
            [MarshalAs(UnmanagedType.U4)]
            public UInt32 shi2_type;

            /// <summary>
            /// Pointer to a Unicode string that contains an optional comment about the shared resource
            /// </summary>
            [MarshalAs(UnmanagedType.LPWStr)]
            public string shi2_remark;

            /// <summary>
            /// Specifies a DWORD value that indicates the shared resource's permissions for servers running with share-level security
            /// </summary>
            [MarshalAs(UnmanagedType.U4)]
            public UInt32 shi2_permissions;

            /// <summary>
            /// Specifies a DWORD value that indicates the maximum number of concurrent connections that the shared resource can accommodate.
            /// The number of connections is unlimited if the value specified in this member is �1.
            /// </summary>
            [MarshalAs(UnmanagedType.U4)]
            public UInt32 shi2_max_uses;

            /// <summary>
            /// Specifies a DWORD value that indicates the number of current connections to the resource
            /// </summary>
            [MarshalAs(UnmanagedType.U4)]
            public UInt32 shi2_current_uses;

            /// <summary>
            /// Pointer to a Unicode string specifying the local path for the shared resource
            /// </summary>
            [MarshalAs(UnmanagedType.LPWStr)]
            public string shi2_path;

            /// <summary>
            /// Pointer to a Unicode string that specifies the share's password when the server is running with share-level security
            /// </summary>
            [MarshalAs(UnmanagedType.LPWStr)]
            public string shi2_passwd;
        }

        /// <summary>
        /// Shares a server resource.
        /// </summary>
        /// <param name="ServerName">Pointer to a string that specifies the DNS or NetBIOS name of the remote server on which the function is to execute. If this parameter is NULL, the local computer is used.</param>
        /// <param name="InfoLevel">Specifies the information level of the data</param>
        /// <param name="ShareInfo">Pointer to the buffer that specifies the data</param>
        /// <param name="OutputBuffer">Pointer to a value that receives the index of the first member of the share information structure that causes the ERROR_INVALID_PARAMETER error. If this parameter is NULL, the index is not returned on error</param>
        /// <returns></returns>
        [DllImport("Netapi32.dll")]
        private static extern Int32 NetShareAdd(
            [MarshalAs(UnmanagedType.LPWStr)] string ServerName,
            [MarshalAs(UnmanagedType.U4)] UInt32 InfoLevel,
            [MarshalAs(UnmanagedType.Struct)] ref _SHARE_INFO_2 ShareInfo,
            ref IntPtr OutputBuffer);

        /// <summary>
        /// Deletes a share name from a server's list of shared resources, disconnecting all connections to the shared resource.
        /// </summary>
        /// <param name="ServerName">Pointer to a string that specifies the DNS or NetBIOS name of the remote server on which the function is to execute. If this parameter is NULL, the local computer is used</param>
        /// <param name="ShareName">Pointer to a string that specifies the name of the share to delete.</param>
        /// <param name="ParameterReserved">Reserved, must be zero.</param>
        /// <returns>If the function succeeds, the return value is NERR_Success</returns>
        [DllImport("Netapi32.dll")]
        private static extern Int32 NetShareDel(
            [MarshalAs(UnmanagedType.LPWStr)] string ServerName,
            [MarshalAs(UnmanagedType.LPWStr)] string ShareName,
            UInt32 ParameterReserved);

        /// <summary>
        /// Retrieves information about a particular shared resource on a server.
        /// </summary>
        /// <param name="ServerName">Pointer to a string that specifies the DNS or NetBIOS name of the remote server on which the function is to execute. If this parameter is NULL, the local computer is used</param>
        /// <param name="ShareName">Pointer to a string that specifies the name of the share for which to return information</param>
        /// <param name="InfoLevel">Specifies the information level of the data</param>
        /// <param name="OutputBuffer">Pointer to the buffer that receives the data</param>
        /// <returns>If the function succeeds, the return value is NERR_Success.</returns>
        [DllImport("Netapi32.dll")]
        private static extern Int32 NetShareGetInfo(
            [MarshalAs(UnmanagedType.LPWStr)] string ServerName,
            [MarshalAs(UnmanagedType.LPWStr)] string ShareName,
            [MarshalAs(UnmanagedType.U4)] UInt32 InfoLevel,
            ref IntPtr OutputBuffer);

        /// <summary>
        /// The NetApiBufferFree function frees the memory that the NetApiBufferAllocate function allocates
        /// </summary>
        /// <param name="InputBuffer">A pointer to a buffer returned previously by another network management function or memory allocated by calling the NetApiBufferAllocate function</param>
        /// <returns></returns>
        [DllImport("Netapi32", CharSet = CharSet.Auto)]
        private static extern Int32 NetApiBufferFree(IntPtr InputBuffer);

        /// <summary>
        /// Prints out the error based on the error code
        /// </summary>
        /// <param name="ErrorCode">Error code returned by pinvoke call</param>
        private static void PrintError(Int32 ErrorCode)
        {
            switch ((NetShareError)ErrorCode)
            {
                case NetShareError.ERROR_ACCESS_DENIED:
                    Console.Error.WriteLine("Access to share denied.");
                    break;

                case NetShareError.ERROR_NOT_ENOUGH_MEMORY:
                    Console.Error.WriteLine("Not enough memory available.");
                    break;

                case NetShareError.ERROR_INVALID_PARAMETER:
                    Console.Error.WriteLine("Invalid parameter specified.");
                    break;

                case NetShareError.ERROR_INVALID_LEVEL:
                    Console.Error.WriteLine("Invalid level specified.");
                    break;

                case NetShareError.ERROR_MORE_DATA:
                    Console.Error.WriteLine("More data available and not large enough buffer specified.");
                    break;

                case NetShareError.ERROR_INVALID_NAME:
                    Console.Error.WriteLine("The filename, directory name, or volume label syntax is incorrect");
                    break;

                case NetShareError.NERR_UnknownDevDir:
                    Console.Error.WriteLine("Unknown device specified.");
                    break;

                case NetShareError.NERR_DuplicateShare:
                    Console.Error.WriteLine("Duplicate share specified.");
                    break;

                case NetShareError.NERR_BufTooSmall:
                    Console.Error.WriteLine("Not large enough buffer specified.");
                    break;

                case NetShareError.NERR_NetNameNotFound:
                    Console.Error.WriteLine("Share name not found.");
                    break;

                case NetShareError.NERR_RedirectedPath:
                    Console.Error.WriteLine("The operation is not valid for a redirected resource. The specified device name is assigned to a shared resource.");
                    break;

                case NetShareError.ERROR_BAD_NETPATH:
                    Console.Error.WriteLine("The network path was not found.");
                    break;

                default:
                    Console.Error.WriteLine(String.Format(CultureInfo.CurrentCulture, "Unknown error occured (Error Code {0}).", ErrorCode));
                    break;
            }
        }

        /// <summary>
        /// Create a net share
        /// </summary>
        /// <param name="ServerName">Server hosting share</param>
        /// <param name="ShareName">name of share</param>
        /// <param name="SharePath">path to share</param>
        /// <param name="ShareDescription">description of share</param>
        /// <returns>success/failure</returns>
        internal static int CreateShare(string ServerName, string ShareName, string SharePath, string ShareDescription)
        {
            //
            // Ensure UNC path formatting at start
            //
            if (!ServerName.StartsWith(@"\\"))
            {
                if (!ServerName.StartsWith(@"\"))
                {
                    ServerName = @"\\" + ServerName;
                }
                else
                {
                    ServerName = @"\" + ServerName;
                }
            }

            //
            // Build share information
            //
            _SHARE_INFO_2 ShareInfo = new _SHARE_INFO_2();
            ShareInfo.shi2_netname = ShareName;
            ShareInfo.shi2_type = _STYPE_DISKTREE;
            ShareInfo.shi2_remark = ShareDescription;
            ShareInfo.shi2_permissions = _PERM_FILE_READ | _PERM_FILE_WRITE | _PERM_FILE_CREATE;
            ShareInfo.shi2_max_uses = UInt32.MaxValue;
            ShareInfo.shi2_current_uses = 0;
            ShareInfo.shi2_path = SharePath;
            ShareInfo.shi2_passwd = String.Empty;

            IntPtr OutputBuffer = IntPtr.Zero;

            //
            // Create the share and report success or failure
            //
            Int32 ErrorCode = NetShareAdd(ServerName, _InfoLevel, ref ShareInfo, ref OutputBuffer);


            if (ErrorCode != (Int32)NetShareError.NERR_Success)
            {
                PrintError(ErrorCode);
            }

            return ErrorCode;
        }

        /// <summary>
        /// Deletes an existing share
        /// </summary>
        /// <param name="ServerName">Server hosting share</param>
        /// <param name="ShareName">name of share</param>
        /// <returns>success/failure</returns>
        internal static bool DeleteShare(string ServerName, string ShareName)
        {
            //
            // Ensure UNC path formatting at start
            //
            if (!ServerName.StartsWith(@"\\"))
            {
                if (!ServerName.StartsWith(@"\"))
                {
                    ServerName = @"\\" + ServerName;
                }
                else
                {
                    ServerName = @"\" + ServerName;
                }
            }

            //
            // Attempt to delete the share and report success/failure
            //
            Int32 ErrorCode = NetShareDel(ServerName, ShareName, 0);
            if (ErrorCode == (Int32)NetShareError.NERR_Success)
            {
                return (true);
            }
            else
            {
                PrintError(ErrorCode);
            }

            return (false);
        }

        /// <summary>
        /// Get the drive where a share is hosted
        /// </summary>
        /// <param name="ServerName">Server hosting share</param>
        /// <param name="ShareName">Name of share</param>
        /// <returns>drive letter (empty string if unsuccessful)</returns>
        internal static string GetLocalDrive(string ServerName, string ShareName)
        {
            //
            // Ensure UNC path formatting at start
            //
            if (!ServerName.StartsWith(@"\\"))
            {
                if (!ServerName.StartsWith(@"\"))
                {
                    ServerName = @"\\" + ServerName;
                }
                else
                {
                    ServerName = @"\" + ServerName;
                }
            }

            string ShareDrive = String.Empty;
            string SharePath = String.Empty;
            IntPtr OutputBuffer = IntPtr.Zero;

            //
            // Attempt to get the share information. Report error if unsuccessful
            //
            Int32 ErrorCode = NetShareGetInfo(ServerName, ShareName, _InfoLevel, ref OutputBuffer);
            if (ErrorCode == (Int32)NetShareError.NERR_Success)
            {
                _SHARE_INFO_2 ShareInfo = (_SHARE_INFO_2)Marshal.PtrToStructure(OutputBuffer, typeof(_SHARE_INFO_2));
                SharePath = ShareInfo.shi2_path;
                NetApiBufferFree(OutputBuffer);
            }
            else
            {
                PrintError(ErrorCode);
                return (ShareDrive);
            }

            if (!String.IsNullOrEmpty(SharePath))
            {
                //
                // If a share path was returned, attempt to parse out the drive letter
                //
                int Index = SharePath.IndexOf(':');
                if (Index > 0)
                {
                    ShareDrive = SharePath.Substring(0, Index + 1);
                }
            }

            return (ShareDrive);
        }

        // TODO: Sync Dryad GM and vertex with new version that returns error code
        internal static string GetLocalPath(string ServerName, string ShareName)
        {
            string SharePath;

            int err = GetLocalPath(ServerName, ShareName, out SharePath);
            if (err != (int)NetShareError.NERR_Success)
            {
                Console.Error.WriteLine("GetLocalPath failed: server {0}, share {1}", ServerName, ShareName);
                PrintError(err);
            }

            return SharePath;
        }

        /// <summary>
        /// Returns the local path to a share on the specified server
        /// </summary>
        /// <param name="ServerName">Server the path resides on</param>
        /// <param name="ShareName">Share to find path to</param>
        /// <returns>Local path to share or empty string if failure</returns>
        internal static int GetLocalPath(string ServerName, string ShareName, out string SharePath)
        {
            //
            // Ensure UNC path formatting at start
            //
            if (!String.IsNullOrEmpty(ServerName) && !ServerName.StartsWith(@"\\"))
            {
                if (!ServerName.StartsWith(@"\"))
                {
                    ServerName = @"\\" + ServerName;
                }
                else
                {
                    ServerName = @"\" + ServerName;
                }
            }

            IntPtr OutputBuffer = IntPtr.Zero;
            SharePath = String.Empty;

            //
            // Get share info structure
            //
            Int32 ErrorCode = NetShareGetInfo(ServerName, ShareName, _InfoLevel, ref OutputBuffer);

            if (ErrorCode == (Int32)NetShareError.NERR_Success)
            {
                //
                // If successful, get the local path to the resource and free the buffer
                //
                _SHARE_INFO_2 ShareInfo = (_SHARE_INFO_2)Marshal.PtrToStructure(OutputBuffer, typeof(_SHARE_INFO_2));
                SharePath = ShareInfo.shi2_path;
                NetApiBufferFree(OutputBuffer);
            }

            return ErrorCode;
        }
    }
}