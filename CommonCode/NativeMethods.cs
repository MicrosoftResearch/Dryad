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
//      Wrapped native methods
// </summary>
//------------------------------------------------------------------------------
namespace Microsoft.Research.Dryad
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Runtime.InteropServices;
    using System.Runtime.ConstrainedExecution;
    using System.Security;
    using System.Security.Permissions;
    using Microsoft.Win32.SafeHandles;

    [SecurityPermission(SecurityAction.InheritanceDemand, UnmanagedCode = true)]
    [SecurityPermission(SecurityAction.Demand, UnmanagedCode = true)]
    public sealed class SafeThreadHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private SafeThreadHandle() : base(true) { }

        public SafeThreadHandle(IntPtr handle)
            : base(false)
        {
            this.SetHandle(handle);
        }

        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.MayFail)]
        protected override bool ReleaseHandle()
        {
            return NativeMethods.CloseHandle(this.handle);
        }
    }


    [SecurityPermission(SecurityAction.InheritanceDemand, UnmanagedCode = true)]
    [SecurityPermission(SecurityAction.Demand, UnmanagedCode = true)]
    public sealed class SafeProcessHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private SafeProcessHandle() : base(true) { }

        public SafeProcessHandle(IntPtr handle)
            : base(false)
        {
            this.SetHandle(handle);
        }

        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.MayFail)]
        protected override bool ReleaseHandle()
        {
            return NativeMethods.CloseHandle(this.handle);
        }
    }
    
    [SecurityPermission(SecurityAction.InheritanceDemand, UnmanagedCode = true)]
    [SecurityPermission(SecurityAction.Demand, UnmanagedCode = true)]
    public sealed class SafeImpersonationToken : SafeHandleZeroOrMinusOneIsInvalid
    {
        private SafeImpersonationToken() : base(true) { }

        public SafeImpersonationToken(IntPtr token)
            : base(false)
        {
            this.SetHandle(token);
        }

        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.MayFail)]
        protected override bool ReleaseHandle()
        {
            return NativeMethods.CloseHandle(this.handle);
        }

    }

    /// <summary>
    /// Wrapped native methods
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    public static class NativeMethods
    {
        public static readonly IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        // Create process
        public const uint CREATE_UNICODE_ENVIRONMENT = 0x00000400;
        public const uint CREATE_SUSPENDED = 0x00000004;
        public const uint CREATE_BREAKAWAY_FROM_JOB = 0x01000000;
        public const uint CREATE_NO_WINDOW = 0x08000000;
        
        // LogonUser
        public const uint LOGON32_LOGON_INTERACTIVE = 0x00000002;
        public const uint LOGON32_LOGON_NETWORK = 0x00000003;
        public const uint LOGON32_LOGON_BATCH = 0x00000004;
        public const uint LOGON32_LOGON_NETWORK_CLEARTEXT = 0x00000008;
        public const uint LOGON32_PROVIDER_DEFAULT = 0x00000000;



        /// <summary>
        /// Error flag for "no error"
        /// </summary>
        public const int ERROR_OK = 0;

        /// <summary>
        /// Error flag for insufficient buffer
        /// </summary>
        public const int ERROR_INSUFFICIENT_BUFFER = 122;


        // Job object
        public const int JobObjectExtendedLimitInformationClass = 9;
        public const uint JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000;

        [StructLayout(LayoutKind.Sequential)]
        public struct PROCESS_INFORMATION
        {
            public IntPtr hProcess;
            public IntPtr hThread;
            public int dwProcessId;
            public int dwThreadId;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        public struct STARTUPINFO
        {
            public Int32 cb;
            public string lpReserved;
            public string lpDesktop;
            public string lpTitle;
            public Int32 dwX;
            public Int32 dwY;
            public Int32 dwXSize;
            public Int32 dwYSize;
            public Int32 dwXCountChars;
            public Int32 dwYCountChars;
            public Int32 dwFillAttribute;
            public Int32 dwFlags;
            public Int16 wShowWindow;
            public Int16 cbReserved2;
            public IntPtr lpReserved2;
            public IntPtr hStdInput;
            public IntPtr hStdOutput;
            public IntPtr hStdError;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct JobObjectExtendedLimitInformation
        {
            public Int64 PerProcessUserTimeLimit;
            public Int64 PerJobUserTimeLimit;
            public UInt32 LimitFlags;
            public UIntPtr MinimumWorkingSetSize;
            public UIntPtr MaximumWorkingSetSize;
            public UInt32 ActiveProcessLimit;
            public IntPtr Affinity;
            public UInt32 PriorityClass;
            public UInt32 SchedulingClass;
            public UInt64 ReadOperationCount;
            public UInt64 WriteOperationCount;
            public UInt64 OtherOperationCount;
            public UInt64 ReadTransferCount;
            public UInt64 WriteTransferCount;
            public UInt64 OtherTransferCount;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct SECURITY_ATTRIBUTES
        {
            public UInt32 nLength;
            public UIntPtr lpSecurityAttributes;
        }

        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool CreateProcess([MarshalAs(UnmanagedType.LPTStr)]string lpApplicationName,
           StringBuilder lpCommandLine, IntPtr lpProcessAttributes,
           IntPtr lpThreadAttributes, bool bInheritHandles,
           uint dwCreationFlags, IntPtr lpEnvironment, string lpCurrentDirectory,
           [In] ref STARTUPINFO lpStartupInfo,
           out PROCESS_INFORMATION lpProcessInformation);

        [DllImport("Advapi32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool CreateProcessAsUser(SafeImpersonationToken hToken, [MarshalAs(UnmanagedType.LPTStr)]string lpApplicationName,
           StringBuilder lpCommandLine, IntPtr lpProcessAttributes,
           IntPtr lpThreadAttributes, bool bInheritHandles,
           uint dwCreationFlags, IntPtr lpEnvironment, string lpCurrentDirectory,
           [In] ref STARTUPINFO lpStartupInfo,
           out PROCESS_INFORMATION lpProcessInformation);

        [DllImport("Advapi32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public extern static bool LogonUser(string lpszUserName, string lpszDomain, string lpszPassword,
            uint dwLogonType, uint dwLogonProvider, out SafeImpersonationToken phToken
            );

        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetExitCodeProcess(SafeProcessHandle hProcess, out uint lpExitCode);

        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool TerminateProcess(SafeProcessHandle hProcess, int uExitCode);

        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true)]
        public static extern uint ResumeThread(SafeThreadHandle hThread);

        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string lpName);

        public const int JobObjectExtendedLimitInformationQuery = 9;
        public const int JobObjectExtendedLimitInformationSet = 9;

        public const int QueryJobObjectBasicProcessIdList = 3;
        [StructLayout(LayoutKind.Sequential)]
        public struct JobObjectBasicProcessIdListHeader
        {
            public UInt32 NumberOfAssignedProcesses;
            public UInt32 NumberOfProcessIdsInList;
        }
        
        [DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Auto, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public extern static bool QueryInformationJobObject(
            IntPtr hJob,
            int query,
            out JobObjectExtendedLimitInformation info,
            int size,
            out int returnedSize
            );
        
        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public extern static bool SetInformationJobObject(IntPtr hJob, int informationClass, [In] ref JobObjectExtendedLimitInformation info, int size);

        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Winapi, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public extern static bool AssignProcessToJobObject(IntPtr hJob, SafeProcessHandle hProcess);

        [DllImport("kernel32.dll", SetLastError = true)]
        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
        public extern static bool CloseHandle(IntPtr handle);

        [DllImport("kernel32.dll", SetLastError = true)]
        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
        public extern static bool CloseHandle(HandleRef handleRef);

        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
        public static void SafeCloseValidHandle(HandleRef handleRef)
        {
            if (handleRef.Handle != IntPtr.Zero && handleRef.Handle != INVALID_HANDLE_VALUE)
            {
                try
                {
                    CloseHandle(handleRef);
                }
                catch
                {
                    // Swallow exception
                }
            }
        }


        /// <summary>
        /// contains information about the current state of both physical and virtual memory, including extended memory
        /// </summary>
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
        public class MEMORYSTATUSEX
        {
            /// <summary>
            /// Size of the structure, in bytes. You must set this member before calling GlobalMemoryStatusEx. 
            /// </summary>
            public uint dwLength;

            /// <summary>
            /// Number between 0 and 100 that specifies the approximate percentage of physical memory that is in use (0 indicates no memory use and 100 indicates full memory use). 
            /// </summary>
            public uint dwMemoryLoad;

            /// <summary>
            /// Total size of physical memory, in bytes.
            /// </summary>
            public ulong ullTotalPhys;

            /// <summary>
            /// Size of physical memory available, in bytes. 
            /// </summary>
            public ulong ullAvailPhys;

            /// <summary>
            /// Size of the committed memory limit, in bytes. This is physical memory plus the size of the page file, minus a small overhead. 
            /// </summary>
            public ulong ullTotalPageFile;



            /// <summary>
            /// Size of available memory to commit, in bytes. The limit is ullTotalPageFile. 
            /// </summary>
            public ulong ullAvailPageFile;

            /// <summary>
            /// Total size of the user mode portion of the virtual address space of the calling process, in bytes. 
            /// </summary>
            public ulong ullTotalVirtual;

            /// <summary>
            /// Size of unreserved and uncommitted memory in the user mode portion of the virtual address space of the calling process, in bytes. 
            /// </summary>
            public ulong ullAvailVirtual;

            /// <summary>
            /// Size of unreserved and uncommitted memory in the extended portion of the virtual address space of the calling process, in bytes. 
            /// </summary>
            public ulong ullAvailExtendedVirtual;

            /// <summary>
            /// Initializes a new instance of the <see cref="T:MEMORYSTATUSEX"/> class.
            /// </summary>
            public MEMORYSTATUSEX()
            {
                this.dwLength = (uint)Marshal.SizeOf(typeof(NativeMethods.MEMORYSTATUSEX));
            }
        }

        /// <summary>
        /// Retrieves information about the system's current usage of both physical and virtual memory.
        /// </summary>
        /// <param name="lpBuffer">A pointer to a MEMORYSTATUSEX structure that receives information about current memory availability</param>
        /// <returns>If the function succeeds, the return value is nonzero. Error code otherwise.</returns>
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);

        /// <summary>
        /// Retrieves information about the amount of space that is available on a disk volume, which is the total amount of space, 
        /// the total amount of free space, and the total amount of free space available to the user that is associated with the calling thread.
        /// </summary>
        /// <param name="lpDirectoryName">A directory on the disk.</param>
        /// <param name="lpFreeBytesAvailable">A pointer to a variable that receives the total number of free bytes on a disk that are available to the user who is associated with the calling thread.</param>
        /// <param name="lpTotalNumberOfBytes">A pointer to a variable that receives the total number of bytes on a disk that are available to the user who is associated with the calling thread.</param>
        /// <param name="lpTotalNumberOfFreeBytes">A pointer to a variable that receives the total number of free bytes on a disk.</param>
        /// <returns></returns>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern bool GetDiskFreeSpaceEx(string lpDirectoryName,
           out ulong lpFreeBytesAvailable,
           out ulong lpTotalNumberOfBytes,
           out ulong lpTotalNumberOfFreeBytes);

        /// <summary>
        /// SID Usage Enum
        /// </summary>
        public enum SID_NAME_USE
        {
            SidTypeUser = 1,
            SidTypeGroup,
            SidTypeDomain,
            SidTypeAlias,
            SidTypeWellKnownGroup,
            SidTypeDeletedAccount,
            SidTypeInvalid,
            SidTypeUnknown,
            SidTypeComputer
        }

        /// <summary>
        /// Get SID for account name
        /// </summary>
        /// <param name="lpSystemName">Compute name</param>
        /// <param name="lpAccountName">Account name</param>
        /// <param name="Sid">Security ID</param>
        /// <param name="cbSid">Number of bytes needed to hold the SID</param>
        /// <param name="ReferencedDomainName">Domain name reference by SID</param>
        /// <param name="cchReferencedDomainName">Number of bytes needed to hold the domain</param>
        /// <param name="peUse">Account type</param>
        /// <returns>error flag</returns>
        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern bool LookupAccountName(
            string lpSystemName,
            string lpAccountName,
            [MarshalAs(UnmanagedType.LPArray)] byte[] Sid,
            ref uint cbSid,
            StringBuilder ReferencedDomainName,
            ref uint cchReferencedDomainName,
            out SID_NAME_USE peUse);

        /// <summary>
        /// Retrieves the name of the account for this SID and the name of the first domain on which this SID is found
        /// </summary>
        /// <param name="lpSystemName">string that specifies the target computer</param>
        /// <param name="Sid">the SID to look up</param>
        /// <param name="lpName">buffer that receives the account name that corresponds to the Sid parameter</param>
        /// <param name="cchName">On input, specifies the size of the lpName buffer. If the function fails because 
        ///                       the buffer is too small or if cchName is zero, cchName receives the required buffer size</param>
        /// <param name="ReferencedDomainName">buffer that receives the name of the domain where the account name was found.</param>
        /// <param name="cchReferencedDomainName">Same as cchName, but for the domain string buffer</param>
        /// <param name="peUse">pointer to a variable that receives a SID_NAME_USE value that indicates the type of the account</param>
        /// <returns>If the function succeeds, the function returns nonzero.If the function fails, it returns zero</returns>
        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern bool LookupAccountSid(
            string lpSystemName,
            [MarshalAs(UnmanagedType.LPArray)] byte[] Sid,
            StringBuilder lpName,
            ref uint cchName,
            StringBuilder ReferencedDomainName,
            ref uint cchReferencedDomainName,
            out SID_NAME_USE peUse);  

        /// <summary>
        /// Converts a security ID pointer to the string value
        /// </summary>
        /// <param name="pSID">pointer to SID</param>
        /// <param name="ptrSid">string value</param>
        /// <returns>error flag</returns>
        [DllImport("advapi32", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern bool ConvertSidToStringSid(
            [MarshalAs(UnmanagedType.LPArray)] byte[] pSID,
            out IntPtr ptrSid);

        /// <summary>
        /// Frees a pointer
        /// </summary>
        /// <param name="hMem">pointer to free</param>
        /// <returns>error flag</returns>
        [DllImport("kernel32.dll")]
        public static extern IntPtr LocalFree(IntPtr hMem);
    }
}
