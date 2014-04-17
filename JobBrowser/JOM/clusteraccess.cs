
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

using System.Text.RegularExpressions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using Microsoft.Research.Peloponnese.Storage;
using Microsoft.Research.Tools;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Research.JobObjectModel
{
    /// <summary>
    /// A cluster-resident object is a file or a folder.
    /// </summary>
    public interface IClusterResidentObject
    {
        /// <summary>
        /// True if the object is a folder.
        /// </summary>
        bool RepresentsAFolder { get; }
        /// <summary>
        /// Returns a stream that can be used to access the contents of the object, if the object is not a folder.
        /// </summary>
        /// <returns>A stream that can be used to access the object contents.</returns>
        ISharedStreamReader GetStream();
        /// <summary>
        /// If the current object is a folder, it returns the contained objects.
        /// </summary>
        /// <returns>An iterator over all contained objects that match the specified string.</returns>
        /// <param name="match">A shell expression (similar to the argument of Directory.GetFiles()).</param>
        IEnumerable<IClusterResidentObject> GetFilesAndFolders(string match);
        /// <summary>
        /// Size of the object in bytes (if not a folder).  The size can be -1 when it is unknown.
        /// </summary>
        long Size { get; }
        /// <summary>
        /// An exception is stored here if the object could not be manipulated.
        /// </summary>
        Exception Exception { get; }
        /// <summary>
        /// Short name of the object.
        /// </summary>
        string Name { get; }
        /// <summary>
        /// Date when object was created.
        /// </summary>
        DateTime CreationTime { get; }
        /// <summary>
        /// For a folder object, returns the contained file with the specified name.
        /// </summary>
        /// <param name="filename">File name within the folder.</param>
        /// <returns>The file within the folder.</returns>
        IClusterResidentObject GetFile(string filename);
        /// <summary>
        /// For a folder object, returns the contained folder with the specified name.
        /// </summary>
        /// <param name="foldername">Folder name within the folder.</param>
        /// <returns>The subfolder within the folder.</returns>
        IClusterResidentObject GetFolder(string foldername);
        /// <summary>
        /// If false do not cache this object.  For a folder, do not cache anything underneath that is reached through this folder.
        /// </summary>
        bool ShouldCacheLocally { get; set;  }
    }

    /// <summary>
    /// Common substrate for caching object contents locally.
    /// </summary>
    [Serializable]
    public abstract class CachedClusterResidentObject : IClusterResidentObject
    {
        /// <summary>
        /// Cluster where the file resides.
        /// </summary>
        public ClusterConfiguration Config { get; protected set; }
        /// <summary>
        /// Cache files from the cluster in this directory.  If null do not cache files.
        /// </summary>
        public static string CacheDirectory { get; set; }
        /// <summary>
        /// If false do not cache this object.
        /// </summary>
        public bool ShouldCacheLocally { get; set; }
        /// <summary>
        /// The path to the local cached version of the object; if null the object is not cached.
        /// </summary>
        public string LocalCachePath { get; protected set; }
        /// <summary>
        /// Job that owns the files.
        /// </summary>
        public DryadLinqJobSummary Job { get; protected set; }

        /// <summary>
        /// Remember for each job all the locally cached files.
        /// </summary>
        static Dictionary<DryadLinqJobSummary, HashSet<string>> perJobFiles = new Dictionary<DryadLinqJobSummary, HashSet<string>>();

        /// <summary>
        /// Initialize an empty cached cluster resident object.
        /// </summary>
        /// <param name="config">Cluster where the file resides.</param>
        /// <param name="job">Job who owns these files.</param>
        protected CachedClusterResidentObject(ClusterConfiguration config, DryadLinqJobSummary job)
        {
            this.cacheWriter = null;
            this.tempFileName = null;
            this.Job = job;
            this.Config = config;
        }

        /// <summary>
        /// Record that the job owns this cached file.
        /// </summary>
        /// <param name="job">Job.</param>
        /// <param name="path">Cached file belonging to this job.</param>
        public static void RecordCachedFile(DryadLinqJobSummary job, string path)
        {
            HashSet<string> list;
            if (!perJobFiles.ContainsKey(job))
            {
                list = new HashSet<string>();
                perJobFiles.Add(job, list);
            }
            else
            {
                list = perJobFiles[job];
            }
            list.Add(path);
        }

        /// <summary>
        /// Record the creation of this particular cluster resident object so it can be retrieved from the job summary.
        /// </summary>
        /// <param name="file">Object to record.</param>
        private static void Record(CachedClusterResidentObject file)
        {
            if (file.Job == null)
                return;
            if (string.IsNullOrEmpty(file.LocalCachePath))
                throw new ClusterException("Missing expected LocalCachePath");

            CachedClusterResidentObject.RecordCachedFile(file.Job, file.LocalCachePath);
        }

        /// <summary>
        /// Get all the files cached associated with a given job.
        /// </summary>
        /// <param name="job">Job with cached files.</param>
        /// <returns>An iterator over all files cached belonging to this job.</returns>
        public static IEnumerable<string> CachedJobFiles(DryadLinqJobSummary job)
        {
            if (perJobFiles.ContainsKey(job))
                return perJobFiles[job];
            return new List<string>();
        }

        /// <summary>
        /// Stream used to write to cache.
        /// </summary>
        private StreamWriter cacheWriter;
        /// <summary>
        /// Cache to a temporary file, and then rename when the file is closed.
        /// </summary>
        private string tempFileName;

        /// <summary>
        /// Create a temporary file-backed local stream.  (Save it in the cacheWriter.)
        /// </summary>
        /// <returns>The writer to the temp stream created.</returns>
        protected StreamWriter CreateTempStream()
        {
            this.tempFileName = Path.GetTempFileName();
            this.cacheWriter = new StreamWriter(this.tempFileName);
            return this.cacheWriter;
        }

        /// <summary>
        /// True if this is a folder.
        /// </summary>
        public virtual bool RepresentsAFolder { get; protected set;  }
        /// <summary>
        /// A stream to the local cache, or null if the file is not cached.
        /// </summary>
        /// <returns>A stream to access the file.</returns>
        public virtual ISharedStreamReader GetStream()
        {
            if (this.LocalCachePath != null && File.Exists(this.LocalCachePath))
            {
                CachedClusterResidentObject.Record(this);
                return new FileSharedStreamReader(this.LocalCachePath);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="match">Expression matching children.</param>
        /// <returns></returns>
        public abstract IEnumerable<IClusterResidentObject> GetFilesAndFolders(string match);

        /// <summary>
        /// Size of the object in bytes (if not a folder).  The size can be -1 when it is unknown.
        /// </summary>
        public virtual long Size {
            get
            {
                if (this.LocalCachePath != null && File.Exists(this.LocalCachePath))
                {
                    FileInfo info = new FileInfo(this.LocalCachePath);
                    return info.Length;
                }
                return -1;
            }
        }

        /// <summary>
        /// Exception stored here if the object could not be manipulated.
        /// </summary>
        public Exception Exception { get; protected set; }
        /// <summary>
        /// Short name of the object.
        /// </summary>
        public virtual string Name { get; protected set; }
        /// <summary>
        /// Date when object was created.
        /// </summary>
        public virtual DateTime CreationTime { get; protected set;  }

        /// <summary>
        /// For a folder, the file with the specified name within the folder.
        /// </summary>
        /// <param name="filename">File to find.</param>
        /// <returns>An object corresponding to the specified file.</returns>
        public abstract IClusterResidentObject GetFile(string filename);

        /// <summary>
        /// For a folder object, returns the contained folder with the specified name.
        /// </summary>
        /// <param name="foldername">Folder name within the folder.</param>
        /// <returns>The subfolder within the folder.</returns>
        public abstract IClusterResidentObject GetFolder(string foldername);

        /// <summary>
        /// This is closed when the cached file has been completely read.
        /// </summary>
        protected void OnClose()
        {
            // save the file to its proper place
            if (this.cacheWriter != null)
            {
                // this.cacheWriter.Close(); -- the callee should have done this already
                try
                {
                    Utilities.EnsureDirectoryExistsForFile(this.LocalCachePath);
                    Utilities.Move(this.tempFileName, this.LocalCachePath);
                    Trace.TraceInformation("Writing to cache {0}", this.LocalCachePath);
                    CachedClusterResidentObject.Record(this);
                }
                catch (Exception e)
                {
                    Trace.TraceInformation("Exception {0} during move", e.Message);
                }
                this.tempFileName = null;
                this.cacheWriter = null;
            }
        }
    }

    /// <summary>
    /// A cluster-resident object, accessed through its UNC pathname.
    /// </summary>
    [Serializable]
    public class UNCFile : CachedClusterResidentObject
    {       
        /// <summary>
        /// Path to the object, if it is accessed through a path; could be null.
        /// </summary>
        public UNCPathname Pathname { get; protected set; }

        /// <summary>
        /// True if the object is a folder.
        /// </summary>
        public override bool RepresentsAFolder {
            get
            {
                return this.Pathname.IsDirectory;
            }
        }

        /// <summary>
        /// Create a cluster resident object corresponding to a given pathname.
        /// </summary>
        /// <param name="path">Path to the cluster-resident object.</param>
        /// <param name="config">Cluster where the file resides.</param>
        /// <param name="shouldCache">If true the file should be cached.</param>
        /// <param name="job">Job who owns this file.</param>
        public UNCFile(ClusterConfiguration config, DryadLinqJobSummary job, UNCPathname path, bool shouldCache) : base(config, job)
        {
            this.Pathname = path;
            this.Exception = null;
            this.ShouldCacheLocally = shouldCache;
            //if (! this.RepresentsAFolder)
                this.LocalCachePath = this.CachePath(this.Pathname);
        }

        /// <summary>
        /// From the URL extract a Path to a filename in the local cache.
        /// </summary>
        /// <param name="path">Path that is to be cached.</param>
        /// <returns>A local pathname, or null if file should not be cached.</returns>
        private string CachePath(UNCPathname path)
        {
            if (CachedClusterResidentObject.CacheDirectory == null || !this.ShouldCacheLocally)
                return null;

            {
                return null;
            }
        }

        /// <summary>
        /// Create a cluster-resident object that only contains an exception.
        /// </summary>
        /// <param name="ex">Exception that occurred when building the object.</param>
        public UNCFile(Exception ex) : base(null, null)
        {
            this.Exception = ex;
            this.Pathname = null;
            this.ShouldCacheLocally = false;
        }

        /// <summary>
        /// The stream with the file contents.
        /// </summary>
        /// <returns>A stream reder.</returns>
        public override ISharedStreamReader GetStream()
        {
            try
            {
                if (!this.RepresentsAFolder)
                {
                    //this.LocalCachePath = this.CachePath(this.Pathname);
                    ISharedStreamReader baseStream = base.GetStream();
                    if (baseStream != null)
                    {
                        // file is cached
                        Trace.TraceInformation("Reading from local cache {0}", baseStream);
                        return baseStream;
                    }
                }

                if (this.LocalCachePath != null && this.ShouldCacheLocally)
                {
                    // cache it 
                    if (this.RepresentsAFolder)
                        throw new ClusterException("Cannot cache folders");

                    StreamWriter writer = this.CreateTempStream();
                    return new FileSharedStreamReader(this.Pathname.ToString(), writer, this.OnClose);
                }
                else
                {
                    // dont cache it
                    return new FileSharedStreamReader(this.Pathname.ToString());
                }
            }
            catch (Exception ex)
            {
                return new FileSharedStreamReader(ex);
            }
        }

        /// <summary>
        /// The contents of the folder.
        /// </summary>
        /// <param name="match">Pattern to match.</param>
        /// <returns>The matching objects.</returns>
        public override IEnumerable<IClusterResidentObject> GetFilesAndFolders(string match)
        {
            if (!this.RepresentsAFolder)
                yield break;
            string[] dirs = null, files = null;
            Exception exception = null;

            try
            {
                dirs = Directory.GetDirectories(this.Pathname.ToString(), match);
            }
            catch (Exception ex) {
                exception = ex;
            }
            if (exception != null)
            {
                yield return new UNCFile(exception);
                yield break;
            }

            foreach (string dir in dirs)
            {
                UNCPathname dirpath = new UNCPathname(this.Pathname);
                // ReSharper disable once AssignNullToNotNullAttribute
                dirpath.Directory = Path.Combine(dirpath.Directory, Path.GetFileName(dir));
                yield return new UNCFile(this.Config, this.Job, dirpath, this.ShouldCacheLocally);
            }

            try
            {
                files = Directory.GetFiles(this.Pathname.ToString(), match);
            }
            catch (Exception ex) {
                exception = ex;
            }
            if (exception != null)
            {
                yield return new UNCFile(exception);
                yield break;
            }
                
            foreach (string file in files)
            {
                UNCPathname dirpath = new UNCPathname(this.Pathname);
                dirpath.Filename = Path.GetFileName(file);
                yield return new UNCFile(this.Config, this.Job, dirpath, this.ShouldCacheLocally);
            }
        }

        /// <summary>
        /// For a folder, the file with the specified name within the folder.
        /// </summary>
        /// <param name="filename">File to find.</param>
        /// <returns>An object corresponding to the specified file.</returns>
        public override IClusterResidentObject GetFile(string filename)
        {
            if (!this.RepresentsAFolder)
                throw new InvalidOperationException("Cannot find file within non-folder");

            UNCPathname dirpath = new UNCPathname(this.Pathname);
            dirpath.Filename = Path.GetFileName(filename);
            return new UNCFile(this.Config, this.Job, dirpath, this.ShouldCacheLocally);
        }

        /// <summary>
        /// Date when object was created.
        /// </summary>
        public override DateTime CreationTime
        {
            get
            {
                if (this.Exception != null)
                    return DateTime.MinValue;
                if (this.RepresentsAFolder)
                {
                    return Directory.GetCreationTime(this.Pathname.ToString());
                }
                else
                {
                    return File.GetCreationTime(this.Pathname.ToString());
                }
            }
        }

        /// <summary>
        /// For a folder object, returns the contained folder with the specified name.
        /// </summary>
        /// <param name="foldername">Folder name within the folder.</param>
        /// <returns>The subfolder within the folder.</returns>
        public override IClusterResidentObject GetFolder(string foldername)
        {
            if (!this.RepresentsAFolder)
                throw new InvalidOperationException("Cannot find file within non-folder");

            UNCPathname dirpath = new UNCPathname(this.Pathname);
            dirpath.Directory = Path.Combine(dirpath.Directory, foldername);
            return new UNCFile(this.Config, this.Job, dirpath, this.ShouldCacheLocally);
        }

        /// <summary>
        /// Size of the file.
        /// </summary>
        public override long Size
        {
            get
            {
                if (this.RepresentsAFolder)
                    throw new ClusterException("Cannot get size of a folder");
                if (File.Exists(this.LocalCachePath))
                {
                    FileInfo info = new FileInfo(this.LocalCachePath);
                    return info.Length;
                }
                if (File.Exists(this.Pathname.ToString()))
                {
                    FileInfo info = new FileInfo(this.Pathname.ToString());
                    return info.Length;
                }
                return -1;
            }
        }

        /// <summary>
        /// String representation of the file.
        /// </summary>
        /// <returns>A string describing the file.</returns>
        public override string ToString()
        {
            if (this.Exception != null)
                return "Exception: " + this.Exception.Message;
            return this.Pathname.ToString();
        }

        /// <summary>
        /// Short name of the object.
        /// </summary>
        public override string Name
        {
            get
            {
                if (this.Exception != null)
                    return "Exception";
                else if (this.Pathname.IsDirectory)
                    return Path.GetFileName(this.Pathname.Directory);
                else
                    return this.Pathname.Filename;
            }
        }
    }

    /// <summary>
    /// A wrapper around a folder on a cached cluster.  Since folders are not cached, we use this trick to find files in the cache.
    /// </summary>
    [Serializable]
    public class FolderInCachedCluster : CachedClusterResidentObject
    {
        /// <summary>
        /// Original folder which is cached.
        /// </summary>
        public CachedClusterResidentObject OriginalFolder { get; protected set; }

        /// <summary>
        /// Create a wrapper around a folder in the cache.
        /// </summary>
        /// 
        /// <param name="folder">Folder to represent.</param>
        public FolderInCachedCluster(CachedClusterResidentObject folder)
            : base(folder.Config, folder.Job)
        {
            if (!folder.RepresentsAFolder)
                throw new ArgumentException(folder + " is not a folder");
            this.OriginalFolder = folder;
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            this.RepresentsAFolder = true;
        }

        /// <summary>
        /// The contents of the folder.
        /// </summary>
        /// <param name="match">Pattern to match.</param>
        /// <returns>The matching objects.</returns>
        public override IEnumerable<IClusterResidentObject> GetFilesAndFolders(string match)
        {
            if (!this.RepresentsAFolder)
                yield break;
            string[] dirs = null, files = null;
            Exception exception = null;

            try
            {
                dirs = Directory.GetDirectories(this.OriginalFolder.LocalCachePath, match);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            if (exception != null)
            {
                yield return new UNCFile(exception);
                yield break;
            }

            foreach (string dir in dirs)
            {
                IClusterResidentObject folder = this.OriginalFolder.GetFolder(dir);
                yield return new FolderInCachedCluster(folder as CachedClusterResidentObject);
            }

            try
            {
                files = Directory.GetFiles(this.OriginalFolder.LocalCachePath, match);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            if (exception != null)
            {
                yield return new UNCFile(exception);
                yield break;
            }

            foreach (string file in files)
            {
                IClusterResidentObject originalFile = this.OriginalFolder.GetFile(Path.GetFileName(file));
                yield return originalFile;
            }
        }

        /// <summary>
        /// The file with the specified name within the folder.
        /// </summary>
        /// <param name="filename">File to find.</param>
        /// <returns>An object corresponding to the specified file.</returns>
        public override IClusterResidentObject GetFile(string filename)
        {
            return this.OriginalFolder.GetFile(filename);
        }

        /// <summary>
        /// Returns the contained folder with the specified name.
        /// </summary>
        /// <param name="foldername">Folder name within the folder.</param>
        /// <returns>The subfolder within the folder.</returns>
        public override IClusterResidentObject GetFolder(string foldername)
        {
            return new FolderInCachedCluster(this.OriginalFolder.GetFolder(foldername) as CachedClusterResidentObject);
        }

        /// <summary>
        /// The contents of the folder.
        /// </summary>
        /// <returns>The contents of the folder.</returns>
        public override ISharedStreamReader GetStream()
        {
            return this.OriginalFolder.GetStream();
        }

        /// <summary>
        /// String representation of the Folder.
        /// </summary>
        /// <returns>A string describing the folder.</returns>
        public override string ToString()
        {
            return this.OriginalFolder.ToString();
        }
    }


    /// <summary>
    /// A constant string extracted from (part) of a file.
    /// </summary>
    [Serializable]
    public class ContentsOfFile : IClusterResidentObject
    {
        IEnumerable<string> contents;

        /// <summary>
        /// The contents of the file.
        /// </summary>
        /// <param name="contents">Contents of the file.</param>
        /// <param name="creationTime">Time file was created.</param>
        /// <param name="name">File name.</param>
        public ContentsOfFile(
            IEnumerable<string> contents,
            DateTime creationTime,
            string name)
        {
            this.contents = contents;
            this.CreationTime = creationTime;
            this.Name = name;
            this.size = -1;            
            this.Exception = null;
        }

        /// <summary>
        /// Never cache locally.
        /// </summary>
        public bool ShouldCacheLocally { get { return false; } set { }  }

        /// <summary>
        /// Exception occurred while obtaining file.
        /// </summary>
        /// <param name="ex">Exception that occurred.</param>
        public ContentsOfFile(Exception ex)
        {
            this.Exception = ex;
            this.contents = null;
            this.size = 0;
        }

        /// <summary>
        /// True if this is a folder; never.
        /// </summary>
        public bool RepresentsAFolder
        {
            get { return false; }
        }

        /// <summary>
        /// A stream returning the contents.
        /// </summary>
        /// <returns>The contents of this object.</returns>
        public ISharedStreamReader GetStream()
        {
            return new StringIteratorStreamReader(this.contents);
        }

        /// <summary>
        /// The files and folders contained in the string.  Throws an exception.
        /// </summary>
        /// <param name="match">Return only matching files.</param>
        /// <returns>Throws an exception.</returns>
        public IEnumerable<IClusterResidentObject> GetFilesAndFolders(string match)
        {
            throw new ClusterException("Object is not a folder");
        }

        private long size;
        /// <summary>
        /// The size of the contents.
        /// </summary>
        public long Size
        {
            get
            {
                if (this.size == -1)
                {
                    this.size = 0;
                    foreach (var c in this.contents)
                        this.size += c.Length;
                }
                return this.size;
            }
        }

        /// <summary>
        /// Exception thrown by this stream.
        /// </summary>
        public Exception Exception
        {
            get;
            protected set;
        }

        /// <summary>
        /// Name of this stream.
        /// </summary>
        public string Name
        {
            get;
            protected set;
        }

        /// <summary>
        /// Stream creation time.
        /// </summary>
        public DateTime CreationTime
        {
            get;
            protected set;
        }

        /// <summary>
        /// Contained file with the specified name.
        /// </summary>
        /// <param name="filename">File with specified name.</param>
        /// <returns>Throws an exception.</returns>
        public IClusterResidentObject GetFile(string filename)
        {
            throw new ClusterException("Object is not a folder");
        }

        /// <summary>
        /// Contained folder with the specified name.
        /// </summary>
        /// <param name="foldername">Folder with specified name.</param>
        /// <returns>Throws an exception.</returns>
        public IClusterResidentObject GetFolder(string foldername)
        {
            throw new ClusterException("Object is not a folder");
        }
    }

    /// <summary>
    /// A file on the local machine.
    /// </summary>
    public class LocalFile : IClusterResidentObject
    {
        private string path;
        /// <summary>
        /// Cached here on demand.
        /// </summary>
        private FileInfo info;

        /// <summary>
        /// A local file reachable with the specified path.
        /// </summary>
        /// <param name="path">Path to file.</param>
        public LocalFile(string path)
        {
            this.path = path;
            this.Exception = null;
            this.info = null;
        }

        /// <summary>
        /// True if the object is a folder.
        /// </summary>
        public bool RepresentsAFolder
        {
            get { return Directory.Exists(this.path); }
        }

        /// <summary>
        /// Returns a stream that can be used to access the contents of the object, if the object is not a folder.
        /// </summary>
        /// <returns>A stream that can be used to access the object contents.</returns>
        public ISharedStreamReader GetStream()
        {
            return new FileSharedStreamReader(this.path);
        }

        /// <summary>
        /// If the current object is a folder, it returns the contained objects.
        /// </summary>
        /// <returns>An iterator over all contained objects that match the specified string.</returns>
        /// <param name="match">A shell expression (similar to the argument of Directory.GetFiles()).</param>
        public IEnumerable<IClusterResidentObject> GetFilesAndFolders(string match)
        {
            foreach (var p in Directory.GetFiles(this.path, match))
                yield return new LocalFile(p);
            foreach (var p in Directory.GetDirectories(this.path, match))
                yield return new LocalFile(p);
        }

        /// <summary>
        /// Size of the object in bytes (if not a folder).  The size can be -1 when it is unknown.
        /// </summary>
        public long Size
        {
            get
            {
                if (this.info == null)
                    this.info = new FileInfo(this.path);
                return this.info.Length; 
            }
        }

        /// <summary>
        /// An exception is stored here if the object could not be manipulated.
        /// </summary>
        public Exception Exception
        {
            get;
            private set;
        }

        /// <summary>
        /// Short name of the object.
        /// </summary>
        public string Name
        {
            get { return Path.GetFileName(this.path); }
        }

        /// <summary>
        /// Date when object was created.
        /// </summary>
        public DateTime CreationTime
        {
            get { return File.GetCreationTime(this.path); }
        }

        /// <summary>
        /// For a folder object, returns the contained file with the specified name.
        /// </summary>
        /// <param name="filename">File name within the folder.</param>
        /// <returns>The file within the folder.</returns>
        public IClusterResidentObject GetFile(string filename)
        {
            return new LocalFile(Path.Combine(this.path, filename));
        }

        /// <summary>
        /// For a folder object, returns the contained folder with the specified name.
        /// </summary>
        /// <param name="foldername">Folder name within the folder.</param>
        /// <returns>The subfolder within the folder.</returns>
        public IClusterResidentObject GetFolder(string foldername)
        {
            return new LocalFile(Path.Combine(this.path, foldername));
        }

        /// <summary>
        /// If false do not cache this object.  For a folder, do not cache anything underneath that is reached through this folder.
        /// </summary>
        public bool ShouldCacheLocally
        {
            get
            {
                return false;
            }
            set
            {
                // noop
            }
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString()
        {
            return this.path;
        }
    }

    /// <summary>
    /// A file residing on AzureDfs.
    /// </summary>
    public class AzureDfsFile : CachedClusterResidentObject
    {
        private string path;
        private AzureDfsClient client;
        /// <summary>
        /// If true the file is a DFS stream, otherwise it's an azure log.
        /// </summary>
        public bool IsDfsStream;
        
        /// <summary>
        /// A file with the specified path.
        /// </summary>
        /// <param name="path">Path to the file.</param>
        /// <param name="client">Azure client.</param>
        /// <param name="config">Cluster configuration.</param>
        /// <param name="job">Job accessing this file.</param>
        /// <param name="isFolder">If true this must be a folder.</param>
        /// <param name="canCache">True if the file can be cached (it is immutable for sure).</param>
        public AzureDfsFile(ClusterConfiguration config, DryadLinqJobSummary job, AzureDfsClient client, string path, bool canCache, bool isFolder)
            : base(config, job)
        {
            this.client = client;
            this.path = path;
            this.ShouldCacheLocally = canCache;
            this.RepresentsAFolder = isFolder;
            this.size = -1;

            if (!string.IsNullOrEmpty(CachedClusterResidentObject.CacheDirectory))
                this.LocalCachePath = Path.Combine(CachedClusterResidentObject.CacheDirectory, this.path);
        }

        /// <summary>
        /// True if the object is a folder.
        /// </summary>
        public override bool RepresentsAFolder
        {
            get;
            protected set;
        }

        /// <summary>
        /// Returns a stream that can be used to access the contents of the object, if the object is not a folder.
        /// </summary>
        /// <returns>A stream that can be used to access the object contents.</returns>
        public override ISharedStreamReader GetStream()
        {

            ISharedStreamReader baseStream = base.GetStream();
            if (baseStream != null)
            {
                // file is cached
                Trace.TraceInformation("Reading from local cache {0}", baseStream);
                return baseStream;
            }

            Stream stream;
            if (this.IsDfsStream)
            {
                var dfsFileStream = this.client.GetDfsFileStream(this.path);
                stream = dfsFileStream.Stream;
            }
            else
            {
                stream = new AzureLogReaderStream(
                    this.client.AccountName,
                    this.client.AccountKey,
                    this.client.ContainerName,
                    this.path);
            }

            long size = this.Size;
            int bufferSize = 1024*1024;
            if (size >= 0)
            {
                bufferSize = (int)(size/10);
                if (bufferSize < 1024*1024)
                    bufferSize = 1024*1024;
                if (bufferSize > 20*1024*1024)
                    bufferSize = 20*1024*1024;
            }
            StreamReader reader = new StreamReader(stream, System.Text.Encoding.UTF8, false, bufferSize);

            if (this.ShouldCacheLocally && this.LocalCachePath != null)
            {
                // cache it 
                if (this.RepresentsAFolder)
                    throw new ClusterException("Cannot cache folders");
                StreamWriter writer = this.CreateTempStream();
                return new SharedStreamReader(reader, writer, this.OnClose);
            }
            else
            {
                // dont cache it
                return new SharedStreamReader(reader);
            }
        }

        // Cache blobs inside a folder; map from name to length
        private Dictionary<string, long> blocks;
        private Dictionary<string, long> pages;

        private void PopulateCache()
        {
            if (this.blocks == null)
            {
                this.blocks = new Dictionary<string, long>();
                this.pages = new Dictionary<string, long>();

                // can happen when we are looking at cached results
                if (this.client == null) return;

                var cloudBlobdir = this.client.Container.GetDirectoryReference(this.path);
                var blobs = cloudBlobdir.ListBlobs();
                foreach (IListBlobItem item in blobs)
                {
                    if (item is CloudBlockBlob)
                    {
                        CloudBlockBlob blob = (CloudBlockBlob)item;
                        blocks.Add(blob.Name, blob.Properties.Length);
                    }
                    else if (item is CloudPageBlob)
                    {
                        CloudPageBlob pageBlob = (CloudPageBlob)item;
                        // not accurate
                        //pages.Add(pageBlob.Name, pageBlob.Properties.Length);
                        pageBlob.FetchAttributes();
                        var metadata = pageBlob.Metadata;
                        if (metadata.ContainsKey("writePosition"))
                        {
                            long sz;
                            if (long.TryParse(metadata["writePosition"], out sz))
                                pages.Add(pageBlob.Name, sz);
                        }
                    }
                    else if (item is CloudBlobDirectory)
                    {
                        //CloudBlobDirectory directory = (CloudBlobDirectory)item;
                    }
                }
            }
        }

        /// <summary>
        /// If the current object is a folder, it returns the contained objects.
        /// </summary>
        /// <returns>An iterator over all contained objects that match the specified string.</returns>
        /// <param name="match">A shell expression (similar to the argument of Directory.GetFiles()).</param>
        public override IEnumerable<IClusterResidentObject> GetFilesAndFolders(string match)
        {
            this.PopulateCache();
            long length = -1;

            foreach (var child in this.client.EnumerateDirectory(this.path))
            {
                Regex re = Utilities.RegexFromSearchPattern(match);
                if (!re.IsMatch(child)) continue;
                
                bool isFolder = false;
                bool isDfsStream = false;

                if (blocks.ContainsKey(child))
                {
                    isDfsStream = true;
                    length = blocks[child];
                }
                else if (pages.ContainsKey(child))
                {
                    isDfsStream = false;
                    length = pages[child];
                }
                else if (this.client != null)
                    // otherwise this information may be incorrect
                    isFolder = true;

                var file = new AzureDfsFile(this.Config, this.Job, this.client, child, this.ShouldCacheLocally, isFolder);
                file.IsDfsStream = isDfsStream;
                file.size = length;
                
                yield return file;
            }
        }

        private long size;
        /// <summary>
        /// Size of the object in bytes (if not a folder).  The size can be -1 when it is unknown.
        /// </summary>
        public override long Size
        {
            get { return this.size; }
        }

       /// <summary>
        /// Short name of the object.
        /// </summary>
        public override string Name
        {
            get { return Path.GetFileName(this.path); }
        }

        /// <summary>
        /// Date when object was created.
        /// </summary>
        public override DateTime CreationTime
        {
            get { return DateTime.Now; }
        }

        /// <summary>
        /// For a folder object, returns the contained file with the specified name.
        /// </summary>
        /// <param name="filename">File name within the folder.</param>
        /// <returns>The file within the folder.</returns>
        public override IClusterResidentObject GetFile(string filename)
        {
            this.PopulateCache();

            string filepath;
            if (this.client != null)
                filepath = this.client.Combine(this.path, filename);
            else
                filepath = Path.Combine(this.path, filename);
            bool isFolder = false;
            bool isDfsStream = false;
            long sz = -1;

            if (blocks.ContainsKey(filepath))
            {
                isDfsStream = true;
                sz = blocks[filepath];
            }
            else if (pages.ContainsKey(filepath))
            {
                isDfsStream = false;
                sz = pages[filepath];
            }
            else if (this.client != null)
                // if the client is null the information may be incorrect
                isFolder = true;

            var file = new AzureDfsFile(this.Config, this.Job, this.client, filepath, this.ShouldCacheLocally, isFolder);
            file.IsDfsStream = isDfsStream;
            file.size = sz;
            return file;
        }

        /// <summary>
        /// For a folder object, returns the contained folder with the specified name.
        /// </summary>
        /// <param name="foldername">Folder name within the folder.</param>
        /// <returns>The subfolder within the folder.</returns>
        public override IClusterResidentObject GetFolder(string foldername)
        {
            this.PopulateCache();

            var file = this.GetFile(foldername);
            if (! file.RepresentsAFolder) throw new InvalidOperationException(foldername + " is not a folder");
            return file;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString()
        {
            return this.path;
        }
    }
}
