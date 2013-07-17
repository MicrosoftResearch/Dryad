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


package GSLHDFS;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

//--------------------------------------------------------------------------------

public class HdfsBridge 
{
	//--------------------------------------------------------------------------------

	public static void main(String[] args) throws IOException 
	{
		Instance i = new Instance();
		int ret = i.Connect("svc-d1-17",9000);
		if (ret != SUCCESS)
		{
			System.out.println("Failed to connect");
			return;
		}

		String fileName = "/data/inputPart0.txt";
		
		FileStatus fs = i.OpenFileStatus(fileName, false);
		System.out.println(fs.getPath().toUri().getPath());

		int rc = i.IsFileExist(fileName);
		System.out.println(rc);

		if (rc == 1)
		{
			Instance.Reader r = i.OpenReader(fileName);

			if (r != null)
			{
				int nRead = 0;
				long offset = 0;
				do
				{
					Instance.Reader.Block b = r.ReadBlock(offset, 64 * 1024);
					nRead = b.ret;
					if (nRead != -1)
					{
						System.out.println("Read " + nRead + " bytes at " + offset);
						offset += nRead;
					}
				} while (nRead >= 0);

				ret = r.Close();
				if (ret != SUCCESS)
				{
					System.out.println("Failed to close");
				}
			}
		}

		ret = i.Disconnect();
		if (ret != SUCCESS)
		{
			System.out.println("Failed to disconnect");
		}
		//String fileToRead = "/data/tpch/customer_1G_128MB.txt";
		//String content = HdfsBridge.ReadBlock(fileToRead, 0);
		//System.out.println(rc);
	}

	//--------------------------------------------------------------------------------

	public static int SUCCESS = 1;
	public static int FAILURE = 0;

	//--------------------------------------------------------------------------------


	//--------------------------------------------------------------------------------

	public static class Instance
	{
		private DistributedFileSystem dfs = null;
		public String exceptionMessage = null;

		//--------------------------------------------------------------------------------

		public static class Reader
		{			
			public static class Block
			{
				public int     ret;
				public byte[]  buffer;
			}

			private FSDataInputStream dis = null;
			public String exceptionMessage = null;

			public void Open(DistributedFileSystem dfs, String fileName) throws IOException
			{
				Path path = new Path(fileName);
				dis = dfs.open(path);
			}

			public Block ReadBlock(long blockOffset, int bytesRequested)
			{
				Block block = new Block();
				block.buffer = null;

				if (dis == null)
				{
					exceptionMessage = "ReadBlock called on closed reader";
					block.ret = -2;
					return block;
				}

				block.buffer = new byte[bytesRequested];

				int numBytesRead = -2;
				try
				{
					numBytesRead = dis.read(blockOffset, block.buffer, 0, bytesRequested); 
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					block.buffer = null;
					block.ret = -2;
					return block;
				}

				block.ret = numBytesRead;
				return block; 
			}

			public int Close()
			{
				int ret = SUCCESS;

				if (dis != null)
				{
					try {
						dis.close();
					}
					catch (IOException e1)
					{
						exceptionMessage = e1.getMessage();
						ret = FAILURE;
					}

					dis = null;
				}

				return ret;
			}
		}

		public static class Writer
		{
			private FSDataOutputStream dos = null;
			public String exceptionMessage = null;

			public void Open(DistributedFileSystem dfs, String fileName) throws IOException
			{
				Path path = new Path(fileName);
				dos = dfs.create(path);
			}

			public int WriteBlock(byte[] buffer, boolean flushAfter)
			{
				if (dos == null)
				{
					exceptionMessage = "WriteBlock called on closed writer";
					return FAILURE;
				}

				try
				{
					dos.write(buffer);
					if (flushAfter)
					{
						dos.flush();
					}
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					return FAILURE;
				}

				return SUCCESS; 
			}

			public int Close()
			{
				int ret = SUCCESS;

				if (dos != null)
				{
					try
					{
						dos.close();
					}
					catch (IOException e1)
					{
						exceptionMessage = e1.getMessage();
						ret = FAILURE;
					}

					dos = null;
				}

				return ret;
			}
		}

		public static class BlockLocations
		{
			private BlockLocation[] bls = null;
			private int[] fileIndex = null;
			private String[] fileName = null;
			public String exceptionMessage = null;
			public long fileSize = -1;

			BlockLocations(
					BlockLocation[] b,
					int[] fIndex,
					String[] fName,
					long fSize)
			{
				bls = b;
				fileIndex = fIndex;
				fileName = fName;
				fileSize = fSize;
			}
			
			public int GetNumberOfFileNames()
			{
				return fileName.length;
			}
			
			public String[] GetFileNames()
			{
				return fileName;
			}

			public int GetNumberOfBlocks()
			{
				return bls.length;
			}

			public long GetBlockOffset(int blockId)
			{
				return bls[blockId].getOffset();
			}

			public long GetBlockLength(int blockId)
			{
				return bls[blockId].getLength();
			}

			public String[] GetBlockHosts(int blockId)
			{
				BlockLocation bl = bls[blockId];
				String[] hosts = null;

				exceptionMessage = null;
				try
				{
					hosts = bl.getHosts();
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					return null;
				}
				return hosts;
			}

			public String[] GetBlockNames(int blockId)
			{
				BlockLocation bl = bls[blockId]; 
				String[] names = null;

				exceptionMessage = null;
				try
				{
					names = bl.getNames();
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					return null;
				}
				return names;
			}
			
			public int GetBlockFileId(int blockId)
			{
				return fileIndex[blockId];
			}
		}

		public int Connect(String inputNameNode, long inputPortNumber)
		{                 
			Configuration config = new Configuration();  
			config.set("fs.defaultFS", "hdfs://" +
					inputNameNode + ":"+ inputPortNumber +"");

			exceptionMessage = null;

			try                         
			{                   
				dfs = (DistributedFileSystem)FileSystem.get(config);
			}                   
			catch (IOException e1)                      
			{
				exceptionMessage = e1.getMessage();
				return FAILURE;
			}

			return SUCCESS;
		}

		//--------------------------------------------------------------------------------

		public int Disconnect()
		{
			int ret = SUCCESS;

			if (dfs != null)
			{
				exceptionMessage = null;

				try
				{
					dfs.close();
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					ret = FAILURE;
				}

				dfs = null;
			}

			return ret;
		}

		//--------------------------------------------------------------------------------

		public int IsFileExist(String fileName)
		{               
			if (dfs == null)
			{
				exceptionMessage = "IsFileExist called on disconnected instance";
				return -1;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(fileName);
				return (dfs.exists(path)) ? 1 : 0;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return -1;
			}
		}

		public int DeleteFile(String fileName, boolean recursive)
		{               
			if (dfs == null)
			{
				exceptionMessage = "DeleteFile called on disconnected instance";
				return -1;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(fileName);
				return (dfs.delete(path, recursive)) ? 1 : 0;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return -1;
			}
		}

		public int RenameFile(String dstFileName, String srcFileName)
		{               
			if (dfs == null)
			{
				exceptionMessage = "RenameFile called on disconnected instance";
				return -1;
			}

			exceptionMessage = null;

			try
			{
				Path dstPath = new Path(dstFileName);
				Path srcPath = new Path(srcFileName);
				return (dfs.rename(srcPath, dstPath)) ? 1 : 0;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return -1;
			}
		}

		public Reader OpenReader(String fileName)
		{
			if (dfs == null)
			{
				System.out.println("OpenReader called on disconnected instance\n");
				return null;
			}

			Reader r = new Reader();

			exceptionMessage = null;

			try
			{
				r.Open(dfs, fileName);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}

			return r;
		}

		public Writer OpenWriter(String fileName)
		{
			if (dfs == null)
			{
				System.out.println("OpenWriter called on disconnected instance\n");
				return null;
			}

			Writer w = new Writer();

			exceptionMessage = null;

			try
			{
				w.Open(dfs, fileName);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}

			return w;
		}

		public FileStatus OpenFileStatus(String fileOrDirectoryName, boolean getLocations)
		{
			if (dfs == null)
			{
				exceptionMessage = "OpenFileStatus called on disconnected instance";
				return null;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(fileOrDirectoryName);
				
				return dfs.getFileStatus(path);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}
		}

		public BlockLocations OpenBlockLocations(FileStatus fileStatus, boolean getBlocks)
		{
			exceptionMessage = null;
			try
			{
				FileStatus[] expanded;
				if (fileStatus.isDirectory())
				{
					expanded = dfs.listStatus(fileStatus.getPath());
					for (int i=0; i<expanded.length; ++i)
					{
						if (expanded[i].isDirectory())
						{
							exceptionMessage = expanded[i].getPath().toString() + " is a directory: recursive descent not supported";
							return null;
						}
					}
				}
				else
				{
					expanded = new FileStatus[1];
					expanded[0] = fileStatus;
				}

				String[] fileName = new String[expanded.length];
				long totalSize = 0;
				for (int i=0; i<expanded.length; ++i)
				{
					fileName[i] = expanded[i].getPath().toUri().getPath();
				}

				BlockLocation[] bls = null;
				int[] fileIndex = null;

				if (getBlocks)
				{
					BlockLocation[][] nested = new BlockLocation[expanded.length][];
					int totalBlocks = 0;
					for (int i=0; i<expanded.length; ++i)
					{
						long fileLength = expanded[i].getLen();
						totalSize += fileLength;

						nested[i] = dfs.getFileBlockLocations(expanded[i], 0, fileLength);
						totalBlocks += nested[i].length;
					}
				
					bls = new BlockLocation[totalBlocks];
					fileIndex = new int[totalBlocks];
				
					int copiedBlock = 0;
					for (int i=0; i<expanded.length; ++i)
					{
						for (int j=0; j<nested[i].length; ++j, ++copiedBlock)
						{
							fileIndex[copiedBlock] = i;
							bls[copiedBlock] = nested[i][j];
						}
					}
				}

				return new BlockLocations(bls, fileIndex, fileName, totalSize);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}
		}
	}

	public static Instance OpenInstance(String inputNameNode, long inputPortNumber)
	{
		Instance i = new Instance();

		int ret = i.Connect(inputNameNode, inputPortNumber);
		if (ret == SUCCESS)
		{
			return i;
		}
		else
		{
			System.out.println("OpenInstance failed for " + inputNameNode + ":" + inputPortNumber + " -- " + i.exceptionMessage);
			return null;
		}
	}
}
