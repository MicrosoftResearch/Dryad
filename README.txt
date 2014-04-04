Dryad

This is a research prototype of the Dryad and DryadLINQ data-parallel 
processing frameworks running on Hadoop YARN.  Dryad utilizes cluster 
services provided as part of Hadoop YARN to reliably execute 
distributed computations on a cluster of computers. DryadLINQ provides
the LINQ programming model for distributed data processing and leverages 
Dryad for reliable execution.

Dryad and DryadLINQ on YARN are still under active development.  
As a result, you should expect some fragility.  

Requirements

A version of YARN built for Windows 
	The BUILDING.txt file in the Hadoop YARN repository contains 
	instructions on building YARN for Windows.
Visual Studio 2012
Java Development Kit 1.6
A Windows YARN cluster composed of x64 machines

Building Dryad

1) Clone the Dryad git repository.
2) Ensure that YARN_HOME environment variable is set.
3) Set the DRYAD_HOME environment variable to binary path 
   (bin\Debug or bin\Release) under the directory Dryad was cloned to.  
4) Use Visual Studio to open The Dryad solution file (Dryad.sln) located 
   in the root of the repository and build the solution.
5) Run Build.bat in the Java directory at the top-level of the repository.  
   The CLASSPATH will need to be set to the output of the 'yarn classpath'
   command.  

Cluster setup
1) Setup your YARN cluster as you normally would.
2) Copy the contents of the DRYAD_HOME directory to the location set by 
   DRYAD_HOME on each compute node in the cluster. 

Notes

The YARN interfaces used are current as of commit dfb83b8 in trunk.

If you are running debug builds of the Dryad, also copy the files msvcp110d.dll
and msvcr110d.dll to the DRYAD_HOME directory on each compute node.  The 
article at http://msdn.microsoft.com/en-us/library/vstudio/aa985618.aspx
describes how to do this.

The HDFS implementation in Dryad currently only supports text files.      
