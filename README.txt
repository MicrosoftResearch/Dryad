Dryad

This is a research prototype of the Dryad and DryadLINQ data-parallel 
processing frameworks running on Hadoop YARN.  Dryad utilizes cluster 
services provided as part of Hadoop YARN to reliably execute 
distributed computations on a cluster of computers. DryadLINQ provides
the LINQ programming model for distributed data processing and leverages 
Dryad for reliable execution.

Dryad and DryadLINQ on YARN are still under active development.  

If you just want to run a sample DryadLINQ program, the simplest way to get started
is to follow the instructions at 
http://microsoftresearchsvc.github.io/Dryad/?topic=html/e992fd94-c956-481d-82e6-dbdf45daa722.htm

Requirements

Visual Studio 2012
An Azure HDInsight 3.0 cluster or a Windows YARN cluster composed of x64 machines

Building Dryad

1) Clone the Dryad git repository.
2) Set the DRYAD_HOME environment variable to binary path
   (bin\Debug or bin\Release) under the directory Dryad was cloned to.
3) Use Visual Studio to open The Dryad solution file (Dryad.sln) located 
   in the root of the repository and build the solution.  The dependencies
   will be fetched from NuGet.


Notes

The HDFS implementation in Dryad currently only supports text files, although Azure Blobs can
be used to store any serialized data type.
