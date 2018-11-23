# CDS-HBase-HDFS-store-agent

CDS HBase-HDFS store agent is an utility to store an object on the basis of its size. If size is greater than 10 MB, It will extract meteadata into Hbase and store the file in HDFS and if size less than 10 MB it will store an object into HBASE.

Code contains following 3 packages

1-com.xavient.hdfshbase.constants:

	This package basically handles the constants used in the project

2-com.xavient.hdfshbase.exception:
	
	This package is used for handling all types of exception in the project.

3-com.xavient.hdfshbase.util:

	Basically It has 4 classes in it
	
	a) CreateMOBTable: It is used to create Hbase table.
	b) FileFilter:  It has many function in it, such as getting file size, file checksum etc and has main function.
	c) HBaseConfigUtil: All HDFS/ HBase configurable properties are present in this class.
	d) WriteIntoHBase: This is class is used to write the data into HBase/ HDFS as per the check.
