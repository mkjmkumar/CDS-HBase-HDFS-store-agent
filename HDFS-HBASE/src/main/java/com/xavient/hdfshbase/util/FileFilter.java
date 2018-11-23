package com.xavient.hdfshbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileUtil;
import com.xavient.hdfshbase.exception.HdfsHbaseException;
import com.xavient.hdfshbase.constants.Constants;
import com.xavient.hdfshbase.exception.HdfsHbaseException;

public class FileFilter {
	// Configuration config;
	private static org.apache.log4j.Logger log = Logger.getLogger(FileFilter.class);

	/**
	 * This method basically verified the file format (filter file for further
	 * processing
	 * 
	 * @param Take
	 *            input File HDFS path
	 * @return true/false
	 */
	public boolean filefilter(String filePath) {
		String ext = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());

		for (int i = 0; i < Constants.fileList.length; i++) {

			if (Constants.fileList[i].equalsIgnoreCase(ext))
				return true;
		}

		return false;

	}

	/**
	 * This method is used to return the object of FileStatus *
	 * 
	 * @param Take
	 *            input as Configuration object
	 * @return fileStatus
	 */
	public FileStatus configurationhdfs(Configuration conf) throws HdfsHbaseException {
		// FileFilter obj = new FileFilter();
		try {
			FileSystem fs = FileSystem.get(conf);
			FileStatus fileStatus = fs.getFileStatus(new Path(Constants.Source_Path));
			log.info("File Status Sucess");

			return fileStatus;
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}

	}

	/**
	 * This method is used to get the file size(in bytes) of the input file in
	 * HDFS
	 * 
	 * @param Take
	 *            input as object of filestatus and checksum of the input file
	 * @return file size
	 */
	public Long getFileSize(FileStatus fileStatus, FileChecksum sourceChecksum) {
		Long size = fileStatus.getLen();

		// System.out.println(size);
		log.info("File Size" + size);

		return size;
	}

	/**
	 * This method is used to get the checksum of the input file in HDFS
	 * 
	 * @param Take
	 *            input as input source file path and Configuration object
	 * @return checksum
	 */
	public FileChecksum getFileCheckSum(Path sourceFilePath, Configuration conf) throws HdfsHbaseException {
		try {
			FileSystem fs = FileSystem.get(conf);
			FileChecksum sourceChecksum = null;
			sourceChecksum = fs.getFileChecksum(sourceFilePath);
			// System.out.println("=========================?>>>>" +
			// sourceChecksum.toString());
			log.info("Checksum of Source File" + sourceChecksum.toString());
			return sourceChecksum;
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}
	}

	public static void main(String args[]) throws HdfsHbaseException {
		Configuration conf = HBaseConfigUtil.getHBaseConfiguration();
		FileFilter obj = new FileFilter();
		FileStatus fileStatus = obj.configurationhdfs(conf);
		FileChecksum sourceChecksum = obj.getFileCheckSum(fileStatus.getPath(), conf);
		CreateMOBTable createTable = new CreateMOBTable();
		WriteIntoHBase writeintohbase = new WriteIntoHBase();
		log.info("All Required objectes Created Successfully");

		boolean found = obj.filefilter(fileStatus.getPath().toString());
		if (found) {
			// System.out.println("File found on HDFS");
			createTable.createmobTable(conf);
			Long size = obj.getFileSize(fileStatus, sourceChecksum);
			if (size < 1048576) {
				writeintohbase.insertIntoHbaseSizeLessThan10MB(sourceChecksum.toString(),
						fileStatus.getPath().toString(), size, conf);
			} else {
				// Configuration conf = HBaseConfigUtil.getHBaseConfiguration();
				FileSystem fs;
				try {
					fs = FileSystem.get(conf);
					Path desPath = new Path(Constants.Des_Path);
					FileUtil.copy(fs, fileStatus.getPath(), fs, desPath, false, conf);
					writeintohbase.insertIntoHbaseSizeGreaterThan10MB(sourceChecksum.toString(), desPath.toString(),
							size, conf);
				} catch (IOException e) {

					throw new HdfsHbaseException(e.getMessage());
				}

			}
		} else {
			log.info("Particular file format is not suitable for processing");
			throw new HdfsHbaseException("Please enter suitable format file");
		}
	}
}
