package com.xavient.hdfshbase.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.xavient.hdfshbase.constants.Constants;
import com.xavient.hdfshbase.exception.HdfsHbaseException;

public class WriteIntoHBase {
	private static org.apache.log4j.Logger log = Logger.getLogger(WriteIntoHBase.class);
	WriteIntoHBase wrintohbase = new WriteIntoHBase();

	/**
	 * This method is used to create Habse connection with the given
	 * configuration parameter
	 * 
	 * @param Take
	 *            input as Configuration object
	 * @return connection object
	 */
	public Connection creatConnection(Configuration config) throws HdfsHbaseException {
		// Configuration config = HBaseConfigUtil.getHBaseConfiguration();
		Connection connection;
		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}
		log.info("Connection with HBASE created Successfully" + connection);
		return connection;
	}

	/**
	 * This method is used to Insert data into Hbase when size of input file is
	 * less than 10MB
	 * 
	 * @param Take
	 *            input Checksum of file, filepath, filesize and Configuration
	 *            Object
	 */
	public void insertIntoHbaseSizeLessThan10MB(String checkSum, String filePath, Long fileSize, Configuration config)
			throws HdfsHbaseException {
		boolean contentType = true;
		// Configuration config = HBaseConfiguration.create();
		// Configuration config = HBaseConfigUtil.getHBaseConfiguration();
		// HTable hTable = new HTable(config, Constants.tableName);
		// Connection connection = ConnectionFactory.createConnection(config);
		Connection connection = wrintohbase.creatConnection(config);

		Table table;
		try {
			table = connection.getTable(TableName.valueOf(Constants.tableName));
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}
		Put p = new Put(Bytes.toBytes("row1"));

		try {
			p.addColumn(Bytes.toBytes(Constants.Row_Checksum_id), Bytes.toBytes(Constants.Row_Checksum_id),
					Bytes.toBytes(checkSum));
			p.addColumn(Bytes.toBytes(Constants.File_Properties), Bytes.toBytes(Constants.File_Size),
					Bytes.toBytes(fileSize));
			p.addColumn(Bytes.toBytes(Constants.File_Properties), Bytes.toBytes(Constants.File_Path),
					Bytes.toBytes(filePath));
			p.addColumn(Bytes.toBytes(Constants.Content_Type), Bytes.toBytes(Constants.Content_Type),
					Bytes.toBytes(contentType));
			
			p.addColumn(Bytes.toBytes(Constants.Mob_Image), Bytes.toBytes(Constants.Mob_Image), extractBytes(filePath));
			table.put(p);
			log.info("Data into Hbase table inserted successfully" + "Checksum:" + checkSum + "FileSize" + fileSize
					+ "FilePath" + filePath + "Content Type" + contentType);
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		} finally {
			try {
				table.close();
				connection.close();
			} catch (IOException e) {
				throw new HdfsHbaseException(e.getMessage());
			}
			log.info("Connection closed");
		}

	}

	/**
	 * This method is used to Extract the input of MOB type
	 * 
	 * @param Take
	 *            input filepath
	 */
	public byte[] extractBytes(String imagePath) throws HdfsHbaseException {
		File imageFile = new File(imagePath);
		BufferedImage image;
		try {
			image = ImageIO.read(imageFile);
		} catch 
		
		(IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try {
			ImageIO.write(image, "jpg", outputStream);
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}
		return outputStream.toByteArray();
	}

	/**
	 * This method is used to Insert data into Hbase when size of input file is
	 * greater than 10MB
	 * 
	 * @param Take
	 *            input Checksum of file, filepath, filesize and Configuration
	 *            Object
	 */
	public void insertIntoHbaseSizeGreaterThan10MB(String checkSum, String filePath, Long fileSize,
			Configuration config) throws HdfsHbaseException {
		boolean contentType = false;
		// Configuration config = HBaseConfigUtil.getHBaseConfiguration();
		// HTable hTable = new HTable(config, Constants.tableName);
		// Connection connection = ConnectionFactory.createConnection(config);
		Connection connection = wrintohbase.creatConnection(config);
		Table table;
		try {
			table = connection.getTable(TableName.valueOf(Constants.tableName));

			// System.out.println(table.getConfiguration().get("fs.defaultFS"));
			Put p = new Put(Bytes.toBytes("row1"));

			p.addColumn(Bytes.toBytes(Constants.Row_Checksum_id), Bytes.toBytes(Constants.Row_Checksum_id),
					Bytes.toBytes(checkSum));
			p.addColumn(Bytes.toBytes(Constants.File_Properties), Bytes.toBytes(Constants.File_Size),
					Bytes.toBytes(fileSize));
			p.addColumn(Bytes.toBytes(Constants.File_Properties), Bytes.toBytes(Constants.File_Path),
					Bytes.toBytes(filePath));
			p.addColumn(Bytes.toBytes(Constants.Content_Type), Bytes.toBytes(Constants.Content_Type),
					Bytes.toBytes(contentType));
			table.put(p);
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}

		log.info("Data into Hbase table inserted successfully" + "Checksum:" + checkSum + "FileSize" + fileSize
				+ "FilePath" + filePath + "Content Type" + contentType);

		try {
			table.close();
			connection.close();
		} catch (IOException e) {
			throw new HdfsHbaseException(e.getMessage());
		}

		log.info("Connection closed");

	}

}
