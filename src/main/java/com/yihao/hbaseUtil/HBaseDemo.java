package com.yihao.hbaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;




public class HBaseDemo {
	//创建Hadoop以及HBased管理配置对象
	public static Configuration conf;
	
	static{
		//使用HBaseConfiguration的单例方法实例化
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","192.168.182.128");
		conf.set("hbase.zookeeper.property.clientPort","2181");

	}






	
	/**
	 * 判断表是否已存在
	 *
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 */
	public static boolean isTableExist(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//在HBase中管理、访问表需要先创建HBaseAdmin对象
		HBaseAdmin admin = new HBaseAdmin(conf);
		return admin.tableExists(tableName);
	}
	
	/**
	 * 创建表
	 *
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 */
	public static void createTable(String tableName, String... columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		//判断表是否存在
		if(isTableExist(tableName)){
			System.out.println("表" + tableName + "已存在");
			//System.exit(0);
		}else{
			//创建表属性对象,表名需要转字节
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			
			//创建多个列族
			for(String cf : columnFamily){
				descriptor.addFamily(new HColumnDescriptor(cf));
			}
			
			//根据对表的配置，创建表
			admin.createTable(descriptor);
			System.out.println("表" + tableName + "创建成功！");
		}
	}
	
	/**
	 * 删除表
	 *
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 */
	public static void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(isTableExist(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("表" + tableName + "删除成功！");
		}else{
			System.out.println("表" + tableName + "不存在！");
		}
		
	}
	
	/**
	 * 向表中插入数据
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param column
	 * @param value
	 * @throws IOException 
	 */
	public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException{
		//创建HTable对象
		HTable hTable = new HTable(conf, tableName);
		//向表中插入数据
		Put put = new Put(Bytes.toBytes(rowKey));
		//向Put对象中组装数据
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		hTable.put(put);
		hTable.close();
		System.out.println("插入数据成功");
	}
	
	/**
	 * 删除多行数据
	 * @param tableName
	 * @param rows
	 * @throws IOException 
	 */
	public static void deleteMultiRow(String tableName, String... rows) throws IOException{
		HTable hTable = new HTable(conf, tableName);
		List<Delete> deleteList = new ArrayList<Delete>();
		for(String row : rows){
			Delete delete = new Delete(Bytes.toBytes(row));
			deleteList.add(delete);
		}
		hTable.delete(deleteList);
		hTable.close();
	}
	
	/**
	 * 得到所有的数据
	 * @param tableName
	 * @throws IOException 
	 */
	public static void getAllRows(String tableName) throws IOException{
		HTable hTable = new HTable(conf, tableName);
		//得到用于扫描region的对象
		Scan scan = new Scan();
		//使用HTable得到resultcanner实现类的对象
		ResultScanner resultScanner = hTable.getScanner(scan);
		for(Result result : resultScanner){
			Cell[] cells = result.rawCells();
			for(Cell cell : cells){
				//得到rowkey
				System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
				//得到列族
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		
	}
	
	public static void main(String[] args) {
			try {
				//System.out.println(isTableExist("student"));
				//createTable("person", "basic_info", "job", "heathy");
//				dropTable("person");
				//addRowData("person", "1001", "basic_info", "name", "Nick");
				addRowData("person", "1001", "basic_info", "sex", "Male");
//				addRowData("person", "1001", "basic_info", "age", "18");
//				addRowData("person", "1001", "job", "dept_no", "7981");
				
//				deleteMultiRow("person", "person");
				//getAllRows("person");
				
				
			} catch (Exception e) {
				e.printStackTrace();
			}
	}

}
