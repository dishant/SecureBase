// cc PutExample Example application inserting data into HBase
// vv PutExample
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.KeyValue;



import javax.crypto.KeyGenerator;
import javax.crypto.Cipher;
import javax.crypto.spec.*;
import java.security.*;
import java.util.*;
// ^^ PutExample
import SecureBase.*;
// vv PutExample

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class HBaseTest{
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	  
	
	public static void writeResult(String msg, long[] normal, long[] encrypted) throws IOException
	{
		File file = new File("testResult.txt");
		if(!file.exists())
		{
		file.createNewFile();
		}
		
		FileWriter fileWriter = new FileWriter(file,true);
	    BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
		
    	bufferWriter.write(msg);
		bufferWriter.newLine();
		bufferWriter.write("Size\t\tNormal\tEncrypted");
		bufferWriter.newLine();
		for(int i=1; i < normal.length; i++)
		{
			bufferWriter.write(Double.toString(64*java.lang.Math.pow(4,i)));
			bufferWriter.write("\t\t" + normal[i] + "\t" + encrypted[i]);
			bufferWriter.newLine();
		}
	    
	    bufferWriter.close();
	    fileWriter.close();
	}
	

  public static void main(String[] args) throws IOException, InterruptedException  {
	  	  
	  //String hbaseZookeeperQuorum="REPLACE_BY_HBASE_SERVER_IP_ADR";
	  //String hbaseZookeeperClientPort="2181";
	  
	  Configuration conf2 = HBaseConfiguration.create();
	  Configuration hConf = HBaseConfiguration.create(conf2);
	  //hConf.set("hbase.master","REPLACE_BY_HBASE_SERVER_IP_ADR");
	  //hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
	  //hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);
	  
	  
	  /* Maintain the order of tests */
	  PutTest.test(hConf);
	  GetTest.test(hConf);
	  ScanTest.test(hConf);
	  ValueFilterTest.test(hConf);
	  CheckAndPutTest.test(hConf);
	  CheckAndDeleteTest.test(hConf);
	  DelTest.test(hConf);
	  ListTest.test(hConf);
	  BatchTest.test(hConf);

  }
 
  }
