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

import java.io.IOException;

public class ListTest {
	
	public static void test (Configuration hConf) throws IOException
	{
		putTest(hConf);
		getTest(hConf);
		delTest(hConf);
	}
	
	
	
	public static void putTest (Configuration hConf) throws IOException
	{
		  byte[] value = Bytes.toBytes("this is a test code for the securebase performance. - Dishant Ai");
		  long[] record_normal = new long[10];
		  long[] record_securebase = new long[10];
		
		  for(int size = 0; size <10; size++)
		  {
			  HBaseAdmin admin = new HBaseAdmin(hConf);
			  String tablename = "testTable" + size;
			  if(admin.tableExists(tablename))
			  {
			  	admin.disableTable(tablename);
			      admin.deleteTable(tablename);
			  }
			  
			  HTableDescriptor desc = new HTableDescriptor(tablename);
			  String colfams[] = {"colfam0","colfam1","colfam2","colfam3"};
			  
			  for (String cf : colfams)
			  {
			    HColumnDescriptor coldef = new HColumnDescriptor(cf);
			    desc.addFamily(coldef);
			  }
			  admin.createTable(desc);
		
			  HTable table2 = new HTable(hConf,tablename);
			  
		
			  List<Put> putList = new ArrayList<Put>();
			  long 	start =  System.currentTimeMillis();
		
			  for(int i=0; i<10; i++)
			  {
			  	  String row = "row" + i;
			  	  Put put1 = new Put(Bytes.toBytes(row));
			  	  for(int j=0; j<4; j++ )
			  	  {
			  		  String colfam = "colfam"+j;
			  		  
			  		  for(int k=0; k<10; k++)
			  		  {
			  			  String qual = "qual" + k;
			  			  
			  			  put1.add(Bytes.toBytes(colfam),Bytes.toBytes(qual),value);
			  				  
			  			 
			  		  }
			  	  }
			  	 
			  		  putList.add(put1);
			  }
			  table2.put(putList);
		  
			  long end = System.currentTimeMillis();
			  record_normal[size]=(end-start);
		 
		  
		  /* SecureTable */
			  String secureTableName = "secureTestTable" + size;
			  
		
			  SecureTable securetable = SecureTable.getSecureTable(hConf);
			  securetable.deleteTable(secureTableName);
			   
			  securetable.createTable(secureTableName, "colfam0","colfam1","colfam2","colfam3");
			  HTable table = new HTable(hConf, secureTableName); // co PutExample-2-NewTable Instantiate a new client.
			  putList.clear();
			 
			 start =  System.currentTimeMillis();
	
			  for(int i=0; i<10; i++)
			  {
				  String row = "row" + i;
				  Put put1 = new Put(Bytes.toBytes(row));
				  for(int j=0; j<4; j++ )
				  {
					  String colfam = "colfam"+j;
					  
					  for(int k=0; k<10; k++)
					  {
						  String qual = "qual" + k;
						  
						  put1.add(Bytes.toBytes(colfam),Bytes.toBytes(qual), value);
						  
						  
					  }
				  }
				  putList.add(put1);
				  
			  }
			table.put(putList);
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			value = Bytes.add(value,value);
		  }
		  
		  HBaseTest.writeResult("List<Put>",record_normal,record_securebase); 
	}
	
	
	public static void getTest (Configuration hConf) throws IOException
	{
		  long[] record_normal = new long[10];
		  long[] record_securebase = new long[10];
		
		  for(int size = 0; size <10; size++)
		  {
			  HBaseAdmin admin = new HBaseAdmin(hConf);
			  
			  String tablename = "testTable" + size;
			  
			  if(!admin.tableExists(tablename))
			  {
			  	PutTest.test(hConf);
			  }
		
			  HTable table2 = new HTable(hConf,tablename);
			  
			  List<Get> getList = new ArrayList<Get>();
			  long 	start =  System.currentTimeMillis();
		
			  for(int i=0; i<10; i++)
			  {
			  	  String row = "row" + i;
			  	  Get get1 = new Get(Bytes.toBytes(row));
			  	  for(int j=0; j<4; j++ )
			  	  {
			  		  String colfam = "colfam"+j;
			  		  
			  		  for(int k=0; k<10; k++)
			  		  {
			  			  String qual = "qual" + k;  
			  			  get1.addColumn(Bytes.toBytes(colfam),Bytes.toBytes(qual));
			  				  
			  			 
			  		  }
			  	  }
			  	 
			  		 getList.add(get1);
			  	 
			  }
			  
			  table2.get(getList);
		  
			  long end = System.currentTimeMillis();
			  record_normal[size]=(end-start);
		 
		  
		  /* SecureTable */
			  String secureTableName = "secureTestTable" + size;
			   
			 
			  HTable table = new HTable(hConf, secureTableName); // co PutExample-2-NewTable Instantiate a new client.
			  
			 getList.clear();
			 start =  System.currentTimeMillis();
	
			  for(int i=0; i<10; i++)
			  {
				  String row = "row" + i;
				  Get get1 = new Get(Bytes.toBytes(row));
				  for(int j=0; j<4; j++ )
				  {
					  String colfam = "colfam"+j;
					  
					  for(int k=0; k<10; k++)
					  {
						  String qual = "qual" + k;
						  get1.addColumn(Bytes.toBytes(colfam),Bytes.toBytes(qual));
						  
						  
					  }
				  }
				  
					  getList.add(get1);
				  
			  }
			  table.get(getList);
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			
		  }
		  
		  HBaseTest.writeResult("List<Get>",record_normal,record_securebase); 

			  
	}
	
	public static void delTest (Configuration hConf) throws IOException
	{
		  long[] record_normal = new long[10];
		  long[] record_securebase = new long[10];
		
		  for(int size = 0; size <10; size++)
		  {
			  HBaseAdmin admin = new HBaseAdmin(hConf);
			  
			  String tablename = "testTable" + size;
			  
			  if(!admin.tableExists(tablename))
			  {
			  	PutTest.test(hConf);
			  }
		
			  HTable table2 = new HTable(hConf,tablename);
			  
			  List<Delete> delList = new ArrayList<Delete>();
			  long 	start =  System.currentTimeMillis();
		
			  for(int i=0; i<10; i++)
			  {
			  	  String row = "row" + i;
			  	  Delete del1 = new Delete(Bytes.toBytes(row));
			  	  for(int j=0; j<4; j++ )
			  	  {
			  		  String colfam = "colfam"+j;
			  		  
			  		  for(int k=0; k<10; k++)
			  		  {
			  			  String qual = "qual" + k;  
			  			  del1.deleteColumns(Bytes.toBytes(colfam),Bytes.toBytes(qual));
			  				  
			  			 
			  		  }
			  	  }
			  	 
			  	delList.add(del1);
			  }
			  table2.delete(delList);
		  
			  long end = System.currentTimeMillis();
			  record_normal[size]=(end-start);
		 
		  
		  /* SecureTable */
			  String secureTableName = "secureTestTable" + size;
			   
			 
			  HTable table = new HTable(hConf, secureTableName); // co PutExample-2-NewTable Instantiate a new client.
			  
			 delList.clear();
			 start =  System.currentTimeMillis();
	
			  for(int i=0; i<10; i++)
			  {
				  String row = "row" + i;
				  Delete del1 = new Delete(Bytes.toBytes(row));
				  for(int j=0; j<4; j++ )
				  {
					  String colfam = "colfam"+j;
					  
					  for(int k=0; k<10; k++)
					  {
						  String qual = "qual" + k;
						  del1.deleteColumns(Bytes.toBytes(colfam),Bytes.toBytes(qual));
						  
						  
					  }
				  }
				  
				  delList.add(del1);
				  
			  }
			  table.delete(delList);
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			
		  }
		  
		  HBaseTest.writeResult("List<Del>",record_normal,record_securebase); 

			  
	}
	

}
