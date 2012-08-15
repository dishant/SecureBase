import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import SecureBase.SecureTable;

public class BatchTest 
{
	public static void test(Configuration hConf) throws IOException, InterruptedException
	{
		batchPut(hConf);
		batchGet(hConf);
		batchRow(hConf);
	}
	
	public static void batchPut(Configuration hConf) throws IOException, InterruptedException
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
			  
		
			  List<Row> putList = new ArrayList<Row>();
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
			  table2.batch(putList);
		  
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
			table.batch(putList);
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			value = Bytes.add(value,value);
		  }
		  
		  HBaseTest.writeResult("Batch<Put>",record_normal,record_securebase); 
	}
	
	public static void batchGet(Configuration hConf) throws IOException, InterruptedException
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
			  
			  List<Row> getList = new ArrayList<Row>();
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
			  
			  table2.batch(getList);
		  
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
			  table.batch(getList);
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			
		  }
		  
		  HBaseTest.writeResult("Batch<Get>",record_normal,record_securebase); 
	}
	
	public static void batchRow(Configuration hConf) throws IOException, InterruptedException
	{
		byte[] value = Bytes.toBytes("this is a test code for the securebase performance. - Dishant Ai");
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
			  
			  List<Row> rowList = new ArrayList<Row>();
			  long 	start =  System.currentTimeMillis();
		
			  for(int i=0; i<10; i++)
			  {
			  	  String row = "row" + i;
			  	  Get get1 = new Get(Bytes.toBytes(row));
			  	  Put put1 = new Put(Bytes.toBytes(row));
			  	  for(int j=0; j<4; j++ )
			  	  {
			  		  String colfam = "colfam"+j;  
			  		  for(int k=0; k<5; k++)
			  		  {
			  			  String qual = "qual" + k;  
			  			  put1.add(Bytes.toBytes(colfam),Bytes.toBytes(qual),value);
			  		  }
			  		 for(int k=5; k<10; k++)
			  		  {
			  			  String qual = "qual" + k;  
			  			  get1.addColumn(Bytes.toBytes(colfam),Bytes.toBytes(qual));
			  		  }
			  		  
			  	  }
			  		 rowList.add(put1);
			  		 rowList.add(get1);
			  }
			  
			  table2.batch(rowList);
		  
			  long end = System.currentTimeMillis();
			  record_normal[size]=(end-start);
		 
		  
		  /* SecureTable */
			  String secureTableName = "secureTestTable" + size;
			   
			 
			  HTable table = new HTable(hConf, secureTableName); // co PutExample-2-NewTable Instantiate a new client.
			  
			 rowList.clear();
			 start =  System.currentTimeMillis();
	
			  for(int i=0; i<10; i++)
			  {
				  String row = "row" + i;
				  Get get1 = new Get(Bytes.toBytes(row));
			  	  Put put1 = new Put(Bytes.toBytes(row));
				  for(int j=0; j<4; j++ )
				  {
					  String colfam = "colfam"+j;
					  for(int k=0; k<5; k++)
			  		  {
			  			  String qual = "qual" + k;  
			  			  put1.add(Bytes.toBytes(colfam),Bytes.toBytes(qual),value);
			  		  }
			  		 for(int k=5; k<10; k++)
			  		  {
			  			  String qual = "qual" + k;  
			  			  get1.addColumn(Bytes.toBytes(colfam),Bytes.toBytes(qual));
			  		  }
				  }
				  
				  rowList.add(put1);
			  		rowList.add(get1);
				  
			  }
			  table2.batch(rowList);
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;	
			value = Bytes.add(value,value);
		  }
		  
		  HBaseTest.writeResult("Batch<Row>",record_normal,record_securebase); 
	}
	
}
