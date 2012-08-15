import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class CheckAndPutTest 
{
	public static void test (Configuration hConf) throws IOException
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
			  
			  Put put1;
			  long 	start =  System.currentTimeMillis();
		
			  for(int i=0; i<10; i++)
			  {
			  	  String row = "row" + i;
			  	  put1 = new Put(Bytes.toBytes(row));
			  	  for(int j=0; j<4; j++ )
			  	  {
			  		  String colfam = "colfam"+j;
			  		  
			  		  for(int k=0; k<10; k++)
			  		  {
			  			  String qual = "qual" + k;
			  			  
			  			  put1.add(Bytes.toBytes(colfam),Bytes.toBytes(qual),value);
			  		  }
			  	  }
			  	 
			  	  if(i<5)
			  	  table2.checkAndPut(Bytes.toBytes(row), Bytes.toBytes("colfam0"),Bytes.toBytes("qual0"), value, put1);
			  	  else
			  	  {
				  table2.checkAndPut(Bytes.toBytes(row), Bytes.toBytes("colfam0"),Bytes.toBytes("qual0"), null, put1);
			  	  }
			  }
		  
			  long end = System.currentTimeMillis();
			  record_normal[size]=(end-start);
			  
			  String secureTableName = "secureTestTable" + size;
			  HTable table = new HTable(hConf, secureTableName); // co PutExample-2-NewTable Instantiate a new client.
			  
				 
				 start =  System.currentTimeMillis();
		
				  for(int i=0; i<10; i++)
				  {
					  String row = "row" + i;
					  put1 = new Put(Bytes.toBytes(row));
					  for(int j=0; j<4; j++ )
					  {
						  String colfam = "colfam"+j;
						  
						  for(int k=0; k<10; k++)
						  {
							  String qual = "qual" + k;
							  
							  put1.add(Bytes.toBytes(colfam),Bytes.toBytes(qual), value);
							  
							  
						  }
					  }
					  if(i<5)
				  	  table.checkAndPut(Bytes.toBytes(row), Bytes.toBytes("colfam0"),Bytes.toBytes("qual0"), value, put1);
				  	  else
				  	  {
					  table.checkAndPut(Bytes.toBytes(row), Bytes.toBytes("colfam0"),Bytes.toBytes("qual0"), null, put1);
				  	  }
					  
				  }
				end = System.currentTimeMillis();
		
				record_securebase[size] = end-start;			
				value = Bytes.add(value,value);
			  }
			  
			  HBaseTest.writeResult("CheckAndPut",record_normal,record_securebase); 

	}
}
