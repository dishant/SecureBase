import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;


public class DelTest {
	public static void test (Configuration hConf) throws IOException
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
			  
			  Delete del1;
			  long 	start =  System.currentTimeMillis();
		
			  for(int i=0; i<10; i++)
			  {
			  	  String row = "row" + i;
			  	  del1 = new Delete(Bytes.toBytes(row));
			  	  for(int j=0; j<4; j++ )
			  	  {
			  		  String colfam = "colfam"+j;
			  		  
			  		  for(int k=0; k<10; k++)
			  		  {
			  			  String qual = "qual" + k;  
			  			  del1.deleteColumns(Bytes.toBytes(colfam),Bytes.toBytes(qual));
			  				  
			  			 
			  		  }
			  	  }
			  	 
			  		  table2.delete(del1);
			  }
		  
			  long end = System.currentTimeMillis();
			  record_normal[size]=(end-start);
		 
		  
		  /* SecureTable */
			  String secureTableName = "secureTestTable" + size;
			   
			 
			  HTable table = new HTable(hConf, secureTableName); // co PutExample-2-NewTable Instantiate a new client.
			  
			 
			 start =  System.currentTimeMillis();
	
			  for(int i=0; i<10; i++)
			  {
				  String row = "row" + i;
				  del1 = new Delete(Bytes.toBytes(row));
				  for(int j=0; j<4; j++ )
				  {
					  String colfam = "colfam"+j;
					  
					  for(int k=0; k<10; k++)
					  {
						  String qual = "qual" + k;
						  del1.deleteColumns(Bytes.toBytes(colfam),Bytes.toBytes(qual));
						  
						  
					  }
				  }
				  
				  table.delete(del1);
				  
			  }
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			
		  }
		  
		  HBaseTest.writeResult("Del",record_normal,record_securebase); 

			  
	}

}
