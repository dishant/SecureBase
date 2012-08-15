import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class ScanTest {

	public static void test(Configuration hConf) throws IOException
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
			  
			  Scan s;
			  long 	start =  System.currentTimeMillis();
		
	
			  	  s = new Scan();
			  	  ResultScanner scanner = table2.getScanner(s);
			  		for(Result result: scanner);
	
		  
			  long end = System.currentTimeMillis();
			  record_normal[size]=(end-start);
		 
		  
		  /* SecureTable */
			  String secureTableName = "secureTestTable" + size;
			   
			 
			  HTable table = new HTable(hConf, secureTableName); // co PutExample-2-NewTable Instantiate a new client.
			  
			 
			 start =  System.currentTimeMillis();
		  	  s = new Scan();
		  	  ResultScanner scanner2 = table.getScanner(s);
		  		for(Result result: scanner2);
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			
		  }
		  
		  HBaseTest.writeResult("Scan",record_normal,record_securebase); 

	}
}
