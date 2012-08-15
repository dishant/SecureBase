import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.*;


public class ValueFilterTest 
{
	/**
	 * @param hConf
	 * @throws IOException
	 */
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
			  
			  Get get1;
			  long 	start =  System.currentTimeMillis();
		
			  for(int i=0; i<10; i++)
			  {
			  	  String row = "row" + i;
			  	  get1 = new Get(Bytes.toBytes(row));
			  	  get1.addFamily(Bytes.toBytes("colfam1"));
			  	  Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("securebase"));
			  	  get1.setFilter(filter1);
			  	  table2.get(get1);
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
			  	  get1 = new Get(Bytes.toBytes(row));
			  	  get1.addFamily(Bytes.toBytes("colfam1"));
			  	  Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("securebase"));
			  	  get1.setFilter(filter1);
			  	  table.get(get1);
			  }
			end = System.currentTimeMillis();
	
			record_securebase[size] = end-start;			
			
		  }
		  
		  HBaseTest.writeResult("ValueFilter:EQUAL_SUBSTRING",record_normal,record_securebase); 

			  
	}
}
