package SecureBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


public class SecureTable
{
	private Configuration conf;
	private HBaseAdmin admin;
	
	private SecureTable(Configuration conf)
	throws IOException
	{
		this.conf = conf;
		this.admin = new HBaseAdmin(conf);
	}
	
	public static SecureTable getSecureTable(Configuration conf) throws IOException
	{
		return new SecureTable(conf);
	}
	
	public void createTable(String tableName, String... colfams)
	throws IOException 
	{
		createTable(tableName, null, colfams);
	}
	
	public void createTable(String tableName, byte[][] splitKeys, String... colfams)
	throws IOException 
	{	
		HTableDescriptor desc = new HTableDescriptor(tableName);
	    for (String cf : colfams)
	    {
	      HColumnDescriptor coldef = new HColumnDescriptor(cf);
	      desc.addFamily(coldef);
	    }
	    if (splitKeys != null) 
	    {
	      admin.createTable(desc, splitKeys);
	    }
	    else 
	    {
	      admin.createTable(desc);
	    }
	    KeyManager.add(tableName);

	 }
	
	/* We need to use has by KeyManager */
	private boolean existsTable(String tableName)
	throws IOException 
	{
		return admin.tableExists(tableName);
	}
	
	private void disableTable(String tableName)
	throws IOException 
	{
	    admin.disableTable(tableName);
	}
	
	public void deleteTable(String tableName) 
	throws IOException 
	{
	    if (existsTable(tableName)) 
	    {
	      disableTable(tableName);
	      admin.deleteTable(tableName);
	      KeyManager.del(tableName);
	    
	    }

	}
	
}
