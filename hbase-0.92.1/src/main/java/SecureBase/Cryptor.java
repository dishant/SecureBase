package SecureBase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;

import javax.crypto.Cipher;
import javax.crypto.spec.*;

import java.security.*;
import java.util.*;

import java.io.IOException;

public class Cryptor 
{
		
	byte[] tableDigest;
	
	/**
	 * Sets up the MessageDigest Object which can be updated according 
	 * to the row provided by the put object
	 * @param tableDigest
	 */
	public Cryptor(byte[] tableDigest)
	{
		this.tableDigest = tableDigest;	
	}
	
	/**
	 * Encrypts a Put object
	 * @param putToEncrypt
	 * @return Put EncryptedObject
	 */
	public Put encrypt(final Put putToEncrypt)
	{
		if(putToEncrypt == null) return null;
		MessageDigest md;
		Put encryptedPut = new Put(putToEncrypt.getRow(),putToEncrypt.getTimeStamp(),putToEncrypt.getRowLock());
		encryptedPut.setWriteToWAL(putToEncrypt.getWriteToWAL());
		
		try
		{
			md = MessageDigest.getInstance("MD5");
			md.update(tableDigest);
			md.update(putToEncrypt.getRow());
			
			
			byte[] rowDigest = md.digest();
			byte[] familyDigest;
			byte[] qualDigest;
			byte[] timestampDigest;
			
			
			for(Map.Entry<byte [], List<KeyValue>> entry : putToEncrypt.getFamilyMap().entrySet()) 
			{
				
				md.reset();
				md.update(rowDigest);
				md.update(entry.getKey());
				familyDigest = md.digest();
				
				
				for(KeyValue kv: entry.getValue())
				{
					md.reset();
					md.update(familyDigest);
					md.update(kv.getQualifier());
					
					qualDigest = md.digest();
					md.reset();
					md.update(qualDigest);
					md.update(Bytes.toBytes(kv.getTimestamp()));
					timestampDigest = md.digest();
					
					SecretKeySpec key = new SecretKeySpec(timestampDigest,"AES");
					Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
					c.init(Cipher.ENCRYPT_MODE, key);
					
					byte encrypted[] = c.doFinal(kv.getValue());
					
					encryptedPut.add(entry.getKey(), kv.getQualifier(), kv.getTimestamp(), encrypted);
					
				}
			}
				    
			return encryptedPut;
				  
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		
		return encryptedPut;
	}
	
	
	/**
	 * Decrypts a Result Object
	 * @param resultToDecrypt
	 * @return
	 */
	public Result decrypt(final Result resultToDecrypt)
	{
		if(resultToDecrypt == null) return null;
		KeyValue[] KVS = resultToDecrypt.raw();
		KeyValue[] decryptedKVS = new KeyValue[KVS.length];
		
		int i = 0;
		for(KeyValue KV : KVS)
		{
			decryptedKVS[i++] = decrypt(KV);
		}
		
		return new Result(decryptedKVS);
	}
	
	/**
	 * Decrypts a KeyValue Object
	 * @param keyvalueToDecrypt
	 * @return
	 */
	public KeyValue decrypt(KeyValue kvToDecrypt)
	{
		MessageDigest md;
		try
		{
			md = MessageDigest.getInstance("MD5");
			md.update(tableDigest);
			md.update(kvToDecrypt.getRow());
			
			byte[] rowDigest = md.digest();
			byte[] familyDigest;
			byte[] qualDigest;
			byte[] timestampDigest;
			
			md.reset();
			md.update(rowDigest);
			md.update(kvToDecrypt.getFamily());
			familyDigest = md.digest();
			
			md.reset();
			md.update(familyDigest);
			md.update(kvToDecrypt.getQualifier());
			qualDigest = md.digest();
			
			md.reset();
			md.update(qualDigest);
			md.update(Bytes.toBytes(kvToDecrypt.getTimestamp()));
			timestampDigest = md.digest();
			
			SecretKeySpec key = new SecretKeySpec(timestampDigest,"AES");
			Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
			
			c.init(Cipher.DECRYPT_MODE, key);
			byte decrypted[] = c.doFinal(kvToDecrypt.getValue());
			
			return new KeyValue(kvToDecrypt.getRow(),kvToDecrypt.getFamily(),kvToDecrypt.getQualifier(),kvToDecrypt.getTimestamp(),decrypted);
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	 
	  
	  public static KeyValue decryptCell(byte[] qualHash, KeyValue kv)
	  {
		  MessageDigest md;
		  try
			{
				md = MessageDigest.getInstance("MD5");
				md.update(qualHash);
				md.update(Bytes.toBytes(kv.getTimestamp()));
				byte[] timestampDigest = md.digest();
				
				SecretKeySpec key = new SecretKeySpec(timestampDigest,"AES");
				Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
				
				c.init(Cipher.DECRYPT_MODE, key);
				byte decrypted[] = c.doFinal(kv.getValue());
				
				return new KeyValue(kv.getRow(),kv.getFamily(),kv.getQualifier(),kv.getTimestamp(),decrypted);
				
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return null;
			}
				
	  }
	
}
