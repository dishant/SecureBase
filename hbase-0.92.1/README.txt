INSTALL: STANDALONE MODE
________________________________
Required OS: Ubuntu Linux

1. Modify /etc/hosts 
	Remove the entry 127.0.1.1 since HBase expect the loopback id to be 127.0.0.1
	The file should contain "127.0.0.1 localhost your-machine-name"

2. Specify JAVA_HOME (e.g. export JAVA_HOME=/usr/lib/jvm/java-6-openjdk). JAVA 1.6 or higher is needed

3. The directory from which shell is started should have reading and writing permission for the user. Otherwise you will notice IOException in the shell.


USAGE
__________

To start hbase server, go to bin folder and type './start-hbase.sh'
start shell by typing './hbase shell'
Type 'help' to get general commands.

To create secureTables use command "securecreate" instead of create. You can go to hbase.root.dir to see the tables you created. You can check that secureTables are storing values in encrypted format. KeyManager



SOURCE_CODE
_____________

SecureBase sourcecode is available in src/main/java/SecureBase

Modification are also made into the Hbase source code
Particularly client/HTable.java, filter/ValueFilter.java and filter/SingleColumnValueFilter.java

