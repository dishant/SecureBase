SecureBase
==========

SecureBase provides client side encryption on top of Apache HBase. It's API is exactly same as that of HBase except at
the time of creation of table user specifies whether or not the table should be encrypted. 

It takes few minutes to try SecureBase. Just clone the repository and follow installation instructions provided in the 
folder. 

It comes with an interactive ruby based shell like that of HBase. To create secureTables you need to use 'securecreate' in
place of create.