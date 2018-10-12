# hbase-py

## Prerequisites 
# Scala 2.12.7 
# Sbt 1.2.4 
# Java 1.8
# In case of Errors during the compilation change the property inside README.md <scala.version>2.11</scala.version> 

1. Sample of Usage of the tables 

Edit the file according to your cluster informations : 

# User Inputs (could be moved to arguments)
hostname            = 'hdpcluster-15377-worker-2.field.hortonworks.com'
port                = 9999 # hbase thrift server port 
table_name          = 'customer_info'
columnfamily        = 'demographics'
number_of_records   = 1000000
batch_size          = 2000


2. Load the data 

[dfossouo@hdpcluster-15377-compute-2 hbasepy]$ python write_to_hbase.py 
[ INFO ] Trying to connect to the HBase Thrift server at hdpcluster-15377-worker-2.field.hortonworks.com:9999
[ INFO ] Successfully connected to the HBase Thrift server at hdpcluster-15377-worker-2.field.hortonworks.com:9999
[ INFO ] Creating HBase table:  customer_info
[ INFO ] Successfully created HBase table:  customer_info
[ INFO ] Inserting 1000000 records into customer_info

[ INFO ] Successfully inserted 1000000 records into customer_info in 174 seconds
[ INFO ] Printing data records from the generated HBase table
('1', {'demographics:level': 'diamond', 'demographics:age': '23', 'demographics:gender': 'male', 'demographics:custid': '7530879'})
('2', {'demographics:level': 'platimum', 'demographics:age': '60', 'demographics:gender': 'male', 'demographics:custid': '1030564'})
('3', {'demographics:level': 'gold', 'demographics:age': '99', 'demographics:gender': 'male', 'demographics:custid': '9144031'})
('4', {'demographics:level': 'diamond', 'demographics:age': '21', 'demographics:gender': 'male', 'demographics:custid': '6349513'})
('5', {'demographics:level': 'diamond', 'demographics:age': '48', 'demographics:gender': 'female', 'demographics:custid': '6985163'})

