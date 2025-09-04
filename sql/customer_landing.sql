CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string, 
  `email` string, 
  `phone` string, 
  `birthday` string, 
  `serialnumber` string, 
  `registrationdate` bigint, 
  `lastupdatedate` bigint, 
  `sharewithresearchasofdate` bigint, 
  `sharewithpublicasofdate` bigint, 
  `sharewithfriendsasofdate` bigint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-project-jimmy/customer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');