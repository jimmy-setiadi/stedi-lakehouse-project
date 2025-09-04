CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string, 
  `timestamp` bigint, 
  `x` float, 
  `y` float, 
  `z` float)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-project-jimmy/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');