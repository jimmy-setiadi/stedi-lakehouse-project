CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorreadingtime` bigint, 
  `serialnumber` string, 
  `distancefromobject` int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-project-jimmy/step-trainer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');