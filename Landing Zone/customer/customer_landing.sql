CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `birthday` date COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` timestamp COMMENT 'from deserializer', 
  `lastupdatedate` timestamp COMMENT 'from deserializer', 
  `sharewithresearchasofdate` timestamp COMMENT 'from deserializer', 
  `sharewithpublicasofdate` timestamp COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` timestamp COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedidb/customer/landing/'
TBLPROPERTIES (
  'classification'='json')
