CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` bigint COMMENT 'from deserializer', 
  `birthday` date COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer',
  `registrationdate`bigint COMMENT 'from deserializer',
  `lastupdatedate` bigint COMMENT 'from deserializer',
  `sharewithresearchasofdate`bigint COMMENT 'from deserializer',
  `sharewithpublicasofdate`bigint COMMENT 'from deserializer',
  `sharewithfriendsasofdate`bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='customername','email','phone','birthday','serialnumber','registrationdate','lastupdatedate','sharewithresearchasofdate','sharewithpublicasofdate','sharewithfriendsasofdate') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hulopza-warehouse/customer/landing/'
TBLPROPERTIES (
  'classification'='json')