-- type: hive
CREATE TABLE %NAME% (
  c1 INT,
  c2 string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '%LOCATION%'
TBLPROPERTIES('serialization.null.format'='#')
