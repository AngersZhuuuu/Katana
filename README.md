# Katana

Katana is a Spark SQL plugin to support Spark use SQL between different Hive Metastore. 
We will define different `catalog` for different Hive Metastore we will connect and query
data with `catalog`, such as 
```
  SELECT * FROM [CATALOG].[DB].[TABLE]
```

## Build

cd /path/to/Katana folder, build with 
```
mvn clean install
```
You will get a jar under /path/to/Katana/target/Katana-${version}.jar

## USE
Submit spark app with `Katana-${version}.jar` or put it into load jar class path

## configuration

```
// when use this, we need to close convert Metastore since have not try with convert open
spark.sql.hive.convertMetastoreOrc false
spark.sql.hive.convertMetastoreParquet false	
  
// config SparkSessionExtension entrance
spark.sql.extensions org.apache.spark.sql.hive.KatanaExtension
  
// if your added hive metatsore have data in other HDFS system, you should config with this 
spark.yarn.access.hadoopFileSystems hdfs://xxx, hdfs://yyy...etc
  
// config mounted hive metastore with catalog name hive_catalog_1 & hive_catalog_2 ...
spark.sql.katana.catalog.instances hive_catalog_1,hive_catalog_2

// config each catalog's metastore uri
spark.sql.katana.hive.metastore.uris.hive_catalog_1 hive_metastore_uri1
spark.sql.katana.hive.metastore.uris.hive_catalog_1 hive_metastore_uri2

// config each catalog corresponding hive metastore's warehouse path 
spark.sql.katana.hive.metastore.warehouse.dir.hive_catalog_1 hdfs://hdfs_server_for_hive/path/to/warehouse2
spark.sql.katana.hive.metastore.warehouse.dir.hive_catalog_2 hdfs://hdfs_server_for_hive/path/to/warehouse2

// config each catalog's staging dir or scratch dir for insert data
spark.sql.katana.hive.exec.stagingdir.hive_catalog_1 hdfs://hdfs_server_for_hivepath/to/stagingDir
spark.sql.katana.hive.exec.stagingdir.hive_catalog_2 hdfs://hdfs_server_for_hivepath/to/stagingDir

```

## query
If we have mount two hive metastore named `hive_catalog_1` & `hive_catalog_2`, student table is in `hive_catalog_1` and 
score table in `hive_catalog_2`,  if you want to get all student's info who's score is higher then 90, you can query like below:

```
SELECT A.* FROM hive_catalog_1.default.student A 
JOIN hive_catalog_2.default.score B 
ON A.id = B.id AND B.score_num > 90
```


## NOTICE
