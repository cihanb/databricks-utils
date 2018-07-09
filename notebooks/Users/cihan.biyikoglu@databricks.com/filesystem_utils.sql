-- Databricks notebook source
-- MAGIC %python
-- MAGIC #dirsize_r: add total size of all files under a given path
-- MAGIC #  dir: path to the root folder to traverse (string: "dbfs:/...")
-- MAGIC #  debug: enable verbose debug details (default 0, 1 to enable)
-- MAGIC def dirsize_r(dir, debug = 0):
-- MAGIC   size = 0
-- MAGIC   for i in dbutils.fs.ls(dir):
-- MAGIC     if i.name[len(i.name)-1]== "/":
-- MAGIC       size = size + dirsize_r(i.path, debug)
-- MAGIC     else:
-- MAGIC       size = size + i.size
-- MAGIC     if debug == 1:
-- MAGIC       print "traversing: ",   i.path
-- MAGIC       print "current size: ", size
-- MAGIC   return size

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #dirfilecount_r: count all directories and files under a given path except the root. 
-- MAGIC #  dir: path to the root folder to traverse (string: "dbfs:/...")
-- MAGIC #  debug: enable verbose debug details (default 0, 1 to enable)
-- MAGIC def dirfilecount_r(dir, debug = 0):
-- MAGIC   count = 0
-- MAGIC   for i in dbutils.fs.ls(dir):
-- MAGIC     if i.name[len(i.name)-1]== "/":
-- MAGIC       # +1 for the directory
-- MAGIC       count = count + dirfilecount_r(i.path, debug) + 1  
-- MAGIC     else:
-- MAGIC       count = count + 1
-- MAGIC     if debug == 1:
-- MAGIC       print "traversing: ", i.path
-- MAGIC       print "current size: ", count
-- MAGIC   return count

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #ls_r: ls with recusion
-- MAGIC #  dir: path to the root folder to traverse (string: "dbfs:/...")
-- MAGIC #  debug: enable verbose debug details (default 0, 1 to enable)
-- MAGIC def ls_r(dir, debug = 0):
-- MAGIC   for i in dbutils.fs.ls(dir):
-- MAGIC     if i.name[len(i.name)-1]== "/":
-- MAGIC       ls_r(i.path, debug)
-- MAGIC     else:
-- MAGIC       print i.path
-- MAGIC     if debug == 1:
-- MAGIC       print "traversing: ",   i.path

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // dataSkippingStats: partition and file skipping stats
-- MAGIC //  query: string for SQL query (string: "select * from table where ...")
-- MAGIC def dataSkippingStats(query: String): Unit = {
-- MAGIC   import com.databricks.sql.transaction.tahoe._
-- MAGIC   import com.databricks.sql.transaction.tahoe.stats._
-- MAGIC 
-- MAGIC   val df = sql(query)
-- MAGIC   val stats = df.queryExecution.optimizedPlan.collect {
-- MAGIC     case DeltaTable(prepared: PreparedDeltaFileIndex) =>
-- MAGIC       prepared.preparedScan
-- MAGIC   }
-- MAGIC   println("Info:\n")
-- MAGIC   stats.foreach { stat =>
-- MAGIC     println(s"Skipped thanks to partitioning (% bytes): ${100 - (stat.partition.bytesCompressed.get.toDouble / stat.total.bytesCompressed.get) * 100}")
-- MAGIC     println(s"Additional reduction thanks to data skipping (% bytes): ${100 - (stat.scanned.bytesCompressed.get.toDouble / stat.partition.bytesCompressed.get) * 100}")
-- MAGIC   }
-- MAGIC   println("\n")
-- MAGIC }

-- COMMAND ----------

