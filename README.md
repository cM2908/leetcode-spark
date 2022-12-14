# Spark Solutions + Leetcode SQL Questions<br/>
<div>
<kbd><img src="https://github.com/cM2908/leetcode-spark/blob/main/apache_spark.png" width="375" height="200"/></kbd>
<img src="https://github.com/cM2908/leetcode-spark/blob/main/plus.png" width="30" height="30"/>
<kbd><img src="https://github.com/cM2908/leetcode-spark/blob/main/leetcode.png" width="375" height="200"/></kbd>
</div>

#### Want to practice & solve some complex questions using Spark? 
- Then nothing is better than solving some Leetcode questions, but you might think how, read along & you will get a fair idea.<br/>
- Now, Execute Spark Dataframe/Dataset/SQL/RDD code on Leetcode SQL Questions.

#### Problem statements of all questions including leetcode premium questions :<br/>

- https://www.jiakaobo.com/leetcode <br/>
- https://leetcode.ca <br/>

#### Repository Contains :<br/>
- Spark Dataframe/Dataset/SQL/RDD Solutions on Leetcode Questions <br/>
- PostgreSQL Dump File (leetcodedb.sql)<br/>

#### Get Started :<br/>

- This guide assumes that you already have PostgreSQL database & Apache Spark installed<br/>
- Load dump file to your local PostgreSQL setup.<br/>
- [Leetcode-Sql](https://github.com/cM2908/leetcode-sql) This repository contains all the information needed to load postgresql dump file (that contains tables of all leetcode sql questions) into your local postgresql setup.<br/>

#### Integrate Apache Spark with PostgreSQL database :<br/> 

- Download PostgreSQL JDBC Connector JAR (select appropriate version of JAR according your PostgreSQL setup)<br/>

- Add PostgreSQL JDBC Connector jar to "spark/jars" directory. (This step will make the JAR available directly to the classpath when starting the spark-shell)<br/>

- Start Spark-Shell
```
user@my-machine:~$ spark-shell
```

- Step to Check for Classpath in Spark Shell (Optional)
```
scala> import java.lang.ClassLoader
scala> val cl = ClassLoader.getSystemClassLoader
scala> cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(println)
```

- Connection code (Replace your credentials)<br/>
```
scala> val url = "jdbc:postgresql://localhost:5432/postgres?user=<your-username>&password=<your-password>"
scala> import java.util.Properties
scala> val connectionProperties = new Properties()
scala> connectionProperties.setProperty("Driver", "org.postgresql.Driver")
```

- Example
```
scala> val query = "(SELECT * FROM employee_181) AS employee"
scala> val employeeDF = spark.read.jdbc(url, query, connectionProperties)
scala> val joinedDF = employeeDF.as("emp")
                        .join(employeeDF.as("mgr"),$"emp.manager_id"===$"mgr.id" && $"emp.salary" > $"mgr.salary","inner")
                        .select($"emp.name")
scala> joinedDF.show
```

#### Want to Contribute :<br/>

- Contribute by providing solution of any question in either/all of these dialacts (Spark DataFrame,Spark DataSet,Spark RDD,Spark SQL)
- Forked the repository 
- Create solution file with proper name (eg. "175. Combine Two Tables (Easy).txt")
- Create Pull Request
- After review I'll merge it with the main repository.
- Congratulations, you've contributed something to the data community.
