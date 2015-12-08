How to use
==========

1. Checkout from github

		$ git clone git@github.com:LambdaNow/lambdanow-examples.git
		$ cd lambdanow-examples/spark-analytics/
		$ cp src/main/resources/application.properties.template src/main/resources/application.properties

2. Use your favorite editor to modify src/main/resources/application.properties by adding your cluster's correct config values. The process is better exampled [in this blog post](http://blog.lambdanow.com/building-an-analytics-service-part2/).

3. Build package

		$ mvn -U clean package
		
4. Upload to your NameNode instance
 
		$ rsync --partial --progress ./target/lambdanow-sparkanalytics-1.0-SNAPSHOT.jar ubuntu@<NAMENODE-PUBLIC-DNS-NAME>:~/
		
5. Submit spark job
	
		$ spark-submit --master yarn --class com.lambdanow.sparkanalytics.AnalyticsCollector ~/lambdanow-sparkanalytics-1.0-SNAPSHOT.jar
	
