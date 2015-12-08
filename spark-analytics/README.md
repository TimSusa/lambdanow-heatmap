How to use
==========

1. Checkout from github

		$ git clone git@github.com:LambdaNow/lambdanow-examples.git
		$ cd lambdanow-examples
	
2. Build package

		$ mvn -U clean package
		
3. Upload to your NameNode instance
 
		$ rsync --partial --progress ./target/lambdanow-sparkanalytics-1.0-SNAPSHOT.jar ubuntu@ec2-54-175-228-0.compute-1.amazonaws.com:~/
		
4. Submit spark job
	
		$ spark-submit --master yarn --class com.lambdanow.sparkanalytics.AnalyticsCollector ~/lambdanow-sparkanalytics-1.0-SNAPSHOT.jar
	
