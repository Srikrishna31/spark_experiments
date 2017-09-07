# spark_experiments
This repository explores spark and machine learning using different languages like Scala and Python.

For Scala, setup scala and spark as per this link: https://stackoverflow.com/questions/25481325/how-to-set-up-spark-on-windows
The link describes the steps well and concisely.

Once the setup is done, then launch the spark shell from the folder where the source file "TwitterStream.scala" is present, and 
then at the command prompt type: "TwitterStream.main('consumerKey', 'consumerSecret', 'accessToken', 'accessTokenSecret')".   
Replace the key and secret keys with appropriate twitter hash values. Details of how to obtain them can be found here: 
https://apps.twitter.com/app/new

Please note that you will need to sign in with your credentials, and based on the name you give to the app, would need to change it 
in the code: 
    val config = new SparkConf().setAppName("your-app-name-here") 

