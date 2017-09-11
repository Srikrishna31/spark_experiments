# **Spark Experiments**
This repository explores spark and machine learning using different languages like Scala and Python. The idea is to compare and contrast the libraries support available for each language, ease of development and other factors. The basic concept behind the project is to do Twitter Sentiment Analysis, using Twtiiter4j libraries, filter the tweets based on various predicates, and plot graphs like Tweets received per time window, number of times a particular word occurs in tweets etc. 
The goal is to provide a notebook kind of environment, by integrating all the relevant plotting libraries for visualization, hooking the twitter stream data obtained through spark streaming. By doing this we hope to understand the differences between corresponding libraries for each technology and draw some conclusions regarding the relative popularity of certain libraries in ML space.

### Scala

For Scala, setup scala and spark as per this link: https://stackoverflow.com/questions/25481325/how-to-set-up-spark-on-windows
The link describes the steps well and concisely.

Once the setup is done, then launch the spark shell from the folder where the source file "TwitterStream.scala" is present, and 
then at the command prompt type: "TwitterStream.main('consumerKey', 'consumerSecret', 'accessToken', 'accessTokenSecret')".   
Replace the key and secret keys with appropriate twitter hash values. Details of how to obtain them can be found here: 
https://apps.twitter.com/app/new

Please note that you will need to sign in with your credentials, and based on the name you give to the app, would need to change it 
in the code: 
    val config = new SparkConf().setAppName("your-app-name-here")


### Python
For python, install Python3.5, setup the required libs using the requirements.txt. 

Once the setup is done, then open a command prompt, and navigate to the path where this project is cloned,
and then type the following:
python Spark_Python.py 'consumerToken' 'consumerTokenSecret' 'accessToken' 'accessTokenSecret'
and then you should see that json files should be created in the current directory from where python script
is launched.