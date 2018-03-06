## Experimenting with Kafka

Few experiments using Kafka and Spark.
The aim is to process stream of data taken from Twitter and exploit Kafka's consumers for providing some insights:

 - CountingLang.scala: it counts the most popular languages from Twitter's users.
 - CountingSource.scala: it counts the most popular platform (iPhone, iPad, Android, Web etc).
 - CountingHashtags: it counts the most popular hashtags

 ## References
 - STData Labs: [real time sentimental analysis from Twitter](http://stdatalabs.blogspot.in/2016/09/spark-streaming-part-3-real-time.html)
 - Twitter API [documentation] (https://developer.twitter.com/docs)

 ## Readings 📖📚
 - [Publishing with Kafka at NYT](https://open.nytimes.com/publishing-with-apache-kafka-at-the-new-york-times-7f0e3b7d2077)
 - Netflix tech blog, [Kafka inside Keystone pipeline](https://medium.com/netflix-techblog/kafka-inside-keystone-pipeline-dd5aeabaf6bb)