# Tweet-Streams
Stream Tweets from twitter using apache Kafka
- First, install kafka on your machine

- To set up Kafka on local machine, refer to: 

- https://www.tutorialkart.com/apache-kafka/install-apache-kafka-on-mac/ (mac)

- https://dzone.com/articles/running-apache-kafka-on-windows-os (windows)

- Once installation is complete, follow the steps below:

- Opening a new terminal, Start Zookeeper using the following command: bin/zookeeper-server-start.sh config/zookeeper.properties
- Then, open a new tab in the terminal and then start the kafka server using this command: bin/kafka-server-start.sh config/server.properties
- Then, create a topic using the following command: bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic topicName
  - Make sure the topic in your code matches the topic you created
- Then run code in IDE, or terminal.

- For more Kafka Commands, refer to: http://ronnieroller.com/kafka/cheat-sheet#list-topics

