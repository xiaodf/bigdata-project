
clush -g rocfka "jps | grep RocketPushToKafka | awk '{print @1}' | xargs kill"
