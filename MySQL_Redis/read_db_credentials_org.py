import json

credentials = json.load(open("./credentials", 'r', encoding='utf8'))
mysql_host = credentials.get("MySQL")
mysql_port = credentials.get("MySQLPort")
mysql_user = credentials.get("MySQLuser")
mysql_pswd = credentials.get("MySQLpswd")
redis_host = credentials.get("Redis")
redis_port = credentials.get("RedisPort")
redis_pswd = credentials.get("Redispswd")
kafka_host = credentials.get("Kafka")
es_host = credentials.get("ES")
