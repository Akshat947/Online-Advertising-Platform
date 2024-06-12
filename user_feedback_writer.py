from kafka import KafkaConsumer
import sys
import csv

### Setting up the Python consumer
bootstrap_servers = ['34.199.55.154:9092']
topicName = 'user-feedback'
consumer = KafkaConsumer (topicName, group_id = 'my_group_id',bootstrap_servers = bootstrap_servers,auto_offset_reset = 'earliest')

### Reading the kafka messages and writing to csv
try:
    lst=[]
    for message in consumer:
        f=open('/home/hadoop/user_feedback.csv','a')
        writer=csv.writer(f)
        txt=str(message.value,'UTF-8')
        lst.append(txt)
        if(len(lst)==19):
            writer.writerow(lst)
            lst=[]
        f.close()

except KeyboardInterrupt:
    sys.exit()
