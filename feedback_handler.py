from kafka import KafkaProducer
import flask
from flask import request,jsonify
import mysql.connector
import time

#Setting up the producer configurations
bootstrap_servers = ['34.200.197.226:9092']
topicName = 'user-feedback'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()


app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['RESTFUL_JSON'] = {"ensure_ascii": False}

@app.route('/ad/<ad_request_id>/feedback', methods=['POST'])
def feedback(ad_request_id):
    conn = mysql.connector.connect(user='admin', password='password',
                               host='ads-database.czdhajbrjw1o.us-east-1.rds.amazonaws.com',
                               database='ads_db')
    cur=conn.cursor()
    request_body=request.json
    view=request_body['view']
    click=request_body['click']
    acquisition=request_body['acquisition']


    query='select * from served_ads where request_id=%s'
    values=(ad_request_id,)
    cur.execute(query,values)
    data=cur.fetchall()
    for row in data:
        request_id=row[0]
        campaign_id=row[1]
        user_id=row[2]
        auction_cpm=row[3]
        auction_cpc=row[4]
        auction_cpa=row[5]
        target_age_range=row[6]
        target_location=row[7]
        target_gender=row[8]
        target_income_bucket=row[9]
        target_device_type=row[10]
        campaign_start_time=row[11]
        campaign_end_time=row[12]

    timestamp=str(round(time.time()))

    if(acquisition==1):
        expenditure=auction_cpa
        user_action='acquisition'
    elif(click==1):
        expenditure=auction_cpc
        user_action='click'
    else:
        expenditure=0
        user_action='view'

    cur.execute('select budget,current_slot_budget from ads where campaign_id=%s',(campaign_id,))
    for row in cur:
        budget=row[0]
        current_slot_budget=row[1]

    budget=int(budget)-expenditure
    current_slot_budget=int(current_slot_budget)-expenditure
    if(budget<=0):
        status="inactive"
    else:
        status="success"

    cur.execute('update ads set budget=%s where campaign_id=%s',(budget,campaign_id))
    cur.execute('update ads set current_slot_budget=%s where campaign_id=%s',(current_slot_budget,campaign_id))
    cur.execute('update ads set status=%s where campaign_id=%s',(status,campaign_id))
    cur.execute('update served_ads set timestamp=%s where request_id=%s',(timestamp,ad_request_id))

    # Sending the kafka message
    producer.send(topicName, key=b'Campaign ID' , value=bytes(campaign_id,'utf-8'))
    producer.send(topicName, key=b'User ID' , value=bytes(user_id,'utf-8'))
    producer.send(topicName, key=b'Request ID' , value=bytes(request_id,'utf-8'))
    producer.send(topicName, key=b'Click' , value=bytes(str(click),'utf-8'))
    producer.send(topicName, key=b'View' , value=bytes(str(view),'utf-8'))
    producer.send(topicName, key=b'Acquisition' , value=bytes(str(acquisition),'utf-8'))
    producer.send(topicName, key=b'Auction CPM' , value=bytes(str(auction_cpm),'utf-8'))
    producer.send(topicName, key=b'Auction CPC' , value=bytes(str(auction_cpc),'utf-8'))
    producer.send(topicName, key=b'Auction CPA' , value=bytes(str(auction_cpa),'utf-8'))
    producer.send(topicName, key=b'Target Age Range' , value=bytes(target_age_range,'utf-8'))
    producer.send(topicName, key=b'Target Location' , value=bytes(target_location,'utf-8'))
    producer.send(topicName, key=b'Target Gender' , value=bytes(target_gender,'utf-8'))
    producer.send(topicName, key=b'Target Income Bucket' , value=bytes(target_income_bucket,'utf-8'))
    producer.send(topicName, key=b'Target Device type' , value=bytes(target_device_type,'utf-8'))
    producer.send(topicName, key=b'Campaign Start Time' , value=bytes(campaign_start_time,'utf-8'))
    producer.send(topicName, key=b'Campaign End Time' , value=bytes(campaign_end_time,'utf-8'))
    producer.send(topicName, key=b'Action' , value=bytes(user_action,'utf-8'))
    producer.send(topicName, key=b'Expenditure' , value=bytes(str(expenditure),'utf-8'))
    producer.send(topicName, key=b'Timestamp' , value=bytes(timestamp,'utf-8'))

    conn.commit()
    cur.close()
    conn.close()

    print(jsonify({"status":status})

    return jsonify({"status":status})

# Start Flask application
app.run(host="0.0.0.0", port=5000)


