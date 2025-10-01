import pika
import json
import datetime
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='user_events', exchange_type='direct', durable=True)


events = [
    {'user': 'João', 'event': 'user.login'},
    {'user': 'Maria', 'event': 'user.upload'},
    {'user': 'Pedro', 'event': 'user.logout'},
    {'user': 'Ana', 'event': 'user.login'}
]


for event_data in events:

    event_data['timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    

    routing_key = event_data['event']
    

    message = json.dumps(event_data)
    

    channel.basic_publish(
        exchange='user_events',
        routing_key=routing_key,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  
        ))
    
    print(f" [x] Enviado '{message}' com a routing key '{routing_key}'")
    time.sleep(1) 

# Fecha a conexão
connection.close()