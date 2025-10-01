import pika
import json
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='user_events', exchange_type='direct', durable=True)

result = channel.queue_declare(queue='login_queue', durable=True)
queue_name = result.method.queue

channel.queue_bind(exchange='user_events', queue=queue_name, routing_key='user.login')

print(' [*] Consumidor de LOGIN aguardando mensagens. Para sair, pressione CTRL+C')

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        user = data.get('user', 'Desconhecido')
        
        print(f" [LOGIN] {user} acabou de fazer login!")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.JSONDecodeError:
        print(" [!] Erro ao decodificar a mensagem JSON.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) 
    except Exception as e:
        print(f" [!] Ocorreu um erro: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 


channel.basic_consume(queue=queue_name, on_message_callback=callback)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('Interrompido')
    sys.exit(0)