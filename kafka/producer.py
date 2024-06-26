from faker import Faker
import pandas as pd
import requests
import random
import json
from kafka import KafkaProducer
from datetime import datetime
import time

# Configurar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='192.168.56.10:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Inicializar el generador de datos aleatorios
fake = Faker()

# Leer los datos de los archivos CSV
products_zara_csv = pd.read_csv('../csv/products_zara.csv', delimiter=',')


# Función para generar registros de eventos de vista
def generate_view_logs():
    product_id = random.choice(products_zara_csv['sku'])
    product = products_zara_csv.loc[products_zara_csv['sku'] == product_id].iloc[0]
    log = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'event': 'view',
        'ip': fake.ipv4(),
        'user_id': fake.uuid4(),
        'product_id': product_id,
        'product_name': product['name'],
        'product_price': product['price'],
        'product_category': product['terms'],
        'currency': product['currency'],
        'product_section': product['section'],
        'response_code': random.choices([200, 400, 401, 403, 404, 500], weights=[0.9, 0.01, 0.01, 0.01, 0.01, 0.01])[0],
        'device_type': random.choice(['desktop', 'mobile', 'tablet']),
        'os': random.choice(['Windows', 'Mac', 'Linux', 'iOS', 'Android']),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera'])
    }
    return log


# Función para enviar registros de eventos de compra
def generate_purchase_logs():

    # Generar un evento de vista
    view_log = generate_view_logs()

    # Cantidad aleatoria de productos comprados
    quantity = random.randint(1, 5)

    log = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'event': 'purchase',
        'ip': view_log['ip'],
        'user_id': view_log['user_id'],
        'product_id': view_log['product_id'],
        'product_name': view_log['product_name'],
        'product_price': view_log['product_price'],
        'product_category': view_log['product_category'],
        'currency': view_log['currency'],
        'product_section': view_log['product_section'],
        'quantity': quantity,
        'response_code': random.choices([200, 400, 401, 403, 404, 500], weights=[0.9, 0.01, 0.01, 0.01, 0.01, 0.01])[0],
        'device_type': view_log['device_type'],
        'os': view_log['os'],
        'browser': view_log['browser'],
        'payment_method': fake.credit_card_provider(),
        'total_amount': view_log['product_price'] * quantity,
        'shipping_method': fake.word(),
        'shipping_cost': random.randint(0, 15)
    }
    return view_log, log


# Función para enviar registros a Kafka según su tipo
def enviar_a_kafka(registro):
    try:
        # Obtener el tipo de log y enviarlo al topic correspondiente
        log_type = registro['event']
        topic = f'zara-{log_type}'  # Ejemplo: 'zara-view', 'zara-purchase'

        # Convertir el registro a JSON y enviarlo al topic especificado
        producer.send(topic, value=registro)

        # Imprimir mensaje de confirmación
        print(f"Registro tipo '{log_type}' enviado a topic '{topic}'")

    except Exception as e:
        print(f"Error al enviar el registro: {e}")


if __name__ == "__main__":

    # Probabilidades de generar eventos de vista y compra
    prob_view_logs = 0.85
    prob_purchase_logs = 0.15

    for i in range(500):
        prob = random.random()
        if prob < prob_view_logs:
            enviar_a_kafka(generate_view_logs())
        else:
            view_log, purchase_log = generate_purchase_logs()
            enviar_a_kafka(view_log)
            enviar_a_kafka(purchase_log)

        print(f"Logs enviados: {i + 1}")
