import pika
import numpy as np
from sklearn.datasets import load_diabetes
import json
from datetime import datetime
import time


# загружаем датасет
X, y = load_diabetes(return_X_y=True)

try:
    while True:
        # сохраняем текущее время, которое будем использовать как id,
        # чтобы понимать, какие признаки и таргет из одного ряда
        ts = datetime.now().timestamp()

        # выбираем случайный ряд из датасета
        random_row = np.random.randint(0, X.shape[0]-1)

        # создадим объекты, которые будем передавать в очередь
        features_obj = {"ts": ts, "features": list(X[random_row])}
        y_true_obj = {"ts": ts, "y_true": float(y[random_row])}

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("rabbitmq"))

        channel = connection.channel()

        channel.queue_declare(queue="y_true")
        channel.basic_publish(exchange="", routing_key="y_true",
                              body=json.dumps(y_true_obj))
        print("Сообщение с правильным ответом отправлено в очередь")

        channel.queue_declare(queue="features")
        channel.basic_publish(exchange="", routing_key="features",
                              body=json.dumps(features_obj))
        print("Сообщение с признаками отправлено в очередь")

        connection.close()
        time.sleep(2)

except:
    print("Не удалось подключиться к очереди")
    raise
