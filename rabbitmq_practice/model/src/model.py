import pika
import pickle
import numpy as np
import json

with open("myfile.pkl", "rb") as pkl_file:
    regressor = pickle.load(pkl_file)

try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()

    channel.queue_declare(queue="features")
    channel.queue_declare(queue="y_pred")

    def callback(ch, method, props, body):
        print(f"Получен вектор признаков {body}")
        obj = json.loads(body)

        # предсказываем значение, используя полученные признаки
        y_pred = regressor.predict([obj["features"]])[0]
        y_pred_obj = {"ts": obj["ts"], "y_pred": float(y_pred)}
        channel.basic_publish(exchange="", routing_key="y_pred",
                              body=json.dumps(y_pred_obj))

    channel.basic_consume(
        queue="features", on_message_callback=callback, auto_ack=True)
    print("...Ожидание сообщений, для выхода нажмите CTRL+C")
    channel.start_consuming()
except:
    print("Не удалось подключиться к очереди")
    raise
