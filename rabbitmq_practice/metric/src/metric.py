import pika
import json
import numpy as np
from collections import defaultdict


def read_file(name):
    with open(f"data/{name}.txt", "r") as log_file:
        return log_file.read()


def update_file(name, data):
    with open(f"data/{name}.txt", "w") as log_file:
        log_file.write(data)


def calc_rmse(log):
    """Считает метрику RMSE, используя логи"""
    groupsByTs = defaultdict(dict)

    # группируем логи по значению timestamp
    for entry in log.split("\n"):
        if len(entry):
            ts, name, value = entry.split()
            groupsByTs[ts][name] = float(value)

    # оставляем только те группы, где есть и истинное значение и предсказание
    filteredGroups = {
        ts: groupsByTs[ts] for ts in groupsByTs if len(groupsByTs[ts]) == 2}

    # считаем ошибку в каждой группе и сохраняем в виде массива numpy
    errors = np.array([entry["y_true"] - entry["y_pred"]
                       for entry in filteredGroups.values()])

    return np.sqrt(np.sum(errors ** 2) / len(errors)) if len(errors) else 0


try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()

    channel.queue_declare(queue="y_true")
    channel.queue_declare(queue="y_pred")

    def callback(ch, method, props, body):
        # читаем сообщение и добавляем запись в лог файл
        target_obj = json.loads(body)
        log = read_file("log")
        new_entry = f"{target_obj['ts']} {method.routing_key} {target_obj[method.routing_key]}\n"
        new_log = log + new_entry
        update_file("log", new_log)

        # читаем файл с ошибками и считаем новую
        errors = read_file("errors")
        errors_list = errors.split("\n")[:-1]
        last_error = float(errors_list[-1]) if errors_list else 0
        new_error = calc_rmse(new_log)

        # добавляем новую ошибку в файл, если она новая и не равна нулю
        if new_error != 0 and new_error != last_error:
            new_errors = errors + f"{new_error}\n"
            update_file("errors", new_errors)
            print(f"Текущее значение RMSE: {new_error}")

    channel.basic_consume(
        queue="y_pred", on_message_callback=callback, auto_ack=True)
    channel.basic_consume(
        queue="y_true", on_message_callback=callback, auto_ack=True)

    print("..Ожидание сообщений, для выхода нажмите CTRL+C")
    channel.start_consuming()
except:
    print("Не удалось подключиться к очереди")
    raise
