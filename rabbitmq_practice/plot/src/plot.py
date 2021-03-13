from matplotlib import pyplot as plt
import time

while True:
    # каждые 5 секунд читает файл, где сохранены ошибки,
    # строит график и сохраняет его в файл
    with open("data/errors.txt") as f:
        errors = [float(error) for error in f.read().split("\n")[:-1]]

        if errors:
            plt.title("RMSE")
            plt.plot(errors)
            plt.savefig("data/rmse.svg")

    time.sleep(5)
