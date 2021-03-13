import numpy as np


def score_game(game_core, num_range=(1, 101)):
    """
    Запускаем игру 1000 раз, чтобы узнать, как быстро игра угадывает число

    :param game_core: алгоритм, который угадывает загаданное число
    :param num_range: диапазон чисел для определения случайных чисел
    :return:
    """
    np.random.seed(
        1)  # фиксируем RANDOM SEED, чтобы эксперимент был воспроизводим!
    random_array = np.random.randint(num_range[0], num_range[1], size=1000)
    count_ls = [game_core(number, num_range) for number in random_array]
    score = int(np.mean(count_ls))
    print(f"Ваш алгоритм угадывает число в среднем за {score} попыток")
    return score


def game_core_v3(number, num_range):
    """
    Используем бинарный поиск для угадывания числа

    :param number: число, которое необходимо угадать
    :param num_range: диапазон чисел, внутри которого находится число
    :return: количество шагов, за которое число угадано
    """
    count = 0

    while True:
        count += 1
        # Определяем число, находящееся в середине диапазона
        guess = (num_range[0]+num_range[1]) // 2

        # Сравниваем число с загаданным:
        # - если оно равно загаданному, завершаем функцию
        # - если больше загаданного, оставляем только левую половину диапазона
        # - если меньше, оставляем только правую половину диапазона
        if guess == number:
            return count
        if guess > number:
            num_range = (num_range[0], guess)
        else:
            num_range = (guess+1, num_range[1])


score_game(game_core_v3)
