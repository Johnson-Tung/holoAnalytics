import math


def round_up_nearest_x(number, x):

    return math.ceil(number / x) * x


def calculate_interval(number, factor=1):

    digits = int(math.log10(number)) + 1
    interval = int(int('1' + '0' * (digits - 1)) / factor)

    return interval
