from time import time


def str_timestamp():
    return str(time()).split(".")[0]


def timestamp():
    return int(str_timestamp())
