import numpy
import pandas
import functools


def log():
    return functools.partial(numpy.log)


def log10():
    return functools.partial(numpy.log10)


def log1p():
    return functools.partial(numpy.log1p)


def log2():
    return functools.partial(numpy.log2)

# def sma(n, min_periods=None, center=False, win_type=None, on=None, axis=0, closed=None):
#     # window, min_periods=None, center=False, win_type=None, on=None, axis=0, closed=None
#     fun = functools.partial(pan)