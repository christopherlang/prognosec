import numpy
import pandas
import functools

"""
functions starting with `t` is a transformation function
functions starting with `a` is an aggrgation function
function starting with `r` is a upsampling function
"""

def t_log():
    return functools.partial(numpy.log)


def t_log10():
    return functools.partial(numpy.log10)


def t_log1p():
    return functools.partial(numpy.log1p)


def t_log2():
    return functools.partial(numpy.log2)


def a_mean():
    pass


def r_evenly_spread():
    def evenly_spread(x):
        print(x)
        v = list(set(x))[0]
        n = len(x)

        return numpy.array([v / n])

    return functools.partial(evenly_spread)

# def sma(n, min_periods=None, center=False, win_type=None, on=None, axis=0, closed=None):
#     # window, min_periods=None, center=False, win_type=None, on=None, axis=0, closed=None
#     fun = functools.partial(pan)