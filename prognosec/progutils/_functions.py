import collections
import copy
import numpy
import pandas
import functools
from progutils import progutils

"""
functions starting with `trans` is a transformation function
functions starting with `agg` is an aggrgation function
functions starting with `sampleup` is a upsampling function
functions starting with `sampledown` is a downsampling function
functions starting with `fillnan` is s `NaN` fill function
"""


class TransformationFunction:
    def __init__(self, func, name, params=None):
        self._func = func
        self._name = name
        self._params = params

    @property
    def name(self):
        return self._name

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def __str__(self):
        if self._params is None:
            output = self._name + "()"
        else:
            params_values = self._params.items()
            funname_params = [k + "=" + str(v) for k, v in params_values]
            funname_params = ", ".join(funname_params)
            funname_params = "(" + funname_params + ")"

            output = self._name + funname_params

        return output

    def __repr__(self):
        return f"<TransformationFunction: {self.__str__()}>"


class MissingValueFillFunction(TransformationFunction):
    def __init__(self, func, name, params=None):
        super().__init__(func, name, params=params)

    def __repr__(self):
        return f"<MissingValueFillFunction: {self.__str__()}>"


class AggregateFunction(TransformationFunction):
    def __init__(self, func, name, params=None):
        super().__init__(func, name, params=params)

    def __repr__(self):
        return f"<AggregateFunction: {self.__str__()}>"


class DownsampleFunction(TransformationFunction):
    def __init__(self, func, name, params=None):
        super().__init__(func, name, params=params)

    def __repr__(self):
        return f"<DownsampleFunction: {self.__str__()}>"


class UpsampleFunction(TransformationFunction):
    def __init__(self, func, name, params=None):
        super().__init__(func, name, params=params)

    def __repr__(self):
        return f"<UpsampleFunction: {self.__str__()}>"


def trans_log():
    func = functools.partial(numpy.log)
    output_func = TransformationFunction(func, 'ln')

    return output_func


def trans_ln():
    return trans_log()


def trans_log10():
    func = functools.partial(numpy.log10)
    output_func = TransformationFunction(func, 'log10')

    return output_func


def trans_log1p():
    func = functools.partial(numpy.log1p)
    output_func = TransformationFunction(func, 'log1p')

    return output_func


def trans_log2():
    func = functools.partial(numpy.log2)
    output_func = TransformationFunction(func, 'log2')

    return output_func


def trans_sqrt():
    func = functools.partial(numpy.sqrt)
    output_func = TransformationFunction(func, 'sqrt')

    return output_func


def trans_reverse():
    func = functools.partial(numpy.flip)
    output_func = TransformationFunction(func, 'reverse')

    return output_func


def trans_flip():
    func = functools.partial(numpy.flip)
    output_func = TransformationFunction(func, 'flip')

    return output_func


def trans_shift(shift=1):
    def shift_array(x, shift=shift):
        if progutils.is_tuple_or_list(x):
            x = numpy.array(x)

        input_array = x.astype(float)

        if shift > 0:
            output = numpy.roll(input_array, shift)
            output[:shift] = numpy.nan
        elif shift < 0:
            output = numpy.roll(input_array, shift)
            output[shift:] = numpy.nan
        else:
            # For when shift == 0
            output = input_array

        if isinstance(x, pandas.Series):
            output_series = output
            output = x.copy(deep=True)
            output[:] = output_series

        return output

    return shift_array


def trans_inverse(add=0):
    def inverse(x, add=add):
        if progutils.is_tuple_or_list(x):
            x = numpy.array(x)

        return 1 / (x + add)

    func = functools.partial(inverse)

    param_dict = collections.OrderedDict()
    param_dict['add'] = add

    output_func = TransformationFunction(func, 'inverse', param_dict)

    return output_func


# def r_evenly_spread():
#     def evenly_spread(x):
#         print(x)
#         v = list(set(x))[0]
#         n = len(x)

#         return numpy.array([v / n])

#     return functools.partial(evenly_spread)


def agg_mean():
    def mean(x):
        x = _remove_nan_inf(x)
        agg_mean = numpy.mean(x)

        return agg_mean

    func = functools.partial(mean)
    output_func = AggregateFunction(func, 'mean')

    return output_func


def agg_median():
    def median(x):
        x = _remove_nan_inf(x)
        agg_median = numpy.median(x)

        return agg_median

    func = functools.partial(median)
    output_func = AggregateFunction(func, 'median')

    return output_func


def agg_max():
    def max(x):
        x = _remove_nan_inf(x)
        agg_max_func = numpy.max(x)

        return agg_max_func

    func = functools.partial(max)
    output_func = AggregateFunction(func, 'max')

    return output_func


def agg_min():
    def min(x):
        x = _remove_nan_inf(x)
        agg_min_func = numpy.min(x)

        return agg_min_func

    func = functools.partial(min)
    output_func = AggregateFunction(func, 'min')

    return output_func


def fillnan_aggregate(agg_fill_func=agg_mean()):
    def nan_fill_agg(x):
        fill_value = agg_fill_func(x)
        x = copy.deepcopy(x)
        x[numpy.isnan(x)] = fill_value
        x[numpy.isinf(x)] = fill_value

        return x

    func = functools.partial(nan_fill_agg)

    param_dict = collections.OrderedDict()
    param_dict['method'] = agg_fill_func.name

    output_func = MissingValueFillFunction(func, 'nan_fill_aggregate',
                                           param_dict)

    return output_func


def sampledown_mean():
    agg_func = agg_mean()

    func = functools.partial(agg_func._func)
    output_func = DownsampleFunction(func, 'downsample_mean')

    return output_func


def sampledown_median():
    agg_func = agg_median()

    func = functools.partial(agg_func._func)
    output_func = DownsampleFunction(func, 'downsample_median')

    return output_func


def sampledown_max():
    agg_func = agg_max()

    func = functools.partial(agg_func._func)
    output_func = DownsampleFunction(func, 'downsample_max')

    return output_func


def sampledown_min():
    agg_func = agg_min()

    func = functools.partial(agg_func._func)
    output_func = DownsampleFunction(func, 'downsample_min')

    return output_func


def _remove_nan_inf(x):
    x = x[~numpy.isnan(x)]
    x = x[~numpy.isinf(x)]

    return x
