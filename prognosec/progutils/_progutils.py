import collections
import functools
import inspect
import copy
import pandas
import numpy


class Transformation:
    """Ordered storage of array transformations

    A transformation maintains a sequence of functions (aka procedure) for
    re-usable application of a sequence of transformation on a given array.


    Attributes
    ----------
    procedure : list[function]
        A sequence of functions to be applied to a data series. Functions are
        applied in order of the list. If `list` is empty, then no functions are
        stored
    size : int
        The number of functions stored
    empty : bool
        Whether there any functions. If zero, there are none, and data series
        passed to `apply` will be passed back
    plan : str, None
        A string representation of the transformation functions to be applied,
        in order. If there are no functions, then `None` is returned
    """

    def __init__(self):
        self._procedure = list()

    @property
    def procedure(self):
        return self._procedure

    @procedure.setter
    def procedure(self, proc):
        if isinstance(proc, list) is not True:
            raise TypeError("'proc' should be a list")

        if callable_sequence(proc) is not True:
            raise TypeError("'proc' should be a list of functions")

        self._procedure = proc

    @property
    def size(self):
        return len(self.procedure)

    @property
    def empty(self):
        return self.size == 0

    @property
    def plan(self):
        if self.procedure:
            output = ['x'] + [str(i) for i in self.procedure]
            output = ' -> '.join(output)
        else:
            output = None

        return output

    def copy(self):
        """Return a deep copy of the transformation object

        Returns
        -------
        Transformation
            A deep copy version of the `Transformation` object is returned
        """
        return copy.deepcopy(self)

    def get(self, index: int):
        """Retrieve a transformation function by index

        Indexing is the same as a `list` object.

        Parameters
        ----------
        index : int
            The function at `index` to be retrieved

        Returns
        -------
        function
            The function at index `index`

        Raises
        ------
        IndexError
            `index` value does not exist in `procedure`
        """
        return self.procedure[index]

    def add(self, func):
        """Add a new transformation function

        Functions added will be added to the end of the procedure.

        Parameters
        ----------
        func : callable (for array_like inputs)
            A transformation function to be added

        Raises
        ------
        TypeError
            Usually raised because `func` is not a function
        """
        if callable(func) is not True:
            raise TypeError("'func' should be a function")

        self.procedure.append(func)

    def drop(self, index):
        """Remove a function by index

        Parameters
        ----------
        index : int
            The function at `index` to be retrieved

        Raises
        ------
        IndexError
            `index` value does not exist in `procedure`
        """
        self.procedure.pop(index)

    def drop_first(self):
        """Remove the first function in the procedure

        Parameters
        ----------
        index : int
            The function at `index` to be retrieved

        Raises
        ------
        IndexError
            Raises when there are no functions i.e. `self.size == 0`,
            `procedure` is empty.
        """
        self.procedure.pop(0)

    def drop_last(self):
        """Remove the last function in the procedure

        Raises
        ------
        IndexError
            Raises when there are no functions i.e. `self.size == 0`,
            `procedure` is empty.
        """
        self.procedure.pop(-1)

    def insert_before(self, func, index):
        """Insert a new function into the procedure

        Uses the `list.insert` method. Therefore, this inserts the function
        after the specified index.

        Note, this will generally not raise an `IndexError` if index is out
        of bound. It will insert even in an empty list. If for example the
        procedure is empty, calling `insert_before(10, functions.log())` will
        execute, where `functions.log()` will become index 0. Please see
        object attribute `plan` to verify the order of operation.

        Parameters
        ----------
        func : callable (for array_like inputs)
            A transformation function to be added
        index : int
            Insert `func` before this index
        """
        self.procedure.insert(index, func)

    def insert_first(self, func):
        """Insert a new function as the first function in procedure

        Uses the `list.insert` method. Therefore, this inserts the function
        after the specified index. The same caveat in `insert_before` method
        applies here.

        Parameters
        ----------
        func : callable (for array_like inputs)
            A transformation function to be added
        """
        self.insert_before(func, 0)

    def insert_last(self, func):
        """Insert a new function as the first function in procedure

        Uses the `list.append` method. Therefore this will always be last.

        Parameters
        ----------
        func : callable (for array_like inputs)
            A transformation function to be added
        """
        self.procedure.append(func)

    def clear(self):
        """Clear all functions from procedure"""
        self.procedure.clear()

    def apply(self, data_series):
        """Apply transformations onto a data series

        The sequence of transformation functions stored in this object will
        be applied sequentially onto the data series. If there are no functions
        e.g. `self.size == 0` then the data series provided will be returned
        as-is, converted to a `numpy.ndarray` if `data_series` is a `tuple` or
        a `list`.

        Parameters
        ----------
        data_series : pandas.Series, numpy.ndarray, list, tuple
            The data series to apply the transformations

        Returns
        -------
        pandas.Series, numpy.ndarray
            A copy of the data series is returned. If `data_series` is either
            a `pandas.Series` or `numpy.ndarray`, then the same types will be
            returned. If `list` or `tuple`, then it is converted to a
            `numpy.ndarray`
        """
        output = copy.deepcopy(data_series)
        for a_trans_fun in self.procedure:
            output = a_trans_fun(output)

        if is_tuple_or_list(data_series):
            output = numpy.array(output)

        return output

    def __repr__(self):
        output = list()
        output.append("Transformation")
        output.append(f"# of functions: {self.size}")
        output.append(self.plan)
        output = "\n".join(output)

        return output


def is_sequence(x):
    """Test if x is of type {`tuple`, `list`, `set`, `numpy.ndarray`}"""
    return isinstance(x, (tuple, list, set, numpy.ndarray))


def is_tuple_or_list(x):
    """Test if x is of type {`tuple`, `list`}"""
    return isinstance(x, (tuple, list))


def isinstance_sequence(x, obj_type):
    """Test if all objects in `x` is of type `obj_type`"""
    return all([isinstance(i, obj_type) for i in x])


def callable_sequence(x):
    """Test if all objects in `x` are callable (e.g. functions)"""
    return all([callable(i) for i in x])


def is_time_index(x):
    acceptable_index_type = (pandas.DatetimeIndex, pandas.PeriodIndex,
                             pandas.TimedeltaIndex)

    return isinstance(x, acceptable_index_type)


def is_dateoffset(freq):
    return isinstance(freq, pandas.tseries.offsets.DateOffset)


def to_dateoffset(freq):
    if isinstance(freq, str):
        output = pandas.tseries.frequencies.to_offset(freq)
    elif is_dateoffset(freq):
        output = freq
    else:
        raise TypeError("'from_freq' is not str or DateOffset")

    return output


def is_upsample(from_freq, to_freq):
    from_freq = to_dateoffset(from_freq)
    to_freq = to_dateoffset(to_freq)

    return freq_rank(from_freq) > freq_rank(to_freq)


def is_downsample(from_freq, to_freq):
    from_freq = to_dateoffset(from_freq)
    to_freq = to_dateoffset(to_freq)

    return freq_rank(from_freq) < freq_rank(to_freq)


def freq_rank(freq):
    if isinstance(freq, str):
        pass
    else:
        freq = str(freq)

    freq = freq.lower()

    if freq.find('second') > 0:
        output = 1
    elif freq.find('minute') > 0:
        output = 2
    elif freq.find('hour') > 0:
        output = 3
    elif freq.find('day') > 0 and freq.find('week') == -1:
        output = 4
    elif freq.find('week') > 0:
        output = 5
    elif (freq.find('month') > 0 and freq.find('quarter') == -1 and
            freq.find('year') == -1):
        output = 6
    elif freq.find('quarter') > 0:
        output = 7
    elif freq.find('year') > 0:
        output = 8
    else:
        raise TypeError("Frequency not supported")

    return output


def base_freq(freq):
    if isinstance(freq, str):
        pass
    else:
        freq = str(freq)

    freq = freq.lower()

    if freq.find('second') > 0:
        output = 'second'
    elif freq.find('minute') > 0:
        output = 'minute'
    elif freq.find('hour') > 0:
        output = 'hour'
    elif freq.find('day') > 0:
        output = 'day'
    elif freq.find('week') > 0:
        output = 'week'
    elif freq.find('month') > 0:
        output = 'month'
    elif freq.find('quarter') > 0:
        output = 'quarter'
    elif freq.find('year') > 0:
        output = 'year'
    else:
        raise TypeError("Frequency not supported")

    return output


# Type check decorators
def dec_does_series_name_exist(func):
    def check_if_series_exist(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError:
            raise KeyError(f"'{kwargs['series_name']}' does not exist")

    return check_if_series_exist


def typecheck_datetime_like(param_name):
    index = (pandas.DatetimeIndex, pandas.PeriodIndex, pandas.TimedeltaIndex)
    index = {param_name: index}

    return typecheck(**index)


def typecheck(**kwtypes):
    # Does not work with callable/functions unless they're a class itself
    # Check to make sure all kwtypes values are actually types
    for pname in kwtypes.keys():
        types = kwtypes[pname]

        if isinstance(types, type):
            continue
        elif isinstance(types, tuple):
            for a_type in types:
                if isinstance(a_type, type) is False:
                    msg = "Parameter '{0}' typecheck value '{1}' is not a type"
                    msg = msg.format(pname, a_type)
                    raise TypeError(msg)
        else:
            msg = "Only 'type' or 'tuple[type]' are allowed in 'typecheck'"
            raise TypeError(msg)

    def check_instance(func):

        funcparams = inspect.signature(func)
        funcparams = [i.name for i in funcparams.parameters.values()]
        funcparams_np = enumerate(funcparams)
        funcparams_np = {param_name: i for i, param_name in funcparams_np}

        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            for param_name, expected_types in kwtypes.items():
                # breakpoint()
                # NOTE: This only captures user-provided arguments. If the
                # parameter has a default and the user doesn't override,
                # It will not show up
                param_was_provided = False
                try:

                    if param_name in kwargs:
                        value = kwargs[param_name]
                    else:
                        value = args[funcparams_np[param_name]]

                    param_was_provided = True

                except IndexError:
                    value = None

                if param_was_provided is True:
                    if isinstance(value, expected_types) is False:

                        if is_tuple_or_list(expected_types) is False:
                            ex_types = [expected_types]
                        else:
                            ex_types = expected_types

                        type_names = ['None' if el is None else el.__name__
                                      for el in ex_types]

                        if type_names:
                            type_names = ["'" + i + "'" for i in type_names]

                            if len(type_names) > 1:
                                type_names = (", ".join(type_names[:-1]) +
                                              ", or " + type_names[-1])
                            else:
                                # Should be just one type name
                                type_names = ", ".join(type_names)

                            msg = "'{0}' is not of type {1}"
                            msg = msg.format(param_name, type_names)
                        else:
                            msg = "'{0}' is note the correct type"
                            msg = msg.format(param_name)

                        raise TypeError(msg)
                else:
                    continue

            return func(*args, **kwargs)

        return func_wrapper

    return check_instance
