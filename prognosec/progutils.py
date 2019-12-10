import collections
import pandas
import numpy
import copy


class Transformation:
    """Ordered storage of array transformations

    A transformation maintains a sequence of functions (aka procedure) for
    re-usable application of a sequence of transformation on a given array.


    Attributes
    ----------
    procedure : list[function], empty list
        A list of functions to be applied to a data series. Functions are
        applied in order of the list
    size : int
        The number of functions stored
    empty : bool
        Whether there any functions. If zero, there are none, and data series
        passed to `apply` will be passed back
    """

    def __init__(self):
        self._procedure = list()

    @property
    def procedure(self):
        return self._procedure

    @procedure.setter
    def procedure(self, list_of_func):
        if isinstance(list_of_func, list) is not True:
            raise TypeError("'list_of_func' should be a list")

        if callable_sequence(list_of_func) is not True:
            raise TypeError("'list_of_func' should be a list of functions")

        self._procedure = list_of_func

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

    def get(self, index):
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
        self.procedure.clear()

    def apply(self, data_series):
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
    return isinstance(x, (tuple, list, set, numpy.ndarray))


def is_tuple_or_list(x):
    return isinstance(x, (tuple, list))


def isinstance_sequence(x, obj_type):
    return all([isinstance(i, obj_type) for i in x])


def callable_sequence(x):
    return all([callable(i) for i in x])
