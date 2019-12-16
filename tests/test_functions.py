import numpy
import pytest
import pandas
from progutils import progexceptions
from progutils import progutils
from progutils import progfunc


class TestTransShiftFunction:
    array_size_range = range(1, 100)
    shift_size_range = range(-100, 100)

    @pytest.fixture
    def trans_shift(self):
        return progfunc.trans_shift

    @pytest.yield_fixture
    def arrays(self):
        def array_generator():
            for a_size in TestTransShiftFunction.array_size_range:
                yield numpy.arange(a_size, dtype='int')

        yield array_generator()

    @pytest.yield_fixture
    def shift_sizes(self):
        def size_generator():
            for s_size in TestTransShiftFunction.shift_size_range:
                yield s_size

        yield size_generator()

    @pytest.yield_fixture
    def shift_direction(self):
        def direction_generator():
            for i in [1, -1]:
                yield i

        yield direction_generator()

    def test_exists(self):
        assert hasattr(progfunc, 'trans_shift')

    def test_shifting_output_size(self, trans_shift, arrays, shift_sizes,
                                  shift_direction):
        for an_array in arrays:
            for a_size in shift_sizes:
                for direction in shift_direction:
                    trans_dat = trans_shift(shift=a_size * direction)(an_array)

                    assert len(trans_dat) == len(an_array)

    def test_shifting_nan_count(self, trans_shift, arrays, shift_sizes,
                                shift_direction):
        for an_array in arrays:
            for s_size in shift_sizes:
                for direction in shift_direction:
                    trans_dat = trans_shift(shift=s_size * direction)(an_array)

                    if len(an_array) == 0:
                        expected_nan_count = 0
                    elif numpy.abs(s_size) >= len(an_array):
                        expected_nan_count = len(an_array)
                    else:
                        expected_nan_count = numpy.abs(s_size) - 1

                    assert numpy.isnan(trans_dat).sum() == expected_nan_count

    def test_pandas_input(self, trans_shift, shift_sizes, shift_direction):
        for a_size in TestTransShiftFunction.array_size_range:
            index = pandas.period_range('2019-01-01', periods=a_size,
                                        name='date')
            og_series = pandas.Series(range(a_size), index=index, name='x1')

            for s_size in shift_sizes:
                for direction in shift_direction:
                    trans_dat = trans_shift(shift=s_size * direction)
                    trans_dat = trans_dat(og_series)

                    if len(og_series) == 0:
                        expected_nan_count = 0
                    elif numpy.abs(s_size) >= len(og_series):
                        expected_nan_count = len(og_series)
                    else:
                        expected_nan_count = numpy.abs(s_size) - 1

                    assert numpy.isnan(trans_dat).sum() == expected_nan_count

                    assert isinstance(trans_dat, pandas.Series)
                    assert trans_dat.name == 'x1'
                    assert trans_dat.index.name == 'date'
