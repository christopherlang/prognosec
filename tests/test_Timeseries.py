import copy
import pytest
import pandas
import numpy
import dataseries
from progutils import progutils
from progutils import progexceptions
from progutils import progfunc


class TestTimeseries:

    @pytest.fixture
    def num_records(self):
        nrecs = 90
        return nrecs

    @pytest.fixture
    def series_name(self):
        name_of_series = 'x1'
        return name_of_series

    @pytest.fixture
    def index_name(self):
        name_of_index = 'date'
        return name_of_index

    @pytest.fixture
    def frequency_str(self):
        freqstr = 'D'
        return freqstr

    @pytest.fixture
    def data_sequence(self, num_records):
        data_series = numpy.array(range(num_records))
        return data_series

    @pytest.fixture
    def index_datetime(self, num_records, frequency_str, index_name):
        return pandas.date_range('2019-01-01', periods=num_records,
                                 freq=frequency_str, name=index_name)

    @pytest.fixture
    def index_period(self, num_records, frequency_str, index_name):
        return pandas.period_range('2019-01-01', periods=num_records,
                                   freq=frequency_str, name=index_name)

    @pytest.fixture
    def pandas_series_periodindex(self, data_sequence, index_period,
                                  series_name):
        return pandas.Series(data_sequence, index=index_period,
                             name=series_name)

    @pytest.fixture
    def timeseries_periodindex(self, pandas_series_periodindex):
        return dataseries.Timeseries(pandas_series_periodindex)

    def test_instantiation(self, pandas_series_periodindex):
        assert dataseries.Timeseries(series=pandas_series_periodindex)

    def test_has_series_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'series') is False:
            pytest.fail("Timeseries missing `series` property")

    def test_property_series_is_pandas_series(self, timeseries_periodindex):
        ts = timeseries_periodindex

        assert isinstance(ts.series, pandas.Series)

    def test_timseries_has_freq(self, timeseries_periodindex):
        assert timeseries_periodindex.series.index.freq is not None

    def test_timeseries_freq_type(self, timeseries_periodindex):
        ts = timeseries_periodindex

        assert isinstance(ts.series.index.freq, pandas.DateOffset)

    def test_convert_numpy_input(self, data_sequence, index_period,
                                 series_name):
        # Check if Timeseries converts `numpy.ndarray` input into
        # `pandas.Series`
        ts = dataseries.Timeseries(series=data_sequence,
                                   index=index_period,
                                   series_name=series_name)

        if isinstance(ts.series, pandas.Series) is False:
            msg = "Timeseries did not convert `numpy.ndarray` into "
            msg += "`pandas.Series`"
            pytest.fail(msg)

    def test_convert_list_input(self, data_sequence, index_period,
                                series_name):
        ts = dataseries.Timeseries(series=list(data_sequence),
                                   index=index_period,
                                   series_name=series_name)

        if isinstance(ts.series, pandas.Series) is False:
            msg = "Timeseries did not convert `list` into `pandas.Series`"
            pytest.fail(msg)

    def test_convert_tuple_input(self, data_sequence, index_period,
                                 series_name):
        ts = dataseries.Timeseries(series=tuple(data_sequence),
                                   index=index_period,
                                   series_name=series_name)

        if isinstance(ts.series, pandas.Series) is False:
            msg = "Timeseries did not convert `tuple` into `pandas.Series`"
            pytest.fail(msg)

    def test_list_input_raise_on_wrong_index(self, num_records, data_sequence):
        # Index must be of DatetimeIndex, PeriodIndex, TimedeltaIndex
        series = list(data_sequence)

        with pytest.raises(TypeError):
            dataseries.Timeseries(series=series, index=range(num_records))

    def test_nonpandas_series_raise_on_no_index(self, data_sequence):
        # If provided non pandas Series, then `index` parameter must be
        # supplied and of the right type
        series = list(data_sequence)

        with pytest.raises(progexceptions.IndexTypeError):
            dataseries.Timeseries(series=series)

    def test_has_index_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'index') is False:
            pytest.fail("Timeseries missing 'index' property")

    def test_has_supported_index_type(self, timeseries_periodindex):
        # indices should only be pandas `DatetimeIndex`, `PeriodIndex`, and
        # `TimedeltaIndex`
        assert progutils.is_time_index(timeseries_periodindex.index)

    def test_has_name_index_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'name_index') is False:
            pytest.fail("Timeseries missing 'name_index' property")

    def test_has_name_index_property_setter(self, timeseries_periodindex):
        ts = copy.deepcopy(timeseries_periodindex)

        try:
            ts.name_index = 'date'
        except AttributeError:
            pytest.fail("Timeseries does not have 'name_series' setter")

        assert ts.name_index == 'date'

    def test_name_index_property_must_be_string(self, index_period,
                                                data_sequence, series_name):

        index_period = copy.deepcopy(index_period)
        index_period.name = None
        with pytest.raises(progexceptions.IndexIntegrityError):
            # Because index is unnamed
            dataseries.Timeseries(data_sequence, index=index_period,
                                  series_name=series_name)

        with pytest.raises(TypeError):
            # Because the provided index name is not a string
            dataseries.Timeseries(data_sequence, index=index_period,
                                  series_name=series_name,
                                  index_name=tuple([1, 2]))

    def test_check_unique_index_values(self, index_period, data_sequence,
                                       frequency_str):
        # Indices MUST have unique values, otherwise joins will mess up
        index = index_period
        index = index.tolist()
        index[10] += 1  # Increment by one day. Should cause duplicates
        index = pandas.PeriodIndex(index, freq=frequency_str)

        with pytest.raises(progexceptions.IndexIntegrityError):
            dataseries.Timeseries(series=data_sequence, index=index)

    def test_has_name_series_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'name_series') is False:
            pytest.fail("Timeseries missing 'name_series' property")

    def test_has_name_series_property_setter(self, timeseries_periodindex):
        ts = copy.deepcopy(timeseries_periodindex)

        try:
            ts.name_series = 'x2'
        except AttributeError:
            pytest.fail("Timeseries does not have 'name_series' setter")

        assert ts.name_series == 'x2'

    def test_name_series_setter_type_check(self, timeseries_periodindex):
        ts = copy.deepcopy(timeseries_periodindex)

        with pytest.raises(TypeError):
            ts.name_series = 600

    def test_name_series_property_must_be_string(self, index_period,
                                                 data_sequence):
        with pytest.raises(TypeError):
            dataseries.Timeseries(data_sequence, index=index_period,
                                  series_name=tuple([3, 4]))

        with pytest.raises(TypeError):
            # pandas raises TypeError if setting of series name uses unhashable
            # types. Capture this and re-raise
            dataseries.Timeseries(data_sequence, index=index_period,
                                  series_name=set([3, 4]))

    def test_nonpandas_input_must_have_name(self, index_period, data_sequence):
        with pytest.raises(progexceptions.SeriesIntegrityError):
            dataseries.Timeseries(series=data_sequence, index=index_period)

    def test_pandas_input_must_have_name(self, index_period, data_sequence):
        ps = pandas.Series(data_sequence, index=index_period)

        with pytest.raises(progexceptions.SeriesIntegrityError):
            dataseries.Timeseries(series=ps)

    def test_has_freq_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'freq') is False:
            pytest.fail("Timeseries missing 'freq' property")

    def test_freq_property_correct_type(self, timeseries_periodindex):
        if isinstance(timeseries_periodindex.freq, pandas.DateOffset) is False:
            pytest.fail("'freq' property not the right type")

    def test_pandas_input_must_have_freq(self, index_datetime, data_sequence,
                                         series_name):
        index = copy.deepcopy(index_datetime)
        index.freq = None

        pseries = pandas.Series(data_sequence, index=index, name=series_name)

        with pytest.raises(progexceptions.IndexIntegrityError):
            dataseries.Timeseries(pseries)

    def test_nonpandas_input_must_have_freq(self, index_datetime,
                                            data_sequence, series_name):
        index = copy.deepcopy(index_datetime)
        index.freq = None

        with pytest.raises(progexceptions.IndexIntegrityError):
            dataseries.Timeseries(series=data_sequence, index=index,
                                  series_name=series_name)

    def test_has_value_series_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'value_series') is False:
            pytest.fail("TImeseries missing 'value_series' property")

    def test_value_series_property_correct_type(self, timeseries_periodindex):
        assert isinstance(timeseries_periodindex.value_series, numpy.ndarray)

    def test_value_series_property_return_correct_values(
            self, timeseries_periodindex):
        series_property = timeseries_periodindex.value_series
        series_direct = timeseries_periodindex._series.to_numpy(copy=True)

        assert list(series_property) == list(series_direct)

    def test_value_series_property_return_copy(self, timeseries_periodindex):
        # Pulling private variable as 'series' property will bypass
        # transformations later
        new_value_for_i = 6758

        series_array = timeseries_periodindex.value_series
        series_array[13] = new_value_for_i

        # If what is returned is a copy, editing outside should not change
        # inside Timeseries
        assert timeseries_periodindex._series[13] != new_value_for_i

    def test_has_value_index_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'value_index') is False:
            pytest.fail("TImeseries missing 'value_index' property")

    def test_value_index_property_return_correct_values(
            self, timeseries_periodindex):
        index_value_property = timeseries_periodindex.value_index
        index_value_direct = timeseries_periodindex.index.values

        assert list(index_value_property) == list(index_value_direct)

    def test_value_index_property_return_copy(self, timeseries_periodindex):
        # Pulling private variable as 'series' property will bypass
        # transformations later
        new_value_for_i = 6758

        series_array = timeseries_periodindex.value_index
        series_array[13] = new_value_for_i

        # If what is returned is a copy, editing outside should not change
        # inside Timeseries
        assert timeseries_periodindex._series.index[13] != new_value_for_i

    def test_has_dtype_property(self, timeseries_periodindex):
        if hasattr(timeseries_periodindex, 'dtype') is False:
            pytest.fail("TImeseries missing 'dtype' property")

    def test_dtype_property_correct_type(self, timeseries_periodindex):
        assert isinstance(timeseries_periodindex.dtype, numpy.dtype)

    def test_dtype_property_returning_correct_type_vs_original(
            self, timeseries_periodindex, pandas_series_periodindex):
        assert timeseries_periodindex.dtype == pandas_series_periodindex.dtype

    def test_has_dtype_index_property(self, timeseries_periodindex):
        assert hasattr(timeseries_periodindex, 'dtype_index')

    def test_dtype_index_property_returning_correct_type_vs_original(
            self, timeseries_periodindex, pandas_series_periodindex):
        assert (timeseries_periodindex.dtype_index ==
                pandas_series_periodindex.index.dtype)

    def test_verify_method_check_index_duplicate(self, data_sequence,
                                                 index_period):
        index = copy.deepcopy(index_period).to_numpy(copy=True)
        index[10] = index[11]
        index = pandas.PeriodIndex(index, name='date')

        pseries = pandas.Series(data_sequence, index=index, name='x2')

        with pytest.raises(progexceptions.IndexIntegrityError):
            dataseries.Timeseries._verify_new_series(series=pseries)

    def test_verify_method_check_index_has_name(self, data_sequence,
                                                index_period):
        index = copy.deepcopy(index_period)
        index.name = None

        pseries = pandas.Series(data_sequence, index=index, name='x2')

        with pytest.raises(progexceptions.IndexIntegrityError):
            dataseries.Timeseries._verify_new_series(series=pseries)

    def test_verify_method_check_series_has_name(self, data_sequence,
                                                 index_period):

        pseries = pandas.Series(data_sequence, index=index_period)

        with pytest.raises(progexceptions.SeriesIntegrityError):
            dataseries.Timeseries._verify_new_series(series=pseries)

    def test_verify_method_check_series_is_str(self, data_sequence,
                                               index_period):

        pseries = pandas.Series(data_sequence, index=index_period,
                                name=tuple())

        with pytest.raises(progexceptions.SeriesIntegrityError):
            dataseries.Timeseries._verify_new_series(series=pseries)

    def test_has_strat_na_property(self, timeseries_periodindex):
        assert hasattr(timeseries_periodindex, 'strat_na')

    def test_strat_na_correct_methods(self, data_sequence, index_period):
        dat = copy.deepcopy(data_sequence)
        dat = dat.astype(numpy.float64)  # numpy.nan is a float
        dat[10] = numpy.nan
        dat[15] = None

        for a_method in dataseries.NAN_STR_METHODS + (None,):
            ts = dataseries.Timeseries(dat, index=index_period,
                                       series_name='x1',
                                       strat_na=a_method)

            if a_method == 'asis' or a_method is None:
                # These methods should leave NaN in
                assert ts.series.hasnans
            else:
                # bfill will fail if last value is NaN
                assert ts.series.hasnans is False

    def test_stra_na_invalid_set_method(self, timeseries_periodindex):
        with pytest.raises(progexceptions.ComputeMethodError):
            timeseries_periodindex.strat_na = 'hey there'

        with pytest.raises(progexceptions.ComputeMethodError):
            timeseries_periodindex.strat_na = numpy.mean

    def test_strat_na_functions(self, data_sequence, index_period):
        dat = copy.deepcopy(data_sequence)
        dat = dat.astype(numpy.float64)  # numpy.nan is a float
        dat[10] = numpy.nan
        dat[15] = None

        ts = dataseries.Timeseries(dat, index_period, series_name='x1')
        ts.strat_na = progfunc.fillnan_aggregate(progfunc.agg_median())

        fill_val = progfunc.agg_median()(dat)

        assert ts.series[10] == fill_val
        assert ts.series[15] == fill_val

    def test_has_strat_up_property(self, timeseries_periodindex):
        assert hasattr(timeseries_periodindex, 'strat_up')

    def test_strat_up_raise_on_wrong_method(self, timeseries_periodindex):
        with pytest.raises(progexceptions.ComputeMethodError):
            timeseries_periodindex.strat_up = 'hello'

    def test_strat_up_correct_methods(self, timeseries_periodindex):
        ts = copy.deepcopy(timeseries_periodindex)
        methods = dataseries.UPSAMPLE_STR_APPLY_METHODS
        methods += dataseries.UPSAMPLE_STR_INTER_METHODS

        for a_method in methods + (None,):
            try:
                ts.strat_up = a_method
            except progexceptions.ComputeMethodError:
                pytest.fail(f"method '{a_method}' does not work")

    def test_has_strat_down_property(self, timeseries_periodindex):
        assert hasattr(timeseries_periodindex, 'strat_down')

    def test_strat_down_raise_on_wrong_method(self, timeseries_periodindex):
        with pytest.raises(progexceptions.ComputeMethodError):
            timeseries_periodindex.strat_down = 'hello'

    def test_strat_down_correct_methods(self, timeseries_periodindex):
        ts = copy.deepcopy(timeseries_periodindex)
        methods = (progfunc.sampledown_mean(), progfunc.sampledown_median(),
                   progfunc.sampledown_max(), progfunc.sampledown_min())

        for a_method in methods + (None,):
            try:
                ts.strat_down = a_method
            except progexceptions.ComputeMethodError:
                pytest.fail(f"method '{a_method}' does not work")

    def test_has_transform_property(self, timeseries_periodindex):
        assert hasattr(timeseries_periodindex, 'transform')

    def test_cast_method(self, timeseries_periodindex):
        ts = timeseries_periodindex
        ts = ts.cast(float)

        assert ts.dtype == numpy.float
        assert timeseries_periodindex.dtype != numpy.float

    def test_has_resample_method(self, timeseries_periodindex):
        assert hasattr(timeseries_periodindex, 'resample')

    def test_downsampling(self, timeseries_periodindex):
        ts = timeseries_periodindex

        # ts should be day frequency. Try higher ones here
        for freq in ('M', 'W', 'Q'):
            assert isinstance(ts.resample(freq), dataseries.Timeseries)

    def test_has_strat_inf_property(self, timeseries_periodindex):
        assert hasattr(timeseries_periodindex, 'strat_inf')

    def test_strat_inf_correct_methods(self, timeseries_periodindex):
        ts = copy.deepcopy(timeseries_periodindex)
        methods = dataseries.INF_STR_METHODS + (None,)

        for a_method in methods:
            try:
                ts.strat_inf = a_method
            except progexceptions.ComputeMethodError:
                pytest.fail(f"'{a_method}' did not get assigned")

        with pytest.raises(progexceptions.ComputeMethodError):
            ts.strat_inf = numpy.mean

        with pytest.raises(progexceptions.ComputeMethodError):
            ts.strat_inf = set([5, 6])

    def test_is_inf_method(self, data_sequence, index_period):
        dat = copy.deepcopy(data_sequence)
        dat = dat.astype(numpy.float64)  # numpy.nan is a float
        dat[10] = numpy.inf
        dat[15] = numpy.inf

        ts = dataseries.Timeseries(dat, index_period, series_name='x1')

        assert ts.is_inf(after_clean=False).any()
        assert not ts.is_inf(after_clean=True).any()
        assert ts.size_inf(after_clean=False) == 2
        assert ts.size_inf(after_clean=True) == 0

    def test_transform_property_set_type_check(self, timeseries_periodindex):
        with pytest.raises(TypeError):
            timeseries_periodindex.transform = 500
