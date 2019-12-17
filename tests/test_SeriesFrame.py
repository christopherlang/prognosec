import pytest
import collections
import copy
import numpy
import string
import pandas
import dataseries
from progutils import progutils
from progutils import progfunc


class TestSeriesFrame:
    nrecs = 10

    @pytest.fixture
    def index(self):
        the_index = pandas.period_range(
            '2019-01-01', periods=TestSeriesFrame.nrecs, freq='D', name='date'
        )

        return the_index

    @pytest.fixture
    def index_dt(self):
        the_index = pandas.bdate_range(
            '2019-01-01', periods=TestSeriesFrame.nrecs, freq='D', name='date'
        )

        return the_index

    @pytest.fixture
    def pandas_df(self, index):
        df = {
            'x1': range(TestSeriesFrame.nrecs),
            'x2': string.ascii_letters[:TestSeriesFrame.nrecs],
            'x3': numpy.arange(1, 2, 1 / TestSeriesFrame.nrecs)
        }

        output = pandas.DataFrame.from_dict(df)
        output.index = index

        return output

    @pytest.fixture
    def new_timeseries(self, index):
        series = list(range(TestSeriesFrame.nrecs))
        series = progfunc.trans_inverse(add=1)(series)
        series = pandas.Series(series, index=index, name='x4')

        return dataseries.Timeseries(series)

    @pytest.fixture
    def sf_pdf(self, pandas_df):
        return dataseries.SeriesFrame(df=pandas_df)

    def test_seriesframe_instantiation_type_check(self, pandas_df):
        with pytest.raises(TypeError):
            dataseries.SeriesFrame(df=50)

        with pytest.raises(TypeError):
            dataseries.SeriesFrame(df=pandas_df, index_name=50)

        with pytest.raises(TypeError):
            dataseries.SeriesFrame(df=pandas_df, index=50, index_name='x10')

    def test_has_frame_property(self, sf_pdf):
        assert hasattr(sf_pdf, 'frame')

    def test_has_split_dataframe_property(self, sf_pdf):
        assert hasattr(sf_pdf, '_split_dataframe')

    def test_frame_property_is_ordereddict(self, sf_pdf):
        assert isinstance(sf_pdf.frame, collections.OrderedDict)

    def test_frame_propert_all_timeseries(self, sf_pdf):
        test_result = all([isinstance(i, dataseries.Timeseries)
                           for i in sf_pdf.frame.values()])
        assert test_result

    def test_has_size_property(self, sf_pdf):
        assert hasattr(sf_pdf, 'size')

    def test_size_property_correct_type(self, sf_pdf):
        assert isinstance(sf_pdf.size, int)

    def test_size_property_correct_results(self, sf_pdf, pandas_df):
        assert sf_pdf.size == len(pandas_df.columns)

    def test_has_index_property(self, sf_pdf):
        assert hasattr(sf_pdf, 'index')

    def test_index_property_correct_type(self, sf_pdf):
        assert progutils.is_time_index(sf_pdf.index)

    def test_setting_index_property(self, sf_pdf, index_dt):
        try:
            sf_pdf.index = index_dt
        except AttributeError:
            pytest.fail("index property doesn't allow setting")

        with pytest.raises(TypeError):
            sf_pdf.index = pandas.RangeIndex(0, 9)

    def test_has_name_index(self, sf_pdf):
        assert isinstance(sf_pdf.name_index, str)

    def test_setting_name_index_property(self, sf_pdf, index_dt):
        try:
            sf_pdf.index = index_dt
            sf_pdf.name_index = "datetime"
        except AttributeError:
            pytest.fail("name_index property doesn't allow setting")

        with pytest.raises(TypeError):
            sf_pdf.name_index = 600

    def test_has_freq_property(self, sf_pdf):
        assert hasattr(sf_pdf, 'freq')

    def test_freq_property_correct_type(self, sf_pdf):
        assert isinstance(sf_pdf.freq, pandas.DateOffset)

    def test_has_value_index_property(self, sf_pdf):
        assert hasattr(sf_pdf, 'value_index')

    def test_dtype_index_property(self, sf_pdf):
        assert hasattr(sf_pdf, 'dtype_index')

    def test_has_add_method(self, sf_pdf, new_timeseries):
        try:
            sf_pdf.add(new_timeseries)
        except AttributeError:
            pytest.fail("SeriesFrame missing add method")

    def test_add_method_name_must_be_str(self, sf_pdf, new_timeseries):
        with pytest.raises(TypeError):
            # Should fail if series_name is
            sf_pdf.add(new_timeseries, series_name=500)

    def test_add_method_fails_on_wrong_type(self, sf_pdf):
        with pytest.raises(TypeError):
            sf_pdf.add([1, 2, 3, 4, 5])

    def test_add_method_fails_wrong_name_type(self, sf_pdf, new_timeseries):
        with pytest.raises(TypeError):
            sf_pdf.add(new_timeseries, series_name=50)

    def test_add_method_name_is_set(self, sf_pdf, new_timeseries):
        sf_pdf.add(new_timeseries)

        try:
            sf_pdf.frame['x4']
        except KeyError:
            msg = "Added timeseries not having expected key name"
            pytest.fail(msg)

        assert sf_pdf.frame['x4'].name_series == 'x4'

        sf_pdf.add(new_timeseries, series_name='x5')

        try:
            sf_pdf.frame['x5']
        except KeyError:
            msg = "Added timeseries not having expected key name"
            pytest.fail(msg)

        assert sf_pdf.frame['x5'].name_series == 'x5'

    def test_add_method_raise_on_series_existing(self, sf_pdf, new_timeseries):
        sf_pdf.add(new_timeseries)

        series = list(range(TestSeriesFrame.nrecs + 10))
        index = pandas.period_range(
            '2019-01-01', periods=TestSeriesFrame.nrecs + 10, freq='D',
            name='date'
        )
        series = progfunc.trans_inverse(add=1)(series)
        series = pandas.Series(series, index=index, name='x4')

        with pytest.raises(TypeError):
            sf_pdf.add(series)

    def test_has_remove_method(self, sf_pdf):
        assert hasattr(sf_pdf, 'remove')

    def test_remove_method_removing_series(self, sf_pdf):
        try:
            sf_pdf.remove('x2')
        except KeyError:
            pytest.fail("Failed to remove existing series by name")

    def test_remove_method_fail_nonexisting_series(self, sf_pdf):
        with pytest.raises(KeyError):
            sf_pdf.remove('x100')

    def test_remove_method_actually_removing_series(self, sf_pdf):
        current_size = sf_pdf.size
        expected_size = current_size - 1

        sf_pdf.remove('x3')

        assert sf_pdf.size == expected_size

    def test_has_replace_method(self, sf_pdf):
        assert hasattr(sf_pdf, 'replace')

    def test_replace_method_replacing_series(self, sf_pdf, new_timeseries,
                                             pandas_df):
        og_x3_series = sf_pdf.frame['x3'].series

        sf_pdf.replace(new_timeseries, 'x3')

        cseries = sf_pdf.frame['x3'].series
        nseries = new_timeseries.series

        try:
            pandas.testing.assert_series_equal(cseries, nseries)
        except AssertionError:
            pytest.fail("Series was not replaced correctly")

        with pytest.raises(AssertionError):
            pandas.testing.assert_series_equal(cseries, og_x3_series)

        assert sf_pdf.size == len(pandas_df.columns)

    def test_replace_method_series_type_check(self, sf_pdf):
        new_series = pandas.Series(range(1000), name='hello')

        with pytest.raises(TypeError):
            sf_pdf.replace(new_series, series_name='x6')

    def test_replace_method_name_type_check(self, sf_pdf, new_timeseries):
        with pytest.raises(TypeError):
            sf_pdf.replace(new_timeseries, series_name=100)

    def test_has_access_series_method(self, sf_pdf):
        assert hasattr(sf_pdf, 'access_series')

    def test_access_series_getting_right_series(self, sf_pdf, pandas_df):
        assert isinstance(sf_pdf.access_series('x1'), dataseries.Timeseries)

        cseries = sf_pdf.access_series('x1').series
        oseries = pandas_df['x1']

        try:
            pandas.testing.assert_series_equal(cseries, oseries)
        except AssertionError:
            pytest.fail("Not accessing the right series")

    def test_access_series_fail_on_wrong_series_name(self, sf_pdf):
        with pytest.raises(KeyError):
            sf_pdf.access_series('x7')

    def test_access_series_direct_manipulation(self, sf_pdf):
        sf_pdf.access_series('x1')._series[6] = 100

        assert sf_pdf.access_series('x1').series[6] == 100

    def test_has_access_transform_method(self, sf_pdf):
        assert hasattr(sf_pdf, 'access_transform')

    def test_access_transform_getting_right_obj(self, sf_pdf):
        assert isinstance(sf_pdf.access_transform('x1'),
                          progutils.Transformation)

    def test_has_access_strat_na_method(self, sf_pdf):
        assert hasattr(sf_pdf, 'access_strat_na')

    def test_has_access_strat_up_method(self, sf_pdf):
        assert hasattr(sf_pdf, 'access_strat_up')

    def test_has_access_strat_down_method(self, sf_pdf):
        assert hasattr(sf_pdf, 'access_strat_down')

    def test_drop_method_type_check(self, sf_pdf):
        with pytest.raises(TypeError):
            sf_pdf.drop(10)

    # def test_has_dtype_index_property(self, sf_pdf):
    #     assert isinstance(sf_pdf.dtype_index, numpy.dtype)
