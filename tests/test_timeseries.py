import unittest
import timeseries
import pandas
import numpy
import functions


class TestExogFrame(unittest.TestCase):

    def setUp(self):
        self.nrecs = 151
        self.dataframe = pandas.DataFrame.from_dict({
            'x1': list(range(self.nrecs)),
            'x2': pandas.util.testing.rands_array(10, self.nrecs),
            'x3': numpy.random.uniform(size=self.nrecs),
            'x4': numpy.random.randint(2, size=self.nrecs)
        })
        self.dataframe.index = pandas.bdate_range('2019-01-01',
                                                  periods=self.nrecs)

        self.exogframe = timeseries.ExogFrame(self.dataframe)

    def test_exogseries_storage(self):
        self.assertIsInstance(self.exogframe.exogseries, dict)
        self.assertEqual(self.exogframe.nseries, 4)

        colnames = zip(self.exogframe.exogseries_names, self.dataframe.columns)
        self.assertTrue(all([i == j for i, j in colnames]))

        self.exogframe.resample('MS')

    def test_downsampling(self):
        self.exogframe.resample('MS')

    def test_upsampling(self):
        self.exogframe.resample('W')

    def test_downsampling_mean_var_x3(self):
        origseries = self.exogframe.get_series('x3').series
        execresult = self.exogframe.resample('MS', method=numpy.mean)
        execresult = execresult.get_series('x3').series

        expected_jan = origseries.loc['2019-01-01':'2019-01-31'].mean()
        expected_feb = origseries.loc['2019-02-01':'2019-02-28'].mean()
        expected_mar = origseries.loc['2019-03-01':'2019-03-31'].mean()

        resampled_jan = execresult.loc['2019-01-01']
        resampled_feb = execresult.loc['2019-02-01']
        resampled_mar = execresult.loc['2019-03-01']

        self.assertAlmostEqual(resampled_jan, expected_jan, places=6)
        self.assertAlmostEqual(resampled_feb, expected_feb, places=6)
        self.assertAlmostEqual(resampled_mar, expected_mar, places=6)

    def test_upsampling_ffill_var_x4(self):
        origseries = self.exogframe.get_series('x4').series
        execresult = self.exogframe.resample('H', method='ffill')
        execresult = execresult.get_series('x4').series

        expected_201901 = origseries.loc['2019-01-15']
        expected_201902 = origseries.loc['2019-02-11']
        expected_201903 = origseries.loc['2019-03-18']

        resampled_jan = execresult.loc['2019-01-15 06:00:00']
        resampled_feb = execresult.loc['2019-02-11 02:00:00']
        resampled_mar = execresult.loc['2019-03-18 09:00:00']

        self.assertAlmostEqual(resampled_jan, expected_201901, places=6)
        self.assertAlmostEqual(resampled_feb, expected_201902, places=6)
        self.assertAlmostEqual(resampled_mar, expected_201903, places=6)

    def test_downsampling_single_series_x1(self):
        origseries = self.exogframe.get_series('x1').series
        execresult = self.exogframe.resample('MS', method={'x1': 'ffill'})

        self.assertGreater(origseries.size, execresult.get_series('x1').size)

        execresult = self.exogframe.resample('MS', series='x1',
                                             method={'x1': 'ffill'})
        self.assertEqual(self.exogframe.get_series('x2').size,
                         execresult.get_series('x2').size)


class TestExogSeries(unittest.TestCase):

    def setUp(self):
        self.series = list(range(151))
        self.series = pandas.Series(self.series, name='random-series')
        self.series.index = pandas.bdate_range(
            '2019-01-01',
            periods=len(self.series))

        self.exogseries = timeseries.ExogSeries(self.series)

        ss_index = pandas.date_range('2019-01-01', periods=90)

        self.ss_float = numpy.arange(0, 30, step=30 / 90, dtype=numpy.float64)
        self.ss_float = pandas.Series(self.ss_float, ss_index, name='series')
        self.exog_ss_float = timeseries.ExogSeries(self.ss_float)

    def test_series_type(self):
        errmsg = "Stored series should be pandas.Series"
        self.assertIsInstance(self.exogseries.series, pandas.Series, errmsg)

    def test_series_name(self):
        errmsg = "Series name in ExogSeries different than original"
        self.assertEqual(self.exogseries.series_name, self.series.name, errmsg)

    def test_index_type(self):
        errmsg = 'Series type must be pandas.DatatimeIndex'
        self.assertIsInstance(self.exogseries.index_type, pandas.DatetimeIndex,
                              errmsg)

    def test_series_dtype(self):
        errmsg = 'ExogSeries and original series is not the same'
        self.assertIsInstance(self.exogseries.dtype, numpy.dtype, errmsg)

        errmsg = 'ExogSeries not returning the expected data type'
        self.assertEqual(self.exogseries.dtype.kind, 'i', errmsg)

    def test_series_casting(self):
        new_exogseries = self.exogseries.copy()
        new_exogseries.cast_series('float')

        errmsg = 'Not expected for original ExogSeries dtype to change'
        self.assertIsInstance(self.exogseries.dtype, numpy.dtype, errmsg)
        self.assertEqual(self.exogseries.dtype.kind, 'i', errmsg)

        errmsg = 'Casted series is not of expected type'
        self.assertIsInstance(new_exogseries.dtype, numpy.dtype, errmsg)
        self.assertEqual(new_exogseries.dtype.kind, 'f', errmsg)

        new_exogseries.cast_series(str)
        self.assertIsInstance(new_exogseries.dtype, numpy.dtype, errmsg)
        self.assertEqual(new_exogseries.dtype.kind, 'O', errmsg)

        self.assertIsInstance(self.exogseries.dtype, numpy.dtype, errmsg)

    def test_na_check(self):
        self.assertFalse(self.exogseries.has_na(), 'Should not have any NaN')
        errmsg = "ExogSeries(na_strategy='asis') is not leaving asis"
        self.assertEqual(self.exogseries.size, self.series.size, errmsg)

        new_exogseries = self.exogseries.copy()
        new_exogseries.cast_series(float)
        new_exogseries.set_value(1, numpy.nan)
        self.assertTrue(new_exogseries.has_na(), 'NaN not detected')
        self.assertGreater(new_exogseries.size_na(), 0, 'NaN not detected')

    def test_na_strategy(self):
        new_exogseries = self.exogseries.copy()
        new_exogseries.cast_series(float)
        new_exogseries.set_value(1, numpy.nan)

        new_exogseries = timeseries.ExogSeries(new_exogseries.series, 'drop')

        errmsg = "ExogSeries(na_strategy='drop') is not dropping NaN"
        self.assertGreater(len(self.series), new_exogseries.size, errmsg)

    def test_series_naming(self):
        self.assertEqual(self.exogseries.series_name, 'random-series')
        new_exogseries = self.exogseries.copy()
        new_exogseries.series_name = 'new-series-name'
        self.assertEqual(new_exogseries.series_name, 'new-series-name')
        self.assertEqual(self.exogseries.series_name, 'random-series')

    def test_resampling_size(self):
        self.assertEqual(self.exog_ss_float.size, 90, '# of days not expected')
        self.assertEqual(self.exog_ss_float.frequency.name, 'D',
                         'Not daily frequency')

        self.assertIsInstance(self.exog_ss_float.resample('M'),
                              timeseries.ExogSeries)

        # Expecting three months
        self.assertEqual(self.exog_ss_float.resample('MS').size, 3)

        # Expecting 13 weeks, techincally 12 weeks and 5 days
        self.assertEqual(self.exog_ss_float.resample('W').size, 13)

    def test_downsampling_values(self):
        # By default, the filling strategy for resample is 'ffill'. This is
        # ok if downsampling, but upsampling might not make sense here
        # here we downsample and take mean
        expected_jan = (self.exog_ss_float.series
                        .loc['2019-01-01':'2019-01-31'].mean())
        expected_feb = (self.exog_ss_float.series
                        .loc['2019-02-01':'2019-02-28'].mean())
        expected_mar = (self.exog_ss_float.series
                        .loc['2019-03-01':'2019-03-31'].mean())

        resampled_series = self.exog_ss_float.resample('MS', numpy.mean).series
        resampled_jan = resampled_series.loc['2019-01-01']
        resampled_feb = resampled_series.loc['2019-02-01']
        resampled_mar = resampled_series.loc['2019-03-01']

        self.assertAlmostEqual(resampled_jan, expected_jan, places=6)
        self.assertAlmostEqual(resampled_feb, expected_feb, places=6)
        self.assertAlmostEqual(resampled_mar, expected_mar, places=6)

        # here we downsample and take max
        expected_jan = (self.exog_ss_float.series
                        .loc['2019-01-01':'2019-01-31'].max())
        expected_feb = (self.exog_ss_float.series
                        .loc['2019-02-01':'2019-02-28'].max())
        expected_mar = (self.exog_ss_float.series
                        .loc['2019-03-01':'2019-03-31'].max())

        resampled_series = self.exog_ss_float.resample('MS', numpy.max).series
        resampled_jan = resampled_series.loc['2019-01-01']
        resampled_feb = resampled_series.loc['2019-02-01']
        resampled_mar = resampled_series.loc['2019-03-01']

        self.assertAlmostEqual(resampled_jan, expected_jan, places=6)
        self.assertAlmostEqual(resampled_feb, expected_feb, places=6)
        self.assertAlmostEqual(resampled_mar, expected_mar, places=6)

        # here we downsample and take median
        expected_jan = (self.exog_ss_float.series
                        .loc['2019-01-01':'2019-01-31'].median())
        expected_feb = (self.exog_ss_float.series
                        .loc['2019-02-01':'2019-02-28'].median())
        expected_mar = (self.exog_ss_float.series
                        .loc['2019-03-01':'2019-03-31'].median())

        resampled_series = self.exog_ss_float.resample('MS', numpy.median)
        resampled_series = resampled_series.series
        resampled_jan = resampled_series.loc['2019-01-01']
        resampled_feb = resampled_series.loc['2019-02-01']
        resampled_mar = resampled_series.loc['2019-03-01']

        self.assertAlmostEqual(resampled_jan, expected_jan, places=6)
        self.assertAlmostEqual(resampled_feb, expected_feb, places=6)
        self.assertAlmostEqual(resampled_mar, expected_mar, places=6)

    def test_upsampling_values(self):
        # here we upsample (hourly) and forward fill
        expected_201901 = self.exog_ss_float.series.loc['2019-01-15']
        expected_201902 = self.exog_ss_float.series.loc['2019-02-10']
        expected_201903 = self.exog_ss_float.series.loc['2019-03-16']

        resampled_series = self.exog_ss_float.resample('H', 'ffill').series
        resampled_jan = resampled_series.loc['2019-01-15 06:00:00']
        resampled_feb = resampled_series.loc['2019-02-10 02:00:00']
        resampled_mar = resampled_series.loc['2019-03-16 09:00:00']

        self.assertAlmostEqual(resampled_jan, expected_201901, places=6)
        self.assertAlmostEqual(resampled_feb, expected_201902, places=6)
        self.assertAlmostEqual(resampled_mar, expected_201903, places=6)


if __name__ == '__main__':
    unittest.main()
