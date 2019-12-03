import unittest
import timeseries
import pandas
import numpy


class TestExogSeries(unittest.TestCase):

    def setUp(self):
        self.series = list(range(151))
        self.series = pandas.Series(self.series, name='random-series')
        self.series.index = pandas.bdate_range(
            '2019-01-01',
            periods=len(self.series))

        self.exogseries = timeseries.ExogSeries(self.series)

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

    # def test_resample_strategy(self):


if __name__ == '__main__':
    unittest.main()
