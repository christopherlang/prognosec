import pandas
import plotly.express as px
import pandas_market_calendars as mcal
import sklearn.model_selection
import copy
import collections


ONEBDAY = pandas.tseries.offsets.BusinessDay(1)
CAL_NASDAQ = mcal.get_calendar('NASDAQ')
CAL_NYSE = mcal.get_calendar('NYSE')


class Timeseries:
    """Time series dataset

    `Timeseries` wraps a `pandas.DataFrame` object that houses the actual
    time series data. It is primarily designed to simplify common analysis
    requirements of time series data through methods, as well as standardizing
    input/output functionalities.

    Datetime frequency is strictly enforced through the requirement of the
    presence of a `pandas.DatetimeIndex`. This requirement should help with
    methods that can group by time for differing period sizes.

    Attributes
    ----------
    X
    primary_series
    periods
    index_name
    series
    frequency
    timezone

    Parameters
    ----------
    X : pandas.DataFrame
        A data frame that houses the time series data. Must have an index that
        is `DatetimeIndex` and must have its frequency set.
    primary_series : str, optional
        The name of the primary series. Many time series can have multiple
        series. This enables quicker selection of the series for plotting
        and modeling.
    """

    def __init__(self, timeseries, primary_series=None,
                 calendar=None, na_strategy='ffill', enable_transforms=False,
                 exog=None):
        if isinstance(timeseries, pandas.DataFrame) is not True:
            raise TypeError("`timeseries` needs to be a `pandas.DataFrame`")

        self._calendar = calendar

        if calendar is not None:
            index_name = timeseries.index.name
            # TODO this causes NaN for all columns IF original
            # DatetimeIndex has a time component
            timeseries_dates = self._calendar.schedule(timeseries.index.min(),
                                                       timeseries.index.max())
            timeseries = timeseries.reindex(index=timeseries_dates.index)
            timeseries.index.name = index_name

        ts = timeseries.copy(deep=True)
        self._timeseries = self._execute_na_strategy(ts, na_strategy)
        self._verify(self._timeseries, primary_series)
        self._primary_series = primary_series
        self._na_strategy = na_strategy

        self._enable_transforms = enable_transforms
        self._transformations = Transformations(self.series)

        self._exog = exog

    @property
    def transformations(self):
        return self._transformations

    @property
    def exog(self):
        return self._exog

    @property
    def enable_transforms(self):
        return self._enable_transforms

    @enable_transforms.setter
    def enable_transforms(self, should_enable):
        self._enable_transforms = should_enable

    @property
    def timeseries(self):
        """pandas.DataFrame : Internal object housing the time series data"""
        output = self._timeseries
        if self.enable_transforms is True:
            for a_series in self.series:
                output[a_series] = self.transformations.apply(a_series,
                                                              output[a_series])

            output = self._execute_na_strategy(output, self._na_strategy)

        return output

    @timeseries.setter
    def timeseries(self, df):
        if isinstance(df, pandas.DataFrame) is not True:
            raise TypeError("Value must be a 'pandas.DataFrame'")

        self._timeseries = df

    @property
    def primary_series(self):
        """str : Name of the primary time series"""
        return self._primary_series

    @primary_series.setter
    def primary_series(self, series):
        self._primary_series = series
        self._verify(self._timeseries, series)

    @property
    def periods(self):
        """pandas.DatetimeIndex : The date time index of the time series"""
        return self._timeseries.index if self._timeseries is not None else None

    @property
    def index_name(self):
        """str : Index name of the object's `pandas.DatetimeIndex`"""
        return self.timeseries.index.name

    @property
    def series(self):
        """list(str) : Names of all time series"""
        return list(self._timeseries.columns.to_numpy())

    @property
    def frequency(self):
        """pandas.tseries.offsets.BusinessDay : Frequency of the time series"""
        return self._timeseries.index.freq

    @frequency.setter
    def frequency(self, freq):
        self._timeseries = self._timeseries.asfreq(freq)

    @property
    def timezone(self):
        """str : The timezone of the time series"""
        return self._timeseries.index.tz

    @timezone.setter
    def timezone(self, tz):
        if self.timezone is None:
            self._timeseries.index = self._timeseries.index.tz_localize(tz)
        else:
            self._timeseries.index = self._timeseries.index.tz_convert(tz)

    def _execute_na_strategy(self, timeseries, na_strategy):
        if isinstance(na_strategy, int) or isinstance(na_strategy, float):
            result = timeseries.fillna(value=na_strategy)
        else:
            result = timeseries.fillna(method=na_strategy)

        return result

    def append(self, timeseries, no_new_series=True):
        """Append time series

        Parameters
        ----------
        timeseries : pandas.DataFrame, Timeseries
            A data frame or `Timeseries` that houses the time series data.
            Must have an index that is `DatetimeIndex` and must have its
            frequency set.
        no_new_series : bool, optional
            Determines whether new series (aka columns) should be allowed
        """
        if isinstance(timeseries, pandas.DataFrame) is True:
            timeseries = Timeseries(timeseries)
        elif isinstance(timeseries, Timeseries) is True:
            if timeseries.frequency != self.frequency:
                raise ValueError("'timeseries' should have the same frequency")

        if no_new_series is True:
            has_new_series = [i not in self.series for i in timeseries.series]
            has_new_series = any(has_new_series)

            if has_new_series is True:
                raise ValueError("'timeseries' has new series")

        self._verify(timeseries.timeseries, self._primary_series)
        new_timeseries = self._timeseries.append(timeseries.timeseries,
                                                 ignore_index=False,
                                                 verify_integrity=True)
        new_timeseries = self._execute_na_strategy(new_timeseries,
                                                   self._na_strategy)
        self._timeseries = new_timeseries

    def has_series(self, name):
        """Determines whether the `Timeseries` has the series name

        Parameters
        ----------
        name : str
            The name of the series to check for

        Returns
        -------
        bool
            If `True`, then `Timeseries` has the series name, otherwise `False`
        """

        return name in self.series

    def get_series_numpy(self, name=None):
        self.get_series_pandas(name=name).to_numpy()

    def get_series_pandas(self, name=None):
        if name is None and self._primary_series is None:
            raise ValueError("'name' cannot be None if primary series is None")
        series_name = name if name is not None else self._primary_series

        return self.timeseries[series_name]

    def plot(self, series=None, bounds=None):
        """Plot time series with a `Plotly` line chart

        Parameters
        ----------
        series : str, optional
            Specifies which time series to plot. If `None`, then the object's
            defined `Timeseries.primary_series` is used
        """
        if series is None and self._primary_series is None:
            raise ValueError("Must provide a value for 'series' "
                             "if 'primary_series' property is not set")
        if series is None:
            series = self._primary_series

        plot = px.line(self.timeseries.reset_index(), x=self.index_name,
                       y=series)
        plot.show()

    def slice(self, indices, positional=False):
        if positional is True:
            output = self._timeseries.iloc[indices, :]
        else:
            output = self._timeseries.loc[indices, :]

        return Timeseries(output, self._primary_series, self._calendar,
                          self._na_strategy, self._enable_transforms)

    def _verify(self, timeseries, primary_series):
        timeseries_freq = timeseries.index.freq

        if timeseries_freq is None:
            raise AttributeError('Time series needs to have frequency set')

        if isinstance(timeseries.index, pandas.DatetimeIndex) is not True:
            raise TypeError("Index must be a pandas.DatetimeIndex")

        if (primary_series is not None and
                primary_series not in timeseries.columns):
            raise ValueError(f"Series '{primary_series}' does not exist")

        if any(timeseries.apply(lambda x: x.hasnans)) is True:
            raise ValueError("No series can have NaN/Inf")

    def __repr__(self):
        output = list()
        output.append(f"# of records: {len(self.timeseries)}, "
                      f"of series: {len(self.timeseries.columns)}")
        output.append(f"Frequency: {self.frequency}")
        output.append(f"Date min: {self.timeseries.index.min()}, "
                      f"max: {self.timeseries.index.max()}")

        return "\n".join(output)

    def split(self, series=None, n=10):
        series = self._primary_series if series is None else series

        series_splitter = sklearn.model_selection.TimeSeriesSplit(n_splits=n)
        ts_splitter = series_splitter.split(self.get_series_pandas(series))

        for train_ds, test_ds in ts_splitter:
            yield [self.slice(train_ds, True), self.slice(test_ds, True)]


class ExogFrame:
    """Wrapper for exogenous time series

    The `ExogFrame` is effectively a wrapper around `pandas.DataFrame` but
    stores each series (aka columns) separately. This also granular control
    over each series' `DatatimeIndex`, such as what frequency they're held as
    how the series is handled when index resampling occurs.

    When instantiating, a `pandas.DataFrame` can be provided. `ExogFrame` will
    split up the `pandas.DataFrame` into individual `ExogSeries`, which wraps
    `pandas.Series`, allowing for index control.

    Additional series can be added via method `self.add_series`.

    Attributes
    ----------
    exogseries : collections.OrderedDict of ExogSeries
    exogseries_names : list of str
    nrows : list of int
    nrows_named : collections.OrderedDict of int
        Keyed on `ExogSeries` names and their size
    nseries : int
        Number of `ExogSeries`
    frequency : list of pandas.tseries.offsets.*
    frequency_named : collections.OrderedDict of pandas.tseries.offsets.*

    Parameters
    -----------
    df : {pandas.DataFrame, None}
        A starting `pandas.DataFrame` to use for creating `ExogSeries`
    na_strat : {'backfill', 'bfill', 'pad', 'ffill', 'drop', None, function}
        Strategy for handling NaN. If not a `dict`, then the `na_strat` is
        applied to all `ExogSeries`. If `dict`, than permitted values are the
        same as above, and should be keyed on the series name. If there is a
        missing series name in the `dict`, than the value is assumed to be
        `None`.
    resample_strat : {str, function, numpy ufunc, None}
        Strategy for resampling. If not a `dict`, `resample_strat` is applied
        to all `ExogSeries`. If `dict`, permitted values are the same as above,
        and should be keyed on the series name. If `None`, than the
        `ExogSeries` property `resample_fill_strategy` is used. Please see
        `pands.DataFrame.apply` for details on this.
    """

    def __init__(self, df=None, na_strat=None, resample_strat=None):
        self._exogseries = collections.OrderedDict()

        if df is not None:
            self._split_df(df, na_strat, resample_strat)

    @property
    def exogseries(self):
        return self._exogseries

    @property
    def exogseries_names(self):
        return list(self.exogseries.keys())

    @property
    def nrows(self):
        return [i.size for i in self._exogseries.values()]

    @property
    def nrows_named(self):
        output = collections.OrderedDict()
        for series_name, series in self._exogseries.items():
            output[series_name] = series.size

        return output

    @property
    def nseries(self):
        return len(self._exogseries)

    @property
    def frequency(self):
        return [i.index.freq for i in self._exogseries.values()]

    @property
    def frequency_named(self):
        output = collections.OrderedDict()
        for series_name, series in self._exogseries.items():
            output[series_name] = series.index.freq

        return output

    def gen_exogseries(self):
        """Extract `ExogSeries` as a generator

        Yields
        ------
        collections.OrderedDict['name', 'series']
            The 'name' key contains the `ExogSeries` name aka column name, and
            the 'series' key contains the actual `ExogSeries`
        """
        def series_iter(exogseries):
            for series_name, series in exogseries.items():
                output = collections.OrderedDict()
                output['name'] = series_name
                output['series'] = series

                yield output

        return series_iter(exogseries=self._exogseries)

    def _split_df(self, df, na_strat=None, resample_strat=None):
        for a_col in df.columns:
            if isinstance(na_strat, dict) is True:
                try:
                    input_na_strat = na_strat[a_col]
                except KeyError:
                    input_na_strat = None
            else:
                input_na_strat = na_strat

            if isinstance(resample_strat, dict) is True:
                try:
                    input_resample_strat = resample_strat[a_col]
                except KeyError:
                    input_resample_strat = None
            else:
                input_resample_strat = resample_strat

            self._exogseries[a_col] = ExogSeries(df[a_col], input_na_strat,
                                                 input_resample_strat)

    def get_series(self, exog_name):
        """Retrieve an `ExogSeries` by name

        Parameters
        ----------
        exog_name : str

        Returns
        -------
        ExogSeries
        """
        return self._exogseries[exog_name]

    def add_series(self, series, series_name, na_strat=None,
                   resample_strat=None):
        """Retrieve a new series to the frame

        Parameters
        ----------
        series : pandas.Series
        series_name : str
        """
        series = copy.deepcopy(series)
        series.name = series_name

        self._exogseries[series_name] = ExogSeries(series, na_strat,
                                                   resample_strat)

    def drop_series(self, exog_name):
        """Remove an ExogSeries"""
        del self._series[exog_name]

    def set_resample_strat(self, exog_name, resample_strat):
        """Set a resampling strategy for an ExogSeries

        Parameters
        ----------
        exog_name : str
        resample_strat : {str, function, numpy ufunc, None}
            How should the resampler handle aggregation/expansion. If `None`,
            than the `ExogSeries` property `resample_fill_strategy` is used.
            Please see `pands.DataFrame.apply` for details on this.
        """
        self.get_series(exog_name).resample_fill_strategy = resample_strat

    def resample(self, freq, series=None, method=None, inplace=False):
        """Resample, or group by, index for a series

        Used to up or down sample a `ExogSeries`. Please see the `ExogSeries`
        `resample_fill_strategy` function on how the aggregation or expansion
        strategy is handled

        Parameters
        ----------
        freq : pandas.tseries.offsets.*
            The new `ExogSeries` datetime frequency
        series : {str, None}
            If the series' name is provide, only resampling is applied to that
            series. If `None`, the resampling is applied to all `ExogSeries`
        method : {str, function, numpy ufunc, None}
            How should the resampler handle aggregation/expansion. If `None`,
            than the `ExogSeries` property `resample_fill_strategy` is used.
            Please see `pands.DataFrame.apply` for details on this.
        inplace : bool
            If `True`, than the method will set the `ExogSeries` data
            internally. Else, returns a copy of the `ExogFrame` with the
            resampling applied

        Returns
        -------
        Nothing, ExogFrame
            If `inplace` is `False`, a copy of the `ExogFrame` is returned.
            Otherwise, nothing is returned and resampling has been applied to
            the object in question
        """
        if inplace:
            if series is not None:
                self.get_series(series).resample(freq, method, inplace=True)
            else:
                for a_exogseries in self.gen_exogseries():
                    exogseries_data = a_exogseries['series']
                    exogseries_data.resample(freq, method, inplace=True)
        else:
            df = copy.deepcopy(self)

            if series is not None:
                df.get_series(series).resample(freq, method, inplace=True)
            else:
                for a_exogseries in df.gen_exogseries():
                    exogseries_data = a_exogseries['series']
                    exogseries_data.resample(freq, method, inplace=True)

            return df

    def copy(self):
        return copy.deepcopy(self)

    def has_na(self):
        """Check if any series has NaN

        Returns
        -------
        bool
        """
        return any([i['series'].has_na() for i in self.gen_exogseries()])

    def is_na(self):
        """Retrieve DataFrame with boolean NaN for all series

        Returns
        -------
        pandas.DataFrame of bool
        """
        output = [(i['name'], i['series'].is_na())
                  for i in self.gen_exogseries()]

        result = collections.OrderedDict()
        for a_series in output:
            result[a_series[0]] = a_series[1]

        return pandas.DataFrame.from_dict(result)

    def size_na(self):
        """Get the number of NaN by series

        Returns
        -------
        collections.OrderedDict
            Keyed on series/column name
        """
        output = [(i['name'], i['series'].size_na())
                  for i in self.gen_exogseries()]

        result = collections.OrderedDict()
        for a_series in output:
            result[a_series[0]] = a_series[1]

        return result


class ExogSeries:
    def __init__(self, series, na_strategy=None, resample_fill_strategy=None):
        """Time series array wrapper

        A wrapper around an internally stored `pandas.Series`, it provides
        a set of convenience access/modification of series attributes and
        functions, especially around `NaN` handling.

        Attributes
        ----------
        series : pandas.Series
        frequency : pandas.tseries.offsets.*
            The exact offset is dependent on the series frequency
        series_name : str
        index_name : str
            The name of the `pandas.DatetimeIndex`
        index_type : pandas.DatetimeIndex
            This and attributes `index` and `periods` return the same object
        index : pandas.DatetimeIndex
        periods : pandas.DatetimeIndex
        size : int
            The number of records, or size of the series
        type : numpy.dtype
            The series data type
        na_strategy : {'backfill', 'bfill', 'pad', 'ffill', 'drop', None,
                       function}
            Indicates how `NaN` in the series are handled. This strategy
            applies only to existing `NaN` in the series when set. This does
            not affect how `NaN` are handled during index resampling
        resample_fill_strategy : {str, function, numpy ufunc, None}
            Indicates the function used to fill in `NaN` after an index
            resampling.

        Parameters
        -----------
        series : pandas.Series
        na_strategy : {'backfill', 'bfill', 'pad', 'ffill', 'drop', 'asis',
                       None, function}
            The method used for handing `NaN` in the series. If `None`,
            defaults to `asis`
        resample_fill_strategy : {str, function, numpy ufunc, None}
            Method used for filling in the seriesafter index resampling. If
            `None`, 'ffill' is default. Please see `pands.DataFrame.apply` for
            details on this
        """
        self._na_strategy = na_strategy if na_strategy else 'asis'

        if resample_fill_strategy is None:
            self._resample_fill_strategy = 'ffill'
        else:
            self._resample_fill_strategy = resample_fill_strategy

        self._verify(series)
        series = self._clean_series(series)
        self._series = series

    @property
    def series(self):
        return self._series

    @property
    def frequency(self):
        return self.series.index.freq

    @property
    def series_name(self):
        return self.series.name

    @property
    def index_name(self):
        return self.series.index.name

    @property
    def index_type(self):
        return self.series.index

    @property
    def index(self):
        return self.series.index

    @property
    def periods(self):
        return self.series.index

    @property
    def size(self):
        return len(self.series)

    @property
    def dtype(self):
        return self.series.dtype

    @property
    def na_strategy(self):
        return self._na_strategy

    @na_strategy.setter
    def na_strategy(self, strat):
        self._na_strategy = strat

    @property
    def resample_fill_strategy(self):
        return self._resample_fill_strategy

    @resample_fill_strategy.setter
    def resample_fill_strategy(self, strat):
        self._resample_fill_strategy = strat

    def has_na(self):
        """Check if series has NaN

        Returns
        -------
        bool
        """
        return self.series.isnull().values.any()

    def is_na(self):
        """Retrieve a is NaN boolean series

        Returns
        -------
        pandas.Series of bool
        """
        return self.series.isnull()

    def size_na(self):
        """Get the number of NaN in the series

        Returns
        -------
        int
        """
        return self.series.isnull().sum()

    def cast_series(self, dtype):
        """Cast series' data type

        Parameters
        ----------
        dtype : numpy.dtype
        """
        self._series = self._series.astype(dtype)

    def set_value(self, index, value):
        self._series[index] = value

    def resample(self, freq, method=None, inplace=False):
        """Resample, or group by, index

        Used to up or down sample the series. Please see the object's
        `resample_fill_strategy` function on how the aggregation or expansion
        strategy is handled

        Parameters
        ----------
        freq : pandas.tseries.offsets.*
            The new `ExogSeries` datetime frequency
        method : {str, function, numpy ufunc, None}
            How should the resampler handle aggregation/expansion. If `None`,
            than the `ExogSeries` property `resample_fill_strategy` is used.
            Please see `pands.DataFrame.apply` for details on this
        inplace : bool
            If `True`, than the method will set the `ExogSeries` data. Else,
            this method returns a new copy instead
        """
        method = method if method else self._resample_fill_strategy
        result = self.series.resample(freq).apply(method)

        if inplace is True:
            self.series = result
        else:
            return result

    def _verify(self, series):
        if isinstance(series, pandas.Series) is not True:
            raise TypeError('series must be a pandas.Series')

        # if isinstance_pddt(series) is not True:
        #     raise IndexError("Series does not have DataTimeIndex")

        if series.index.freq is None:
            raise TypeError("Series does not have a frequency set")

    def _clean_series(self, series):
        series = series.replace([pandas.np.inf, -pandas.np.inf], pandas.np.nan)

        # NA cleaning
        fillna_methods = ['backfill', 'bfill', 'pad', 'ffill', None]

        output = None
        if self._na_strategy is not None:
            if self._na_strategy == 'drop':
                output = series.dropna()
            elif self._na_strategy == 'asis':
                output = series
            elif self._na_strategy in fillna_methods:
                output = series.fillna(method=self._na_strategy)
            else:
                output = series.replace(
                    pandas.np.nan,
                    self._na_strategy(series))
        else:
            output = series

        return output

    def copy(self):
        return copy.deepcopy(self)


class TimeseriesForecast(Timeseries):
    def __init__(self, X, primary_series=None, calendar=None,
                 na_strategy='ffill'):
        super().__init__(X, primary_series, calendar, na_strategy)

    def plot(self):
        """Plot time series with a `Plotly` line chart"""

        r = self._X.reset_index().drop('se', 1)
        r = r.melt(self.index_name, var_name='series')

        plot = px.line(r, x=self.index_name, y='value', color='series')
        plot.show()


class TimeseriesFitted(Timeseries):
    def __init__(self, X, primary_series=None, calendar=None,
                 na_strategy='ffill'):
        super().__init__(X, primary_series, calendar, na_strategy)

    def plot(self):
        """Plot time series with a `Plotly` line chart"""
        r = self._X.reset_index().melt(self.index_name, var_name='series')

        plot = px.line(r, x=self.index_name, y='value', color='series')
        plot.show()


class Transformations:
    def __init__(self, series_names):
        self._transforms = {series: list() for series in series_names}

    def add(self, series, fun):
        if isinstance(series, str):
            self._transforms[series].append(fun)

        if isinstance(series, list):
            for a_series in series:
                self._transforms[a_series].append(fun)

    def drop(self, series, index):
        if isinstance(series, str):
            self._transforms[series].pop(index)

        if isinstance(series, list):
            for a_series in series:
                self._transforms[a_series].pop(index)

    def drop_last(self, series):
        self.drop(series, -1)

    def drop_first(self, series):
        self.drop(series, 0)

    def replace(self, series, index, fun):
        if isinstance(series, str):
            self._transforms[series][index] = fun

        if isinstance(series, list):
            for a_series in series:
                self._transforms[a_series][index] = fun

    def insert(self, series, index, fun):
        if isinstance(series, str):
            self._transforms[series].insert(index, fun)

        if isinstance(series, list):
            for a_series in series:
                self._transforms[a_series].insert(index, fun)

    def apply(self, series_name, series_data):
        result = series_data
        for a_fun in self._transforms[series_name]:
            result = a_fun(result)

        return result


def isinstance_pddt(index):
    isit = isinstance(index, pandas.core.indexes.datetimes.DatetimeIndex)

    return isit
