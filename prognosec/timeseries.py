import pandas
import plotly.express as px
import pandas_market_calendars as mcal


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

    def __init__(self, X, primary_series=None, calendar=None,
                 na_strategy='ffill'):
        if isinstance(X, pandas.DataFrame) is not True:
            raise TypeError("`X` needs to be a `pandas.DataFrame`")

        self._calendar = calendar

        if calendar is not None:
            index_name = X.index.name
            ts_dates = self._calendar.schedule(start_date=X.index.min(),
                                               end_date=X.index.max())
            X = X.reindex(index=ts_dates.index)
            X.index.name = index_name

        self._X = self._execute_na_strategy(X.copy(deep=True), na_strategy)
        self._verify(self._X, primary_series)
        self._primary_series = primary_series
        self._na_strategy = na_strategy

    @property
    def X(self):
        """pandas.DataFrame : Internal object housing the time series data"""
        return self._X

    @property
    def primary_series(self):
        """str : Name of the primary time series"""
        return self._primary_series

    @primary_series.setter
    def primary_series(self, series):
        self._primary_series = series
        self._verify(self._X, series)

    @property
    def periods(self):
        """pandas.DatetimeIndex : The date time index of the time series"""
        return self._X.index if self._X is not None else None

    @property
    def index_name(self):
        """str : Index name of the object's `pandas.DatetimeIndex`"""
        return self._X.index.name

    @property
    def series(self):
        """list(str) : Names of all time series"""
        return self._X.columns.to_numpy() if self._X is not None else None

    @property
    def frequency(self):
        """pandas.tseries.offsets.BusinessDay : Frequency of the time series"""
        return self._X.index.freq if self._X is not None else None

    @frequency.setter
    def frequency(self, freq):
        self._X = self._X.asfreq(freq)

    @property
    def timezone(self):
        """str : The timezone of the time series"""
        return self._X.index.tz

    @timezone.setter
    def timezone(self, tz):
        if self.timezone is None:
            self._X.index = self._X.index.tz_localize(tz)
        else:
            self._X.index = self._X.index.tz_convert(tz)

    def _execute_na_strategy(self, X, na_strategy):
        if isinstance(na_strategy, int) or isinstance(na_strategy, float):
            result = X.fillna(value=na_strategy)
        else:
            result = X.fillna(method=na_strategy)

        return result

    def append(self, X, no_new_series=True):
        """Append time series

        Parameters
        ----------
        X : pandas.DataFrame, Timeseries
            A data frame or `Timeseries` that houses the time series data.
            Must have an index that is `DatetimeIndex` and must have its
            frequency set.
        no_new_series : bool, optional
            Determines whether new series (aka columns) should be allowed
        """
        if isinstance(X, pandas.DataFrame) is True:
            X = Timeseries(X)
        elif isinstance(X, Timeseries) is True:
            if X.frequency != self.frequency:
                raise ValueError("'X' should have the same frequency")

        if no_new_series is True:
            has_new_series = any([i not in self.series for i in X.series])

            if has_new_series is True:
                raise ValueError("'X' has new series")

        self._verify(X.X, self._primary_series)
        new_X = self._X.append(X.X, ignore_index=False, verify_integrity=True)
        new_X = self._execute_na_strategy(new_X, self._na_strategy)
        self._X = new_X

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

        return self._X[series_name]

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

        plot = px.line(self._X.reset_index(), x=self.index_name, y=series)
        plot.show()

    def _verify(self, X, primary_series):
        X_freq = X.index.freq

        if X_freq is None:
            raise AttributeError('Time series needs to have frequency set')

        if isinstance(X.index, pandas.DatetimeIndex) is not True:
            raise TypeError("Index must be a pandas.DatetimeIndex")

        if (primary_series is not None and primary_series not in X.columns):
            raise ValueError(f"Series '{primary_series}' does not exist")

        if any(X.apply(lambda x: x.hasnans)) is True:
            raise ValueError("No series can have NaN/Inf")

    def __repr__(self):
        output = list()
        output.append(f"# of records: {len(self._X)}, "
                      f"of series: {len(self._X.columns)}")
        output.append(f"Frequency: {self.frequency}")
        output.append(f"Date min: {self._X.index.min()}, "
                      f"max: {self._X.index.max()}")

        return "\n".join(output)


class TimeseriesForecast(Timeseries):
    def __init__(self, X, primary_series=None, calendar=None,
                 na_strategy='ffill'):
        super().__init__(X, primary_series, calendar, na_strategy)

    def plot(self):
        """Plot time series with a `Plotly` line chart"""

        r = self._X.reset_index().drop('se', 1)
        r = r.melt('date', var_name='series')

        plot = px.line(r, x=self.index_name, y='value', color='series')
        plot.show()
