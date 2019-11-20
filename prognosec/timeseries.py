import pandas
import plotly.express as px


ONEBDAY = pandas.tseries.offsets.BusinessDay(1)


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

    def __init__(self, X, primary_series=None):
        if isinstance(X, pandas.DataFrame) is not True:
            raise TypeError("`X` needs to be a `pandas.DataFrame`")

        self._X = X.copy(deep=True)
        self._primary_series = primary_series
        self._verify()

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
        self._verify()

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

    def plot(self, series=None):
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

    def _verify(self):
        if self.frequency is None:
            raise AttributeError('Time series needs to have frequency set')

        if isinstance(self._X.index, pandas.DatetimeIndex) is not True:
            raise TypeError("Index must be a pandas.DatetimeIndex")

        if (self._primary_series is not None and
                self._primary_series not in self.X.columns):
            raise ValueError(f"Series '{self._primary_series}' does not exist")

    def __repr__(self):
        output = list()
        output.append(f"# of records: {len(self._X)}, "
                      f"of series: {len(self._X.columns)}")
        output.append(f"Frequency: {self.frequency}")
        output.append(f"Date min: {self._X.index.min()}, "
                      f"max: {self._X.index.max()}")

        return "\n".join(output)
