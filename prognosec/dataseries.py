import collections
import copy
import pandas
import numpy
import functions
import progutils
import exceptions

NAN_STR_METHODS = ('backfill', 'bfill', 'pad', 'ffill', 'asis', 'drop')
INF_STR_METHODS = ('asis', 'asna', 'zero')
UPSAMPLE_STR_APPLY_METHODS = ('ffill', 'bfill')
UPSAMPLE_STR_INTER_METHODS = ('linear',)

# Downsampling (using apply method) is far larger as it usually uses
# Some aggregation function. Instead, we try catch strings instead
DOWNSAMPLE_STR_METHODS = None


class SeriesFrame:
    def __init__(self, df=None, index=None, index_name=None):
        df = copy.deepcopy(df)
        split_df = self._split_dataframe(df)

        findex = split_df['index'] if index is None else index

        findexname = split_df['index'].name
        findexname = findexname if index_name is None else index_name

        if progutils.is_time_index(findex) is False:
            msg = "'index' must be a time index"
            raise exceptions.IndexIntegrityError(msg)

        if isinstance(findexname, str) is False:
            msg = "'index_name' must be string"
            raise exceptions.IndexIntegrityError(msg)

        self._frame = split_df['frame']

        findex.name = findexname
        self._index = findex
        self._name_index = findexname

    @property
    def frame(self):
        return self._frame

    @property
    def size(self):
        return len(self._frame)

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, index):
        if progutils.is_time_index(index) is False:
            msg = f"index type {type(index)} is not a datetime index"
            raise exceptions.IndexIntegrityError(msg)

        self._index = index

    @property
    def name_index(self):
        return self._name_index

    @name_index.setter
    def name_index(self, index_name):
        if isinstance(index_name, str) is False:
            msg = "index_name must be a string"
            raise exceptions.IndexIntegrityError(msg)

        self._name_index = index_name

    @property
    def freq(self):
        return self.index.freq

    @property
    def value_index(self):
        return self.index.values

    @property
    def dtype_index(self):
        return self.index.dtype

    def _split_dataframe(self, dataframe):
        timeseries = collections.OrderedDict()

        for colname in dataframe.columns:
            timeseries[colname] = Timeseries(dataframe[colname])

        output = {'index': dataframe.index, 'frame': timeseries}

        return output

    def add(self, series, series_name=None):
        if isinstance(series, Timeseries) is False:
            msg = "'series' should be a 'Timeseries' instance"
            raise exceptions.SeriesIntegrityError(msg)

        if series_name is None:
            series_name = series.name_series
        else:
            if isinstance(series_name, str) is False:
                msg = "'series_name' must be a `str`"
                raise exceptions.SeriesIntegrityError(msg)

        if series_name in self._frame.keys():
            msg = f"'{series_name}' already exists"
            raise exceptions.SeriesIntegrityError(msg)

        series.name_series = series_name

        self._frame[series_name] = series

    def remove(self, series_name):
        if isinstance(series_name, str) is False:
            msg = "'series_name' must be 'str'"
            raise exceptions.SeriesIntegrityError(msg)

        if series_name not in self._frame.keys():
            raise KeyError(f"series named '{series_name}' does not exist")

        del self._frame[series_name]

    def drop(self, series_name):
        return self.remove(series_name)

    def replace(self, series, series_name=None):
        if isinstance(series, Timeseries) is False:
            raise exceptions.SeriesIntegrityError("series must be Timeseries")

        if series_name is None:
            series_name = series.name_series
        else:
            if isinstance(series_name, str) is False:
                msg = "'series_name' must be a `str`"
                raise exceptions.SeriesIntegrityError(msg)

        series.name_series = series_name

        self._frame[series_name] = series

    @progutils.dec_does_series_name_exist
    def access_series(self, series_name):
        try:
            output = self._frame[series_name]
        except KeyError:
            raise KeyError(f"Series '{series_name}' does not exist")

        return output

    def access_transform(self, series_name):
        return self.access_series(series_name).transform

    def access_strat_na(self, series_name):
        return self.access_series(series_name).strat_na

    def access_strat_up(self, series_name):
        return self.access_series(series_name).strat_up

    def access_strat_down(self, series_name):
        return self.access_series(series_name).strat_down

    def set_transform(self, series_name, transformation):
        self.access_series(series_name).transform = transformation

    def set_strat_na(self, series_name, strat_na):
        self.access_series(series_name).strat_na = strat_na

    def set_strat_up(self, series_name, strat_up):
        self.access_series(series_name).strat_up = strat_up

    def set_strat_down(self, series_name, strat_down):
        self.access_series(series_name).strat_down = strat_down

    def resample(self, freq, series_names=None, use_original=False):
        if series_names is None:
            stored_series = self.frame.values()
        elif isinstance(series_names, str):
            stored_series = [self.access_series(series_names)]
        elif progutils.is_tuple_or_list(series_names):
            stored_series = [self.access_series[i] for i in series_names]
        else:
            raise TypeError("'series_names' is not a 'str' or sequence")

        resampled_series = list()
        for a_ts in stored_series:
            new_ts = a_ts.resample(freq, use_original=use_original)
            resampled_series.append(new_ts)

        fake_df = pandas.DataFrame.from_dict({'col1': [1, 2]})
        fake_df.index = pandas.period_range('2019-01-01', periods=2,
                                            name='date')

        new_frame = SeriesFrame(fake_df)
        new_frame.drop('col1')

        for a_ts in resampled_series:
            new_frame.add(a_ts)

            if len(a_ts.index) > len(new_frame.index):
                new_frame.index = a_ts.index

        return new_frame


class Timeseries:
    """Time series array wrapper

    A wrapper around an internally stored `pandas.Series`, it provides
    a set of convenience access/modification of series attributes and
    functions, especially around `NaN` handling and resampling.

    Attributes
    ----------
    series : pandas.Series
    index : DatetimeIndex, PeriodIndex, TimedeltaIndex
    name_index : str
    name_series : str
    freq : pandas.DateOffset
    value_series : numpy.ndarray
    value_index : numpy.ndarray
    dtype : numpy.dtype
    dtype_index : numpy.dtype
    strat_na : str, scalar, dict, MissingValueFillFunction
        Optional. Defaults to `'asis'`, or leave `NaN` in place.
    strat_inf : {'asna', 'asis', 'zero', scalar_like}
        Optional. Defaults to `'asna'`, or replace `Inf` with `NaN`.
    strat_up : str, UpsampleFunction
        Optional. Defaults to `'linear'`, or linear interpolation.
    strat_down : DownsampleFunction
        Optional. Defaults to `functions.sampledown_mean()`, or mean
        aggregation.
    transform : Transformation
        Optional. Defaults to an empty `Transformation` instance (no transform)

    Parameters
    -----------
    series : pandas.Series, array_like
        The time series data. Numeric and string data is supported. If an
        ordered sequence (e.g. `list`, `tuple`) or `numpy.ndarray` is provided,
        parameters `index`, `series_name`, and `index_name` cannot be `None`
    index : DatetimeIndex, PeriodIndex, TimedeltaIndex, Optional
        The time index of the time series. Optional only if `series` is a
        `pandas.Series`, otherwise this must be provided
    series_name : str, Optional
        The name of the time series. Optional only if `series` is a
        `pandas.Series` and it already has a name. Otherwise this must be
        provided. This also overwrise `series` name attribute
    index_name : str, Optional
        The name of the time series' index. Otherwise, behavior is the same
        as `series_name`.
    strat_na : str, scalar, dict, MissingValueFillFunction
        Method for handling missing values in `series`. Missing value handling
        is done on-the-fly when accessing a `Timseries` instances' `series`
        property
    strat_up : str, UpsampleFunction
        Method for handling upsampling when resampling to new frequency
    strat_down : DownsampleFunction
        Method for handling the dowsampling when resampling to new frequency
    transform : Transformation
        Object hosting the ordered sequence of array transformation. Direct
        access to this object enables the editing of the sequence
    """

    def __init__(self, series, index=None, series_name=None, index_name=None,
                 strat_na=None, strat_inf=None, strat_up=None, strat_down=None,
                 transform=None):
        if isinstance(series, pandas.Series) is not True:
            if index is None:
                errmsg = "Index must be provided for non-pandas.Series"
                raise exceptions.IndexTypeError(errmsg)
            elif progutils.is_time_index(index) is not True:
                errmsg = f"'index' parameter must be of type 'DatetimeIndex', "
                errmsg += "'PeriodIndex', or 'TimedeltaIndex'"
                raise exceptions.IndexTypeError(errmsg)

            prepped_series = pandas.Series(series, index=index)
        else:
            prepped_series = series

        # At this point `prepped_series` should be a `pandas.Series`
        if series_name is not None:
            try:
                prepped_series.name = series_name
            except TypeError as e:
                msg = ". ".join(e.args)
                raise exceptions.SeriesIntegrityError(msg)

        if index_name is not None:
            try:
                prepped_series.index.name = index_name
            except TypeError as e:
                msg = ". ".join(e.args)
                raise exceptions.IndexIntegrityError(msg)

        self._verify_new_series(prepped_series)

        self._series = prepped_series

        self._strats = dict()
        self.strat_na = strat_na
        self.strat_inf = strat_inf
        self.strat_up = strat_up
        self.strat_down = strat_down

        self.transform = transform

    @property
    def series(self):
        """Timeseries data object

        Stores the time series data as a `pandas.Series`. This includes the
        data sequence and index.

        Missing valuing handling and transformations are applied to the
        returned series.

        Returns
        -------
        pandas.Series
        """
        output = self._clean_series(self._series)
        output = self.transform.apply(output)

        return output

    @property
    def index(self):
        """Datetime-like index of the time series

        Returns
        -------
        DatetimeIndex, PeriodIndex, TimedeltaIndex
            A `pandas` datetime-like index. Which is returned depends on the
            original input.
        """
        return self._series.index

    @property
    def name_index(self):
        """Time series index name

        Returns
        -------
        str
        """
        return self._series.index.name

    @name_index.setter
    def name_index(self, new_name):
        """Set a new name for the index of the time series

        Parameters
        ----------
        new_name : str
        """
        if isinstance(new_name, str) is False:
            raise exceptions.IndexIntegrityError("index name must be string")

        self._series.index.name = new_name

    @property
    def name_series(self):
        """Time series name

        Returns
        -------
        str
        """
        return self._series.name

    @name_series.setter
    def name_series(self, new_name):
        """Set a new name for the time series

        Parameters
        ----------
        new_name : str
        """
        if isinstance(new_name, str) is False:
            raise exceptions.SeriesIntegrityError("series name must be string")

        self._series.name = new_name

    @property
    def freq(self):
        """The frequency of the time series

        Returns
        -------
        pandas.DateOffset
        """
        return self._series.index.freq

    @property
    def value_series(self):
        """Raw values as an array of the time series

        These values do not have NA handling and transformations applied.

        Returns
        -------
        numpy.ndarray
        """
        return self._series.to_numpy(copy=True)

    @property
    def value_index(self):
        """Raw values as an array of the index

        These values do not have NA handling and transformations applied.

        Returns
        -------
        numpy.ndarray
        """
        return self._series.index.values

    @property
    def dtype(self):
        """The time series data type

        Returns
        -------
        numpy.dtype
        """
        return self._series.dtype

    @property
    def dtype_index(self):
        """The time series index data type

        Returns
        -------
        numpy.dtype
        """
        return self.index.dtype

    @property
    def strat_na(self):
        """Method for handling missing values

        See `strat_na` setter for more information.

        Returns
        -------
        str, scalar, dict, MissingValueFillFunction
        """
        return self._strats['na_handling']

    @strat_na.setter
    def strat_na(self, method):
        """Setting a new method for handling missing values

        Please see `pandas.Series.fillna` method for more information. If
        method is `drop`, will use `pandas.Series.dropna`.

        Parameters
        ----------
        method : str, scalar, dict, MissingValueFillFunction
            Method for handling missing values in `series`. Missing value
            handling is done on-the-fly when accessing a `Timseries` instances'
            `series` property.
            If `None`, defaults to `'asis'`, or leave `NaN` in place.
        """
        if method is None:
            method = 'asis'

        if isinstance(method, str):
            if method not in NAN_STR_METHODS:
                msg = f"'{method}' string method is invalid"
                raise exceptions.ComputeMethodError(msg)
        elif callable(method):
            if not isinstance(method, functions.MissingValueFillFunction):
                msg = "function method is not a MissingValueFillFunction"
                raise exceptions.ComputeMethodError(msg)

        self._strats['na_handling'] = method

    @property
    def strat_inf(self):
        """Method for handling infinite value

        See `strat_inf` setter for more information.

        Returns
        -------
        str, scalar
        """
        return self._strats['inf_handling']

    @strat_inf.setter
    def strat_inf(self, method):
        """Setting a new method for handling infinite values

        Please see `pandas.Series.replace` method for more information.

        Parameters
        ----------
        method : {'asna', 'asis', 'zero', scalar_like}
            Method for handling infinite values in `series`. Infinite values
            handling is done on-the-fly when accessing a `Timseries` instances'
            `series` property.
            If `'asna'`, infinite values are replaced with `NaN`, at which
            point `strat_na` handles it.
            If `'asis'`, infinite values are left in place.
            If `'zero'`, infinite values are replaced with zero.
            If `scalar_like`, infinite values are replace with the value.
            If `None`, defaults to `'asna'`, or replace `Inf` with `NaN`.
        """
        if method is None:
            method = 'asna'

        if isinstance(method, str):
            if method not in INF_STR_METHODS:
                msg = f"'{method}' string method is invalid"
                raise exceptions.ComputeMethodError(msg)
        elif (numpy.isscalar(method) and numpy.isreal(method) and
                isinstance(method, bool) is False):
            # If int or float, let it pass
            # apparently, numpy.isreal(numpy.mean) -> True
            # more complicate one above is to try to get rid of this
            pass
        else:
            raise exceptions.ComputeMethodError("Invalid 'strat_inf' method")

        self._strats['inf_handling'] = method

    @property
    def strat_up(self):
        """Method for upsampling

        Please see `pandas.Series.apply` and `pandas.Series.interpolate`
        methods for more information.

        Returns
        -------
        str, UpsampleFunction
        """
        return self._strats['upsampling']

    @strat_up.setter
    def strat_up(self, method):
        """Setting a method for upsampling

        Please see `pandas.Series.resample.apply` and
        `pandas.Series.resample.interpolate` methods for more information.

        # TODO
        At some point, only accept `UpsamplingFunction`.

        Parameters
        ----------
        method : str, UpsampleFunction
            Method for handling upsampling when resampling to new frequency.
            If `None`, defaults to `'linear'` for linear interpolation.
        """
        if method is None:
            method = 'linear'

        if isinstance(method, str):
            if (method not in UPSAMPLE_STR_APPLY_METHODS and
                    method not in UPSAMPLE_STR_INTER_METHODS):
                msg = "Method provided is invalid"
                raise exceptions.ComputeMethodError(msg)
        elif isinstance(method, functions.AggregateFunction):
            pass
        else:
            msg = "Method provided is invalid"
            raise exceptions.ComputeMethodError(msg)

        self._strats['upsampling'] = method

    @property
    def strat_down(self):
        """Method for downsampling

        Please see `pandas.Series.resample.apply` and
        `pandas.Series.resample.interpolate` methods for more information.

        Returns
        -------
        DownsampleFunction
        """
        return self._strats['downsampling']

    @strat_down.setter
    def strat_down(self, method):
        """Setting a method for downsampling

        Please see `pandas.Series.resample.apply` and
        `pandas.Series.resample.interpolate` methods for more information.

        Parameters
        ----------
        method : DownsampleFunction
            Method for handling the dowsampling when resampling to new
            frequency. If `None`, defaults to `functions.sampledown_mean()`, or
            or mean aggregation.
        """
        if method is None:
            method = functions.sampledown_mean()

        if callable(method):
            if isinstance(method, functions.DownsampleFunction) is False:
                msg = "Not a DownsampleFunction"
                raise exceptions.ComputeMethodError(msg)
        else:
            raise exceptions.ComputeMethodError("Method is not a function")

        self._strats['downsampling'] = method

    @property
    def transform(self):
        """Instance of Transformation

        A `Transformation` instance manages an ordered sequence of array
        transformations. This is applied when accessing `series` property.
        See `progutils.Transformation` for details.

        Direct access to this object enables the editing of the sequence.

        Returns
        -------
        Transformation
        """
        return self._transform

    @transform.setter
    def transform(self, transform):
        """Set a new transformation instance

        Parameters
        ----------
        transform : Transformation
            If `None`, will internally create an empty `Transformation`
            instance, which applies no transformation.
        """
        if transform is None:
            transform = progutils.Transformation()
        elif isinstance(transform, progutils.Transformation) is False:
            raise TypeError("Not a `Transformation` instance")

        self._transform = transform

    @classmethod
    def _verify_new_series(cls, series):
        """Checks the series' integrity for Timeseries instances

        This is an exception raising method for checking the "correctness" of a
        `pandas.Series` for use with `Timeseries`.

        Parameters
        ----------
        series : pandas.Series

        Raises
        ------
        IndexIntegrityError
            - Duplicate index values
            - Lack of an index name
            - Index name not being a string
            - Index not having a frequency set
        SeriesIntegrityError
            - Lack of a series name
            - Series name not being a string
        """
        if series.index.has_duplicates is True:
            msg = "series index has duplicate values"
            raise exceptions.IndexIntegrityError(msg)

        if series.index.name is None:
            raise exceptions.IndexIntegrityError("Index must be named")

        if isinstance(series.index.name, str) is False:
            msg = "Index name must be a string"
            raise exceptions.IndexIntegrityError(msg)

        if series.name is None:
            raise exceptions.SeriesIntegrityError("Series must have name")

        if isinstance(series.name, str) is False:
            msg = "Series name must be a string"
            raise exceptions.SeriesIntegrityError(msg)

        if hasattr(series.index, 'freq') is False or series.index.freq is None:
            raise exceptions.IndexIntegrityError("index must have frequency")

    def _clean_series(self, series):
        """Applies cleaning procedures to the series

        This method is meant to apply cleaning processes to the series.
        Currently implements the missing value handling process.

        Parameters
        ----------
        series : pandas.Series

        Returns
        -------
        pandas.Series

        Raises
        ------
        ComputeMethodError
            The missing value method provided is not correct. Check the setter
            of `strat_na` property as this should never be raised.
        """
        if self.strat_inf in INF_STR_METHODS:
            if self.strat_inf == 'asis':
                pass
            elif self.strat_inf == 'asna':
                # replace Inf with NaN
                series = series.replace(numpy.inf, numpy.nan)
            elif self.strat_inf == 'zero':
                # replace Inf with zero
                series = series.replace(numpy.inf, numpy.nan)
            else:
                msg = "'strat_na' not implemented"
                raise exceptions.ComputeMethodError(msg)
        elif (numpy.isscalar(self.strat_inf) and
                numpy.isreal(self.strat_inf) and
                isinstance(self.strat_inf, bool) is False):
            series = series.replace(numpy.inf, self.strat_inf)
        else:
            raise exceptions.ComputeMethodError("'strat_inf' not supported")

        if self.strat_na in NAN_STR_METHODS:
            if self.strat_na == 'asis':
                # Leave NaN in
                pass
            elif self.strat_na == 'drop':
                series = series.dropna()
            else:
                series = series.fillna(method=self.strat_na)
        elif isinstance(self.strat_na, functions.MissingValueFillFunction):
            series = self.strat_na(series)
        elif numpy.isscalar(self.strat_na) or isinstance(self.strat_na, dict):
            # Fill in using value parameter. Please see `pandas.Series.fillna`
            series = series.fillna(value=self.strat_na)
        else:
            raise exceptions.ComputeMethodError("'strat_na' not supported")

        return series

    def has_na(self, after_clean=True):
        """Check if the series has any missing values

        This checks for missing values after cleaning and transformation
        processes are applied.

        Returns
        -------
        boolean
        """
        return self.size_na(after_clean=after_clean) > 0

    def size_na(self, after_clean=True):
        """Count the number of missing values

        This counts missing values after cleaning and transformation
        processes are applied.

        Returns
        -------
        int
        """
        return self.is_na(after_clean=after_clean).sum()

    def is_na(self, after_clean=True):
        """Retrieve a boolean series that locates missing values

        This generates a True/False series, indicating where missing values
        are located. This is boolean series is relevant for the series after
        cleaning and transformations are applied.

        Returns
        -------
        pandas.Series
        """
        if after_clean is True:
            output = pandas.isna(self.series)
        else:
            output = pandas.isna(self._series)

        return output

    def has_inf(self, after_clean=True):
        """Check if the series has any infinities

        This checks for infinities after cleaning and transformation
        processes are applied.

        Returns
        -------
        boolean
        """
        return self.size_inf(after_clean=after_clean) > 0

    def size_inf(self, after_clean=True):
        """Count the number of infinities

        This counts infinities after cleaning and transformation
        processes are applied.

        Returns
        -------
        int
        """
        return self.is_inf(after_clean=after_clean).sum()

    def is_inf(self, after_clean=True):
        """Retrieve a boolean series that locates infinities

        This generates a True/False series, indicating where infinities
        are located. This is boolean series is relevant for the series after
        cleaning and transformations are applied.

        Returns
        -------
        pandas.Series
        """
        if after_clean is True:
            output = numpy.isinf(self.series)
        else:
            output = numpy.isinf(self._series)

        return output

    def cast(self, dtype):
        """Cast a copy of the series

        Please see `pandas.Series.astype` for more information.

        Parameters
        ----------
        dtype : str, numpy.dtype

        Returns
        -------
        Timeseries
            A copy of the original Timeseries with values casted to the new
            type
        """
        new_series = self.copy()._series.astype(dtype)

        output = self._create_copy_timeseries_new_series(new_series,
                                                         self.name_series)

        return output

    def copy(self):
        """Retrieve a deep copy of the Timeseries instance

        Returns
        -------
        Timeseries
            A deep copy of the Timeseries
        """
        return copy.deepcopy(self)

    def resample(self, to_freq, use_original=False):
        """Resamples, or group by, values by index

        Will aggregate or partition series values by index, depending on the
        current frequency and target frequency. Checks to see if it is a
        downsampling or upsampling procedure, then uses the `strat_down` and
        `strat_up` properies to apply the appropriate methods.

        'to_freq' is directly passed to `pandas.resample`, but `Timeseries`
        only supports the calendar and time partitions (e.g. minute, monthly).

        Common string frequencies include:
        - 'S' for secondly
        - 'T' or 'min' for minutely
        - 'H' for hourly
        - 'B', 'C', or 'D' for daily
        - 'W' for weekly
        - 'M' or 'MS' for monthly
        - 'Q' for quarterly
        - 'Y' for yearly

        Parameters
        ----------
        to_freq : str, pandas.DateOffset
            The target frequency for the resampling procedure
        use_original : boolean, Optional
            If `False`, then resampling is done after cleaning and
            transformation procedures. If `True`, then resampling is performed
            without cleaning/transformation.

        Returns
        -------
        Timeseries
            A new Timeseries object (copy) with resampled data series
        """
        if use_original is True:
            series_resampled = self._series
        else:
            series_resampled = self.series

        series_resampled = series_resampled.resample(to_freq)

        if progutils.is_upsample(self.freq, to_freq) is True:

            if isinstance(self.strat_up, functions.UpsampleFunction):
                raise TypeError("UpsampleFunction not implemented")
            elif self.strat_up in UPSAMPLE_STR_APPLY_METHODS:
                resample_exec = series_resampled.apply(self.strat_up)
            elif self.strat_up in UPSAMPLE_STR_INTER_METHODS:
                resample_exec = series_resampled.interpolate(self.strat_up)

        if progutils.is_downsample(self.freq, to_freq) is True:
            # Remember, 'self.strat_up' is always a
            # functions.DownsampleFunction
            resample_exec = series_resampled.apply(self.strat_down)

        output = self._create_copy_timeseries_new_series(resample_exec,
                                                         self.name_series)

        return output

    def _create_copy_timeseries_new_series(self, series, series_name):
        """Creating new Timeseries instance with included strats"""
        return Timeseries(series, series_name=series_name,
                          strat_up=self.strat_up,
                          strat_down=self.strat_down,
                          transform=self.transform)
