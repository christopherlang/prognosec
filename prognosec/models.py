import pandas
import numpy
import collections
import prognosec.timeseries as timeseries
from statsmodels.tsa.arima_model import ARIMA
import abc
import typing as ty


ONEBDAY = pandas.tseries.offsets.BusinessDay(1)


class Model(abc.ABC):
    """Abstract Model class

    Intended for subclassing

    Standardizes method and attribute interface for all models

    Attributes
    ----------
    ts : timeseries.Timeseries
    pseries : str
    params : ModelParameters
    param_names : array_like[str]

    Parameters
    ----------
    ts : timeseries.Timeseries
        The timeseries dataset containing the time series for forecasting
        and exogenous/independent variables
    """
    @abc.abstractmethod
    def __init__(self, ts: ty.Optional[timeseries.Timeseries] = None):
        self._ts = ts
        self._params = ModelParameters()
        self._models = dict()
        self._model_fit_converge = dict()

        self._model_name_prefix = ''
        self._model_version = 1
        self._package = ''

        if ts is not None:
            for a_series in ts.series:
                self._params.set_parameters(a_series)

        if self._ts is not None:
            self.fit(self._ts)

    @abc.abstractmethod
    def model_name(self, series: ty.Optional[str] = None) -> str:
        """Name of the model for a given series

        Provides the name of the model. Typically includes the model's
        shorthand name with important and influence parameters

        Parameters
        ----------
        series : str
            Name of the series

        Returns
        -------
        str
            The name of the model
        """
        series = self.pseries if series is None else series

        output = (self._model_name_prefix +
                  f'{self._params.get_parameters(series)}')

        return output

    @property
    def ts(self) -> ty.Union[timeseries.Timeseries, None]:
        """The `Timeseries` object used for fitting

        Returns
        -------
        timeseries.Timeseries, None
            If `None` then the model was not set up with a `Timeseries` object
        """
        return self._ts

    @property
    def pseries(self) -> ty.Union[str, None]:
        """The primary series

        Returns
        -------
        str, None
            The name of the primary series for both the model and the stored
            `timeseries.Timeseries`
        """
        return self._ts.primary_series

    @property
    def params(self):
        """Parameters of the model

        Returns
        -------
        models.ModelParameters
        """
        return self._params

    def change_parameter(self, series, param):
        """Setting a model's hyperparameters

        Parameters
        ----------
        series : str
            Name of the series
        param : array_like
            The parameter array. Please see the internal `ModelParameters` for
            more information about how it is set up
        """
        self._params.set_parameters(series, param)

    @params.setter
    @abc.abstractproperty
    def params(self, params):
        pass
        # if isinstance(params, dict) is not True:
        #     raise TypeError("'params' must be of type dict")
        # if len(params) != len(self._params):
        #     raise ValueError(f"len(params) does is not {len(self._params)}")
        # if len(set([len(i) for i in params.values()])) is not True:
        #     raise ValueError("All series in 'params' is not correct")

        # self._params = params

    @property
    def param_names(self) -> ty.Sequence:
        """The names of the parameters

        Return
        ------
        array_like
            A sequence containing the names of the parameter
        """
        return self._params.param_names

    @abc.abstractmethod
    def fit(self, tsobj: ty.Optional[timeseries.Timeseries] = None,
            append: ty.Optional[bool] = False):
        """Fit a `Timeseries` to the model

        Parameters
        ----------
        tsobj : timeseries.Timeseries, optional
            A time series data for fitting. If `None`, it is assumed that the
            object already instantiated with a `Timeseries`
        append : bool
            If `True`, the provided `Timeseries` will be appended to the
            internally stored `Timeseries`
        """
        pass

    @abc.abstractmethod
    def fitted(self, series: ty.Optional[str] = None) -> ty.Sequence[float]:
        """Retrieve the in-sample fitted values

        Parameters
        ----------
        series : str, optional
            Name of the series. If `None`, will use the primary series

        Returns
        -------
        array_like[float]
            An array containing the in-sample fitted values
        """
        pass

    @abc.abstractmethod
    def summary(self, series: ty.Optional[str] = None) -> ty.Dict:
        """Gather a summary of model statistics

        Parameters
        ----------
        series : str, optional
            Name of the series. If `None`, will use the primary series

        Returns
        -------
        dict
            Keyed on name and valued on the various statistics and other
            model information
        """
        pass

    def mse(self, series: ty.Optional[str] = None) -> float:
        """Calculate the in-sample mean squared error (MSE)

        Parameters
        ----------
        series : str, optional
            Name of the series. If `None`, will use the primary series

        Returns
        -------
        float
            The in-sample MSE
        """
        series = self.pseries if series is None else series

        diff = self.fitted(series) - self.ts.get_series_pandas(series)

        return diff.dropna().pow(2).mean()

    @abc.abstractmethod
    def aic(self, series: ty.Optional[str] = None) -> float:
        """Calculate the model's Akaike Information Criterion (AIC)

        Parameters
        ----------
        series : str, optional
            Name of the series. If `None`, will use the primary series

        Returns
        -------
        float
            The model's AIC
        """
        pass

    @abc.abstractmethod
    def bic(self, series: ty.Optional[str] = None) -> float:
        """Calculate the model's Bayesian Information Criterion (BIC)

        Parameters
        ----------
        series : str, optional
            Name of the series. If `None`, will use the primary series

        Returns
        -------
        float
            The model's BIC
        """
        pass

    def plot(self, series: ty.Optional[str] = None):
        """Plot the time serie's on a line chart

        Uses `Plotly` as the charting library

        Parameters
        ----------
        series : str
            Name of the series
        """
        series = self.pseries if series is None else series
        fittedvalues = self.fitted(series)
        output = self.ts.get_series_pandas(series).to_frame()
        output['fittedvalues'] = fittedvalues
        output = output.dropna()

        output = timeseries.TimeseriesFitted(output, series,
                                             self.ts._calendar,
                                             self.ts._na_strategy)

        return output

    def forecast(self, nperiods: int,
                 series: ty.Optional[str] = None) -> timeseries.Timeseries:
        """Forecast future values of the time series

        Parameters
        ----------
        nperiods : int
            Number of values to forecast into the future
        series : str, optional
            Name of the series. If `None`, will use the primary series

        Returns
        -------
        timeseries.Timeseries
            A `Timeseries` object with the following series:
            - `series_name`: the actual name of the series being forecasted
            - se: the standard error of the forecast
            - lower: the lower limit of the confidence interval
            - upper: the upper limit of the confidence interval
        """
        q_nperiods = nperiods
        series = self._ts.primary_series if series is None else series

        while True:
            new_dti = self._ts.periods.shift(q_nperiods)[-q_nperiods:]
            new_dti = self._ts._calendar.valid_days(new_dti[0], new_dti[-1],
                                                    tz=self._ts.timezone)

            if len(new_dti) != nperiods:
                q_nperiods += 1
            else:
                break

        result = self._model_forecast(nperiods, series)
        result.index = new_dti
        result.index.name = self.ts.periods.name

        return timeseries.Timeseries(result, series, self.ts._calendar,
                                     self.ts._na_strategy)

    @abc.abstractmethod
    def _model_forecast(self, nperiods: int,
                        series: ty.Optional[str] = None) -> pandas.DataFrame:
        """Forecast Values

        Columns of the dataframe should have the following names:
        - `series_name`: the actual name of the series being forecasted
        - se: the standard error of the forecast
        - lower: the lower limit of the confidence interval
        - upper: the upper limit of the confidence interval
        """
        pass

    def fit_forecast(self, test_ds: timeseries.Timeseries,
                     series: ty.Optional[str] = None) -> dict:
        """Calculate the Mean Squared Error (MSE) of test data

        This will forecast future values and compare them to the provided
        test `Timeseries`. The MSE is then calculated.

        Parameters
        ----------
        test_ds : timeseries.Timeseries
            The test time series dataset
        series : str, optional
            Name of the series. If `None`, will use the primary series

        Returns
        -------
        dict
            Keyed on:
            - mse : Mean squared error vs. test dataset
            - var : The variance of the squared error
        """
        series = self.pseries if series is None else series
        nperiods = len(test_ds.X)

        forecasted_series = self.forecast(nperiods, series)
        diff = (forecasted_series.get_series_pandas(series) -
                test_ds.get_series_pandas(series))
        squared_error = diff.dropna().pow(2)

        return {'mse': squared_error.mean(), 'var': squared_error.var()}


class Arima(Model):
    """Autoregressive integrated moving average (ARIMA) time series model

    Implements the non-seasonal ARIMA time series model for forecasting
    purposes. The main parameters is the (p, d, q) order:
    - p : Number of time lags (order of autoregressive model)
    - d : Degree of differencing
    - q : Order of moving average

    Model is provided by the `statsmodels` library

    Attributes
    ----------
    ts : timeseries.Timeseries
    pseries : str
    params : ModelParameters
    param_names : array_like[str]

    Parameters
    ----------
    order : array_like[int]
        The (p, d, q) order of the ARIMA model
    ts : timeseries.Timeseries
        The timeseries dataset containing the time series for forecasting
        and exogenous/independent variables
    """

    def __init__(self, order: ty.Sequence[int],
                 ts: ty.Optional[timeseries.Timeseries] = None):
        self._ts = ts
        self._params = ModelParameters()
        self._models = dict()
        self._model_fit_converge = dict()

        self._model_name_prefix = 'ARIMA'
        self._model_version = 1
        self._package = 'statsmodels'
        self._order = order

        if ts is not None:
            for a_series in ts.series:
                self._params.set_parameters(a_series, order)

        if self._ts is not None:
            self.fit(self._ts)

    def model_name(self, series: ty.Optional[str] = None) -> str:
        series = self.pseries if series is None else series

        return (self._model_name_prefix +
                f'{self._params.get_parameters(series)}')

    @Model.params.setter
    def params(self, params):
        self._params = params

    def fit(self, tsobj: ty.Optional[timeseries.Timeseries] = None,
            append: ty.Optional[bool] = False):
        if self._ts is None and tsobj is None:
            raise ValueError("`X` is not set")

        if self._ts is None and tsobj is not None:
            for a_series in tsobj.series:
                self._params.set_parameters(a_series, self._order)

        if tsobj is not None:
            if append is False:
                self._ts = tsobj
            else:
                self._ts.append(tsobj)

        for a_series in self._ts.series:
            mod = ARIMA(self._ts.X[[a_series]],
                        self._params.get_parameters(a_series))

            self._models[a_series] = mod.fit()

            did_model_converge = self._models[a_series].mle_retvals
            did_model_converge = did_model_converge['converged']
            self._model_fit_converge[a_series] = did_model_converge

    def fitted(self, series: ty.Optional[str] = None) -> ty.Sequence[float]:
        series = self.pseries if series is None else series

        return self._models[series].predict(typ='levels')

    def summary(self, series: ty.Optional[str] = None) -> ty.Dict:
        series = self.pseries if series is None else series

        output = collections.OrderedDict()
        output['Dependent'] = series
        output["Model"] = self.model_name(series)
        output['# of Obs.'] = len(self._ts.X)
        output['Model Converged'] = self._model_fit_converge[series]
        output['Log Likelihood'] = self._models[series].llf
        output['AIC'] = self.aic(series)
        output['BIC'] = self.bic(series)
        output['MSE'] = self.mse(series)

        return output

    def aic(self, series: ty.Optional[str] = None) -> float:
        series = self.pseries if series is None else series

        return self._models[series].aic

    def bic(self, series: ty.Optional[str] = None) -> float:
        series = self.pseries if series is None else series

        return self._models[series].bic

    def _model_forecast(self, nperiods: int,
                        series: ty.Optional[str] = None) -> pandas.DataFrame:
        """Forecast Values

        Columns of the dataframe should have the following names:
        - `series_name`: the actual name of the series being forecasted
        - se: the standard error of the forecast
        - lower: the lower limit of the confidence interval
        - upper: the upper limit of the confidence interval
        """
        results = self._models[series].forecast(nperiods)
        result = {
            series: results[0],
            'se': results[1],
            'lower': [i[0] for i in results[2]],
            'upper': [i[1] for i in results[2]]
        }
        result = pandas.DataFrame.from_dict(result)

        return result


class RandomWalk(Arima):
    def __init__(self, ts: ty.Optional[timeseries.Timeseries] = None):
        super().__init__((0, 1, 0), ts)


class ModelParameters:
    """Storage of model parameters

    Primarily used to store model parameters by series in a
    `timeseries.Timeseries` object.

    Attributes
    ----------
    series_names
    param_names
    params

    Parameters
    ----------
    series_name : array_like[str], optional
        Names of the series to store the parameters for
    param_names : array_like[str], optional
        Names of parameters. If provided, this determines the size of the
        arrays that stores the parameters
    """

    def __init__(self, series_names=None, param_names=None):
        self._series_names = series_names
        self._param_names = param_names

        self._params = collections.OrderedDict()

        if series_names is not None:
            for serie_name in series_names:
                self._params[serie_name] = None

    @property
    def series_names(self):
        """array_like[str], None : Names of the series stored"""
        return self._series_names

    @property
    def param_names(self):
        """array_like[str], None : Names of the parameters stored"""
        return self._param_names

    @param_names.setter
    def param_names(self, names):
        if self._param_names is not None:
            if len(names) != len(self._param_names):
                raise ValueError("Length of 'names' is not expected")

        self._param_names = names

    @property
    def params(self):
        """dict[str, array_like] : Raw series and parameters stored"""
        return self._params

    def set_parameters(self, series, param_array):
        """Set a series' model parameters

        This is wholesale setting of a parameter array by series name

        Parameters
        ----------
        series : str
            Name of the series
        param_array : array_like
            An array of the new set of parameters
        """
        self._params[series] = param_array

    def get_parameters(self, series):
        """Retrieve a series' model parameters

        Parameters
        ----------
        series : str
            Name of the series

        Returns
        -------
        array_like
            The parameter array
        """
        return self._params[series]


def cv_timeseries(time_series, model, series=None, k=10, **kwargs):
    # TODO deal with this
    # if isinstance(time_series, timeseries.Timeseries) is not True:
    #     raise TypeError("'time_series' is not a Timeseries object")

    output = list()
    for ds, kth in zip(time_series.split(), range(10)):
        mod = model(**kwargs)
        mod.fit(ds[0])

        split_stats = mod.fit_forecast(ds[1], series=series)
        rec = {
            'k': kth, 'series': mod._ts.primary_series,
            'model': mod.model_name(series),
            'train_size': len(ds[0].X),
            'test_size': len(ds[1].X),
            'mse': split_stats['mse'], 'var': split_stats['var'],
            'AIC': mod.aic(series),
            'BIC': mod.bic(series)}

        output.append(rec)

    col_headers = ['k', 'series', 'model', 'train_size', 'test_size', 'mse',
                   'var', 'AIC', 'BIC']
    output = pandas.DataFrame.from_records(output, columns=col_headers)

    return output
