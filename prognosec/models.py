import pandas
import numpy
import prognosec.timeseries as timeseries
from statsmodels.tsa.arima_model import ARIMA


ONEBDAY = pandas.tseries.offsets.BusinessDay(1)


class RandomWalk:
    def __init__(self, tsobj=None):
        if tsobj is not None:
            if isinstance(tsobj, timeseries.Timeseries) is not True:
                raise TypeError("`tsobj` needs to be a `Timeseries` object")

            self._ts = tsobj
            self.fit(tsobj)
        else:
            self._params = dict()  # key by series
            self._models = dict()  # key by series, store model as value
            self._ts = None

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, params):
        if isinstance(params, dict) is not True:
            raise TypeError("'params' must be of type dict")
        if len(params) != len(self._params):
            raise ValueError(f"len(params) does is not {len(self._params)}")
        if len(set([len(i) for i in params.values()])) is not True:
            raise ValueError("All series in 'params' is not correct")

        self._params = params

    @property
    def param_names(self):
        return tuple(self._params.keys())

    def fit(self, tsobj=None, append=False):
        if self._ts is None and tsobj is None:
            raise ValueError("`X` is not set")

        if tsobj is not None:
            if append is False:
                self._ts = tsobj
            else:
                self._ts.append(tsobj)

        for a_series in self._ts.series:
            mod = ARIMA(self._ts.X[[a_series]], (0, 1, 0))
            self._params[a_series] = (0, 1, 0)
            self._models[a_series] = mod.fit()

    def forecast(self, nperiods, series):
        q_nperiods = nperiods
        while True:
            new_dti = self._ts.periods.shift(q_nperiods)[-q_nperiods:]
            new_dti = self._ts._calendar.valid_days(new_dti[0], new_dti[-1],
                                                    tz=self._ts.timezone)

            if len(new_dti) != nperiods:
                q_nperiods += 1
            else:
                break

        results = self._models[series].forecast(nperiods)
        result = {
            series: results[0],
            'se': results[1],
            'lower': [i[0] for i in results[2]],
            'upper': [i[1] for i in results[2]]
        }
        result = pandas.DataFrame.from_dict(result)
        result.index = new_dti
        result.index.name = self._ts.periods.name

        return timeseries.TimeseriesForecast(result, series,
                                             self._ts._calendar,
                                             self._ts._na_strategy)
