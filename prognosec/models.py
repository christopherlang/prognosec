import pandas
import numpy
import timeseries


ONEBDAY = pandas.tseries.offsets.BusinessDay(1)


class RandomWalk:
    def __init__(self, X=None):
        if X is not None:
            if isinstance(X, timeseries.Timeseries) is not True:
                raise TypeError("`X` needs to be a `Timeseries` object")

            self._X = X
            self.fit(X)
        else:
            self._X = None
            self._fdmean = None
            self._fdstd = None

    def fit(self, X=None):
        if self._X is None and X is None:
            raise ValueError("`X` is not set")

        if X is not None:
            self._X = X

        self._fdmean = self._X.X.diff(1).apply(lambda x: x.mean()).to_dict()
        self._fdstd = self._X.X.diff(1).apply(lambda x: x.std()).to_dict()

    def forecast(self, nperiods, series):
        xs = self._X.X[series].last('1' + self._X.X.index.freqstr)

        pred_series = numpy.random.normal(loc=0, scale=self._fdstd[series],
                                          size=nperiods)

        pred_series[0] = xs[0]
        pred_series = pred_series.cumsum()

        result = pandas.DataFrame.from_dict({series: pred_series})
        result.index = pandas.bdate_range(xs.index[0] + ONEBDAY,
                                          periods=nperiods,
                                          name=self._X.X.index.name)

        return timeseries.Timeseries(result, series)
