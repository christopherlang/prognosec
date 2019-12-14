import pandas
import numpy
import plotly.express as px
import pandas_market_calendars as mcal
import sklearn.model_selection
import copy
import collections


ONEBDAY = pandas.tseries.offsets.BusinessDay(1)
CAL_NASDAQ = mcal.get_calendar('NASDAQ')
CAL_NYSE = mcal.get_calendar('NYSE')


def conform_calendar(dataframe, calendar, min_date=None, max_date=None):
    if base_freq(dataframe.index.freq) != 'day':
        raise ValueError("'dataframe' is not a frequency by day")

    if min_date or max_date:
        new_index = calendar.valid_days(min_date, max_date,
                                        tz=dataframe.index.tz)
    else:
        new_index = calendar.valid_days(dataframe.index.min(),
                                        dataframe.index.max(),
                                        tz=dataframe.index.tz)

    return dataframe.reindex(index=new_index, method=None)


def merge_series(left, right, how='left', freq=None):
    """Join ExogSeries into an DataFrame

    Parameters
    ----------
    left, right: ExogSeries
    how : {'left', 'right', 'inner', 'outer'}
    freq: {str, offset}
        Set time frequency for both series after joining

    Returns
    -------
    DataFrame
    """
    if freq:
        left = left.resample(freq).series
    else:
        left = left.series

    right = right.resample(freq).series

    output = pandas.merge(left, right, how=how, left_index=True,
                          right_index=True)

    # output = ExogFrame(output)

    return output
