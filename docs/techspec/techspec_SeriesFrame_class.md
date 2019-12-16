# Technical Design Specification - SeriesFrame class

A `SeriesFrame` instance is an object that stores several `Timeseries` data series. Imagine a
data frame, but each column is stored as a `Timeseries`. Keeping the columns separate primarily
enables a jagged data frame, with each `Timeseries` to have there own set of `NaN`, resampling, and
transformation behaviors.

Similar to `Timeseries`, `SeriesFrame` will be designed for simplified modeling automation by
leaning on sensible default values. But it would also include important analysis capabilities
specific for time series analysis.

## Dependencies

### Third-party libraries
  * `numpy` : For access to `numpy.ndarray`
  * `pandas` : For access to `pandas.Series`
  * `pytest` : For class testing
  * `pandas_market_calendars` : For stock trading calendars

### Local libraries
  * `functions`  : For a standardized set of transformation functions
  * `progutils`  : For `Transformation` and other utility functions
  * `exceptions` : For `prognosec` specific errors and exceptions

## Associated tests
  * `tests/test_SeriesFrame_class.py` : A `pytest` test for the class

## Functional Requirements
The following outlines the class' features, behaviors, and minimum definitions.

### Features and Behaviors
  * Instantiates with a `pandas.Series` or a `dict` of `Timeseries`
  * Stores all series as a `Timeseries`
  * Internalizes its own `DatetimeIndex`, `TimedeltaIndex`, or `PeriodIndex` for joining
  * Index integrity is strictly enforced
  * Supports `pandas_market_calendars` defined calendars
  * Can add, delete, replace, and rename `Timeseries`
  * Enforces R-like column naming scheme (i.e. alphanumeric, underscore, and can't start with digit)
  * Has resampling capabilities. More than one series can be selected, returning a copy

### Minimum definitions
The following class attributes (i.e. properties and methods) **must** be defined.

#### Properties
Instance `property` should be computed and/or created on the fly to enable up-to-date information
    
| Name            | Setter  | Return                                            | Description                                           |
|---------------  |-------- |-------------------------------------------------- |-----------------------------------------------------  |
| `frame`         | No      | `collections.OrderedDict`                         | Original `Timeseries` series                          |
| `size`          | No      | `int`                                             | The number of `Timeseries` stored                     |
| `index`         | Yes     | `DatetimeIndex`, `TimedeltaIndex`, `PeriodIndex`  | The primary index associated with the `SeriesFrame`   |
| `name_index`    | Yes     | `str`                                             | Name of the primary index                             |
| `name_series`   | No      | `list[str]`                                       | Name of all of the `Timeseries` objects               |
| `freq`          | No      | `pandas.DateOffset`                               | Frequency of the primary index                        |
| `value_index`   | No      | `numpy.ndarray`                                   | Values of the primary index                           |
| `dtype_index`   | No      | `numpy.dtype`                                     | Data type of the primary index                        |

#### `property` setters
All instance `property` must type checked, raising errors otherwise

| Name          | Input Type                                        | Description                 | Errors                          |
|-------------- |-------------------------------------------------- |---------------------------  |-------------------------------  |
| `index`       | `DatetimeIndex`, `TimedeltaIndex`, `PeriodIndex`  | Change primary index        | Raise error on incorrect type   |
| `name_index`  | `str`                                             | Change primary index name   | Raise error on incorrect type   |

#### Methods

| Name                  | Return                                                | Description                                                                               | Errors                            |
|---------------------  |-----------------------------------------------------  |------------------------------------------------------------------------------------------ |---------------------------------- |
| `_split_dataframe`    | `collections.OrderedDict[Timeseries]`                 | Accepts a `pandas.DataFrame` and splits it into `Timeseries`. Keyed on `Timeseries` name  | Unknown                           |
| `add`                 | N/A                                                   | Add a new `Timeseries` object                                                             | Error on wrong type               |
| `remove`              | N/A                                                   | Removes a `Timeseries` object by name                                                     | Error on missing key              |
| `replace`             | N/A                                                   | Replaces an existing `Timeseries` object by name                                          | Error on missing key, wrong type  |
| `access_series`       | `Timeseries`                                          | Get `Timeseries` by name, directly accessing it (no copy)                                 | Error on missing key              |
| `access_transform`    | `Transformation`                                      | Get `Transformation` associated by `Timeseries` name, directly accessing it (no copy)     | Error on missing key              |
| `access_strat_na`     | `str`, `scalar`, `dict`, `MissingValueFillFunction`   | Get `strat_na` property of `Timeseries` by name, directly accessing it (no copy)          | Error on missing key              |
| `access_strat_up`     | `str`, `UpsampleFunction`                             | Get `strat_up` property of `Timeseries` by name, directly accessing it (no copy)          | Error on missing key              |
| `access_strat_down`   | `DownsampleFunction`                                  | Get `strat_down` property of `Timeseries` by name, directly accessing it (no copy)        | Error on missing key              |
| `set_transform`       | N/A                                                   | Set a new `Transformation` object for a given series                                      | Error on missing key, wrong type  |
| `set_strat_na`        | N/A                                                   | Set a new `NaN` handling strategy for a given series                                      | Error on missing key, wrong type  |
| `set_strat_up`        | N/A                                                   | Set a new upsampling handling strategy for a given series                                 | Error on missing key, wrong type  |
| `set_strat_down`      | N/A                                                   | Set a new downsampling handling strategy for a given series                               | Error on missing key, wrong type  |
| `resample`            | `SeriesFrame`                                         | Resampling one or more `Timeseries` to a target frequency, or to primary index            | Various                           |