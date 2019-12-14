# Technical Design Specification - Timeseries class

A `Timeseries` instance is an object that stores an ordered sequence of
values. It is a wrapper around `pandas.Series` but has several attributes and
methods conducive for modeling automation and time series analysis. Including,
but not limited to time series analysis, automated (via default) resampling,
and simplified sequences of transformations (via `Transformation` class).

## Dependencies

### Third-party libraries
  * `numpy` : For access to `numpy.ndarray`
  * `pandas` : For access to `pandas.Series`
  * `pytest` : For class testing

### Local libraries
  * `functions` : For a standardized set of transformation functions
  * `progutils` : For `Transformation` and other utility functions

## Associated tests
  * `tests/test_Timeseries_class.py` : A `pytest` test for the class

## Functional Requirements
The following outlines the class' features, behaviors, and minimum definitions.

### Features and Behaviors
  * Convert input ordered sequence into a `pandas.Series`
  * Support inputs of ordered sequences of values
  * Internally store `pandas.Series` with a `DatetimeIndex`, `TimedeltaIndex`, or `PeriodIndex`
  * Inputs must have a coercible index to one of the three above
  * Index integrity is enforced
  * Have a resampling capability (change frequency)
  * Resampling must always return a series with `DatetimeIndex`, `TimedeltaIndex`, or `PeriodIndex`
  * Automatically understands whether it is upsampling, downsampling, or none at all
  * Stores its own upsampling/downsampling methods
  * Infers, understands, and handles statistical data types:
      - Ordinal
      - Categorical
      - Binary
      - Binomial
      - Real numbers:
          + Counts
          + Intervals
          + Ratios
  * Inference of statistical data types for integers and floats will default to intervals
  * Maps standard data types to statistical data types:
      - TBD
  * Uses `pandas` data types to infer statistical data types:
      - TBD
  * Real valued transformations (i.e. log) will map to either intervals or ratios
  * User will be able to redefine statistical data types
  * Stores its own `Transformation` instance
  * Modifications to the time series will always return a copy of `Timeseries`
  * Transformations is done on the fly via `series` property
  * Transformation editing is done via `transform` property
  * Handles initial `NaN` (construction) and stores this result as the series
  * Possesses time series analysis related functionality


### Minimum definitions
The following class attributes (i.e. properties and methods) **must** be defined.

#### Properties
Instance `property` should be computed and/or created on the fly to enable up-to-date information
    
| Name            | Setter  | Return                                            | Description                               |
|---------------- |-------- |-------------------------------------------------- |-----------------------------------------  |
| `series`        | No      | `pandas.Series`                                   | The actual time series, including index   |
| `size`          | No      | `int`                                             | The number of data points in the series   |
| `index`         | No      | `DatetimeIndex`, `TimedeltaIndex`, `PeriodIndex`  | The index associated with the series      |
| `name_index`    | Yes     | `str`, `None`                                     | Name of the index                         |
| `name_series`   | Yes     | `str`                                             | Name of the series                        |
| `freq`          | No      | `pandas.tseries.offsets.*`                        | Frequency of the index                    |
| `value_series`  | No      | `numpy.ndarray`                                   | Series data                               |
| `value_index`   | No      | `numpy.ndarray`                                   | Index data series                         |
| `dtype`         | No      | `numpy.dtype`                                     | `numpy` data type of series               |
| `dtype_index`   | No      | `numpy.dtype`                                     | `numpy` data type of index                |
| `stype`         | No      | `str`                                             | The mapped statistical data type          |
| `strat_na`      | Yes     | `str`, `function`                                 | Method for handling `NaN`                 |
| `strat_up`      | Yes     | `str`, `function`                                 | Method for upsampling                     |
| `strat_down`    | Yes     | `str`, `function`                                 | Method for downsampling                   |
| `transform`     | Yes     | `Transformation`                                  | Instance of `Transformation`              |

#### `property` setters
All instance `property` must type checked, raising errors otherwise

| Name          | Input Type          | Description                                 | Errors                          |
|-------------- |-------------------  |-------------------------------------------- |-------------------------------  |
| `name_index`  | `str`               | Change series name                          | Raise error on incorrect type   |
| `name_series` | `str`               | Change index name                           | Raise error on incorrect type   |
| `strat_na`    | `str`, `function`   | Change the method for `NaN` handling        | Raise error on incorrect type   |
| `strat_up`    | `str`, `function`   | Change the method for upsampling            | Raise error on incorrect type   |
| `strat_down`  | `str`, `function`   | Change the method for downsampling          | Raise error on incorrect type   |
| `transform`   | `Transformation`    | Overwrite we new `Transformation` instance  | Raise error on incorrect type   |

#### Methods

| Name                  | Return                  | Description                                                                                     | Errors                                                    |
|---------------------- |-----------------------  |------------------------------------------------------------------------------------------------ |---------------------------------------------------------  |
| `_verify_new_series`  | N/A                     | Accepts a `pandas.Series` input and check correctness, raising errors if not                    | Various                                                   |
| `_clean_series`       | `pandas.Series`         | Accepts a `pandas.Series` input and performs cleaning operations, such as `NaN` handling        | Unknown                                                   |
| `has_na`              | `bool`                  | Check if a the series has `NaN`. If user inputted `asis` `NaN` strategy, this should be `True`  | None                                                      |
| `size_na`             | `int`                   | Counts the number of `NaN` found                                                                | None                                                      |
| `is_na`               | `numpy.ndarray[bool]`   | Returns an array of booleans, positional on `NaN` location in series                            | None                                                      |
| `cast`                | `Timeseries`            | Cast the instance's `dtype` (`stype` updated as well) and return a copy                         | Error on incompatible cast                                |
| `resample`            | `Timeseries`            | Resamples the series to target frequency. Handles up/down sampling, and returns a copy          | Error on incompatible frequency, or incompatible method   |
| `copy`                | `Timeseries`            | Returns a deep copy of the instance                                                             | None                                                      |
