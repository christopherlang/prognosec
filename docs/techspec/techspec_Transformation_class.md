# Technical Design Specification - Transformation class

A `Transformation` instance is an object that stores an ordered sequence of
transformation functions, called a `procedure`, to be applied to an `array_like`
object. A `Transformation` instance stores only one such sequence, intended to
be stored in an `DSeries` for automated transformation. The `Transformation` object
does not store series data, and it's sequence of transformation is accessed through
an `apply` method.

## Dependencies

### Third-party libraries
* `numpy` : For access to `numpy.ndarray`
* `pandas` : For access to `pandas.Series`

### Local libraries
* `functions` : For a standardized set of transformation functions

## Functional Requirements
The following outlines the class' features, behaviors, and minimum definitions.

### Features and Behaviors
* A `procedure` is directly accessible through as a `property`
* `procedure` is a `list`
* A `procedure` can be modified outside the object, though not recommended
* Handles the modification of `procedure` in-place:
    - Adding a new transformation function to a `procedure`
    - Dropping a transformation function in `procedure`
    - Inserting a new transformation function to a `procedure`
    - Replacing a `procedure`'s sequence of transformation functions
    - Clearing a `procedure`'s sequence of transformation functions
* If no transformations has been added, property `procedure` is empty `list`
* Adding new transformation functions equates to the order of transformations
* Does not handle data types, relying on transformation functions only
* If no transformation is added, series passed in is pass out unmodified
* Always return a `pandas.Series` when asked
* Always return a `numpy.ndarray` when asked

### Minimum definitions
The following class attributes (i.e. properties and methods) **must** be defined.

#### Properties
Instance `property` should be computed and/or created on the fly to enable up-to-date information
    
| Name          | Setter    | Return                            | Description                                                                           |
|-------------  |--------   |---------------------------------  |-------------------------------------------------------------------------------------  |
| `procedure`   | Yes       | `list[function]`                  | The storage sequences of transformation functions, in order                           |
| `size`        | No        | `int`                             | The number of stored transformation functions                                         |
| `empty`       | No        | `bool`                            | If `True`, `Transformation` has transformation functions. Otherwise `False`           |
| `plan`        | No        | `list[tuple[int, str]]`, `None`   | Shows the order of function application `Transformation` will perform when executed   |

#### `property` setters
All instance `property` must type checked, raising errors otherwise

| Name          | Input Type        | Description                                                                       | Errors                            |
|-------------  |------------------ |--------------------------------------------------------------------------------   |-------------------------------    |
| `procedure`   | `list[function]`  | Replace all `procedure` with a new sequence of functions. Will be type checked    | Raise error on incorrect type     |

#### `Transformation` modification methods

| Name              | Return    | Description                                                                                                           | Errors                |
|----------------   |--------   |--------------------------------------------------------------------------------------------------------------------   |--------------------   |
| `add`             | N/A       | Add a new transformation function to a `Transformation`. Function is added to the end of the sequence of functions    | None                  |
| `drop`            | N/A       | Drop a transformation function by index                                                                               | Out of bound error    |
| `drop_first`      | N/A       | Same as `drop`, but drops the first transformation function                                                           | Same as `drop`        |
| `drop_last`       | N/A       | Same as `drop`, but drops the last transformation function                                                            | Same as `drop`        |
| `insert`          | N/A       | Add a new transformation function to a `procedure` by specifying the index position of the sequence                   | Out of bound error    |
| `insert_first`    | N/A       | Same as `insert`, but inserts the provided transformation function as the first function in the sequence              | Same as `insert`      |
| `insert_last`     | N/A       | Same as `insert`, but inserts the provided transformation function as the last function in the sequence               | Same as `insert`      |
| `clear`           | N/A       | Clears all transformation function                                                                                    | None                  |

#### The `apply` method

| Name      | Return                            | Description                                                                                                                                                                                                                                                                                                                                               |
|---------  |---------------------------------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  |
| `apply`   | `numpy.ndarray`, `pandas.Series`  | Applies a `procedure` to the provided data array. The actual return type is dependent on the input type and behavior of the transformation function, but should target the return types provided. Should raise error `procedure` doesn't exist, and other errors from transformation functions. A check on `len(input) == len(output)` should performed   |

#### Miscellaneous

| Name      | Return            | Description                                           |
|--------   |------------------ |-----------------------------------------------------  |
| `copy`    | `Transformation`  | Return a deep copy of the `Transformation` instance   |

## Glossary
### Procedure
A `procedure` is a sequence of transformation function
### Transformation function
A transformation function is a function that takes an array-like object, perform
some operation on it, and returns a `numpy.array` of the **same size** as the
original input