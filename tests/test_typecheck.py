import pytest
import pandas
from progutils import progutils
import collections
import dataseries


class TestTypeCheck:

    def test_positional_one_type(self):
        @progutils.typecheck(x1=int)
        def example_fun(x1, x2):
            pass

        try:
            example_fun(5, 5)
        except TypeError:
            pytest.fail("Single positional, single type did not catch")

    def test_positional_wrong_type_one_type(self):
        @progutils.typecheck(x1=dataseries.Timeseries)
        def example_fun(x1, x2=3):
            pass

        with pytest.raises(TypeError):
            index = pandas.period_range('2019-01-01', periods=10, freq='B',
                                        name='date')
            x = pandas.Series(range(10), index=index, name='x1')
            example_fun(x)

    def test_positional_class_tuple_one_type(self):
        @progutils.typecheck(x1=(dataseries.Timeseries,))
        def example_fun(x1, x2=3):
            pass

        with pytest.raises(TypeError):
            index = pandas.period_range('2019-01-01', periods=10, freq='B',
                                        name='date')
            x = pandas.Series(range(10), index=index, name='x1')
            example_fun(x)

    def test_positional_multi_type(self):
        @progutils.typecheck(x1=(int, float))
        def example_fun(x1, x2):
            pass

            try:
                example_fun(5, 5)
            except TypeError:
                pytest.fail("Single positional, single type did not catch")

            try:
                example_fun(5.1, 5)
            except TypeError:
                pytest.fail("Single positional, single type did not catch")

    def test_keyword_multi_type(self):
        @progutils.typecheck(x1=(int, float))
        def example_fun(x1, x2):
            pass

            try:
                example_fun(sum, x1=50)
            except TypeError:
                pytest.fail("Single positional, single type did not catch")

    def test_catch_wrong_multi_type(self):
        @progutils.typecheck(x1=(int, float))
        def example_fun(x1, x2):
            pass

        with pytest.raises(TypeError):
            example_fun(x2=sum, x1=sum)

    # def test_catch_wrong_one_type(self):
    #     @progutils.typecheck(x1=float)
    #     def example_fun(x1, x2):
    #         pass

    #     # with pytest.raises(TypeError):
    #     example_fun(x2=sum, x1=5)

    def test_catch_none_as_not_a_type(self):
        with pytest.raises(TypeError):
            @progutils.typecheck(x1=(int, float, None))
            def example_fun(x1, x2):
                pass

    def test_allow_none_if_typed(self):
        @progutils.typecheck(x2=(int, float, type(None)))
        def example_fun(x1, x2):
            pass

        try:
            example_fun(50, None)
        except TypeError:
            pytest.fail("'NoneType' caught even though it is allowed")

    def test_allow_custom_class(self):

        class TestClass:
            def __init__(self):
                pass

        @progutils.typecheck(x2=(int, float, TestClass))
        def example_fun(x1, x2):
            pass

        try:
            example_fun(x1=10, x2=TestClass())
        except TypeError:
            pytest.fail("'TestClass' caught even though it is allowed")

    def test_allow_any_callable(self):
        @progutils.typecheck(x2=(int, float, collections.abc.Callable))
        def example_fun(x1, x2):
            pass

        try:
            example_fun(x1=10, x2=sum)
        except TypeError:
            pytest.fail("'sum' caught even though it is allowed")
