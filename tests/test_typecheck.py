import pytest
import datetime
import pandas
from progutils import progutils
from progutils import typechecks
import collections
import dataseries


class TestTypeCheck:

    def test_positional_one_type(self):
        @typechecks.typecheck(x1=int)
        def example_fun(x1, x2):
            pass

        try:
            example_fun(5, 5)
        except TypeError:
            pytest.fail("Single positional, single type did not catch")

    def test_positional_wrong_type_one_type(self):
        @typechecks.typecheck(x1=dataseries.Timeseries)
        def example_fun(x1, x2=3):
            pass

        with pytest.raises(TypeError):
            index = pandas.period_range('2019-01-01', periods=10, freq='B',
                                        name='date')
            x = pandas.Series(range(10), index=index, name='x1')
            example_fun(x)

    def test_positional_class_tuple_one_type(self):
        @typechecks.typecheck(x1=(dataseries.Timeseries,))
        def example_fun(x1, x2=3):
            pass

        with pytest.raises(TypeError):
            index = pandas.period_range('2019-01-01', periods=10, freq='B',
                                        name='date')
            x = pandas.Series(range(10), index=index, name='x1')
            example_fun(x)

    def test_positional_multi_type(self):
        @typechecks.typecheck(x1=(int, float))
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
        @typechecks.typecheck(x1=(int, float))
        def example_fun(x1, x2):
            pass

            try:
                example_fun(sum, x1=50)
            except TypeError:
                pytest.fail("Single positional, single type did not catch")

    def test_catch_wrong_multi_type(self):
        @typechecks.typecheck(x1=(int, float))
        def example_fun(x1, x2):
            pass

        with pytest.raises(TypeError):
            example_fun(x2=sum, x1=sum)

    # def test_catch_wrong_one_type(self):
    #     @typechecks.typecheck(x1=float)
    #     def example_fun(x1, x2):
    #         pass

    #     # with pytest.raises(TypeError):
    #     example_fun(x2=sum, x1=5)

    def test_catch_none_as_not_a_type(self):
        with pytest.raises(TypeError):
            @typechecks.typecheck(x1=(int, float, None))
            def example_fun(x1, x2):
                pass

    def test_allow_none_if_typed(self):
        @typechecks.typecheck(x2=(int, float, type(None)))
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

        @typechecks.typecheck(x2=(int, float, TestClass))
        def example_fun(x1, x2):
            pass

        try:
            example_fun(x1=10, x2=TestClass())
        except TypeError:
            pytest.fail("'TestClass' caught even though it is allowed")

    def test_allow_any_callable(self):
        @typechecks.typecheck(x2=(int, float, collections.abc.Callable))
        def example_fun(x1, x2):
            pass

        try:
            example_fun(x1=10, x2=sum)
        except TypeError:
            pytest.fail("'sum' caught even though it is allowed")


class TestParseTyperuleFunction:

    @pytest.fixture
    def parse_typerule(self):
        return typechecks.parse_typerule

    def test_module_has_function(self):
        assert hasattr(typechecks, 'parse_typerule')

    def test_singleton_input_int(self, parse_typerule):
        assert parse_typerule('50', 'int') == 50

    def test_singleton_input_float(self, parse_typerule):
        assert parse_typerule('50.7', 'float') == 50.7

    def test_singleton_input_date(self, parse_typerule):
        expected = datetime.date(2019, 1, 1)
        assert parse_typerule('2019-01-01', 'date') == expected

    def test_singleton_input_datetime(self, parse_typerule):
        expected = datetime.datetime(2019, 1, 1, 10, 20, 30)
        parsed_version = parse_typerule('2019-01-01T10:20:30',
                                        'datetime_second')
        assert parsed_version == expected

    def test_singleton_input_int_or_str_no_primary(self, parse_typerule):
        assert parse_typerule('50', 'int|str') == 50
        assert parse_typerule('50', 'str|int') == '50'

    def test_singleton_input_int_or_str_with_primary(self, parse_typerule):
        assert parse_typerule('50', 'int|~str') == '50'
        assert parse_typerule('50', 'str|~int') == 50

    def test_singleton_input_bool(self, parse_typerule):
        assert parse_typerule(True, 'bool') is True
        assert parse_typerule('true', 'bool') is True

    def test_tuple_singleton_input_str(self, parse_typerule):
        assert parse_typerule((1, 2), 'tuple[str]') == ('1', '2')

    def test_tuple_set_int(self, parse_typerule):
        dataseries = [set([1, 2]), set([3, 4]), set([5])]
        expected = (['1', '2'], ['3', '4'], ['5'])
        typespec = 'tuple[list[str]]'
        assert parse_typerule(dataseries, typespec) == expected

    def test_tuple_str_or_int_no_primary(self, parse_typerule):
        parsed = parse_typerule(('10', '20', '30'), 'tuple[int|str]')
        expected = (10, 20, 30)
        assert parsed == expected

    def test_tuple_str_or_int_with_primary(self, parse_typerule):
        parsed = parse_typerule((10, 20, 30), 'tuple[int|~str]')
        expected = ('10', '20', '30')
        assert parsed == expected

    def test_tuple_mixed_str_or_int_no_primary(self, parse_typerule):
        parsed = parse_typerule(('10', '20', 30), 'tuple[int|~str]')
        expected = ('10', '20', '30')
        assert parsed == expected

    def test_dict_str_int_or_float(self, parse_typerule):
        parsed = parse_typerule({'a': 50, 'b': '100'}, 'dict[str, ~int|float]')
        expected = {'a': 50, 'b': 100}
        assert parsed == expected

    def test_dict_str_tuple_int_or_float(self, parse_typerule):
        data = {
            'a': (50, 100),
            'b': (50, 100.65),
        }
        parsed = parse_typerule(data, 'dict[str, tuple[int|~float]]')
        expected = {
            'a': (50.0, 100.0),
            'b': (50.0, 100.65),
        }

        assert parsed == expected

    def test_dict_str_int(self, parse_typerule):
        dataseries = {'a': '60', 'b': '100'}
        expected = {'a': 60, 'b': 100}
        typespec = 'dict[str, int]'
        assert parse_typerule(dataseries, typespec) == expected

    def test_dict_str_tuple_int(self, parse_typerule):
        dataseries = {'a': tuple(['60']), 'b': tuple(['100'])}
        expected = {'a': tuple([60]), 'b': tuple([100])}
        typespec = 'dict[str, tuple[int]]'
        assert parse_typerule(dataseries, typespec) == expected

    def test_tuple_list_dict_str_int(self, parse_typerule):
        dataseries = [[{'a': '4', 'b': '6'}, {'c': '10'}], [{'z': '102'}]]
        expected = ([{'a': 4, 'b': 6}, {'c': 10}], [{'z': 102}])
        typespec = 'tuple[list[dict[str, int]]]'
        assert parse_typerule(dataseries, typespec) == expected

    def test_tuple_list_dict_str_set_int(self, parse_typerule):
        dataseries = (
            [
                [{'a': set(['70', '50', '100'])}, {'a': set(['17', '59'])}],
                [{'a': set(['70', '79'])}, {'a': set(['100', '95'])}],
                [{'a': set(['700', '7'])}, {'a': set(['55', '69', '90'])}]
            ]
        )
        expected = (
            (
                [{'a': set([70, 50, 100])}, {'a': set([17, 59])}],
                [{'a': set([70, 79])}, {'a': set([100, 95])}],
                [{'a': set([700, 7])}, {'a': set([55, 69, 90])}]
            )
        )
        typespec = 'tuple[list[dict[str, set[int]]]]'
        assert parse_typerule(dataseries, typespec) == expected


class TestConformsToTypespecFunction:

    def test_conforms_int(self):
        assert typechecks.conforms_to_typerule(50, 'int')

    def test_conforms_date(self):
        assert typechecks.conforms_to_typerule('2019-01-01', 'date')

    def test_conforms_datetime(self):
        assert typechecks.conforms_to_typerule('2019-01-01T09:10:10',
                                               'datetime_second')

    def test_conforms_datetime_utc(self):
        assert typechecks.conforms_to_typerule('2019-01-01T09:10:10Z',
                                               'datetime_second')

    def test_conforms_datetime_obj(self):
        the_datetime = datetime.datetime.utcnow().replace(microsecond=0)

        assert typechecks.conforms_to_typerule(the_datetime, 'datetime_second')

    def test_conforms_date_obj(self):
        the_date = datetime.datetime.utcnow().date()

        assert typechecks.conforms_to_typerule(the_date, 'datetime_second')

    def test_conforms_int_or_str(self):
        assert typechecks.conforms_to_typerule(50, 'int|str')
        assert typechecks.conforms_to_typerule('50', 'int|str')

    def test_conforms_int_or_str_float_input(self):
        assert typechecks.conforms_to_typerule(56.0, 'int|str') is False

    def test_conforms_list_int_or_str_singular_input(self):
        assert typechecks.conforms_to_typerule([50, 100, 500], 'list[int|str]')
        assert typechecks.conforms_to_typerule(['50', '100'], 'list[int|str]')

    def test_conforms_list_int_or_str_mixed_input(self):
        assert typechecks.conforms_to_typerule([50, '100', 500],
                                               'list[int|str]')

    def test_conforms_list_tuple_int_or_float(self):
        data = [
            (100, 200, 300),
            (24.3, 78.8, 400.10),
            (24.3, 100, 10)
        ]
        assert typechecks.conforms_to_typerule(data, 'list[tuple[int|float]]')

    def test_conforms_list_int(self):
        assert typechecks.conforms_to_typerule([50, 100], 'list[int]')

    def test_conforms_list_dict_int(self):
        data = [
            {'a': 500, 'b': 1090},
            {'c': 1000, 'd': 453, 'e': 9870}
        ]
        assert typechecks.conforms_to_typerule(data, 'list[dict[str, int]]')

    def test_conforms_tuple_list_str(self):
        data = (
            ['hey', 'this', 'is', 'test'],
            ['this', 'is', 'another'],
            ['singular']
        )
        assert typechecks.conforms_to_typerule(data, 'tuple[list[str]]')

    def test_conforms_tuple_empty(self):
        assert typechecks.conforms_to_typerule(tuple(), 'tuple[str]')

    def test_conforms_dict_tuple_bool(self):
        data = {
            'hey': (True, False, True),
            'there': (False, False, True, True, False)
        }
        assert typechecks.conforms_to_typerule(data, 'dict[str, tuple[bool]]')

    def test_conforms_dict_dict_float(self):
        data = {
            'a': {'b': 5.6, 'c': 7.8},
            'b': {'g': 5.6, 'h': 7.8}
        }

        typespec = 'dict[str, dict[str, float]]'
        assert typechecks.conforms_to_typerule(data, typespec)

    def test_conforms_dict_dict_tuple_bool(self):
        data = {
            'a': {'b': (False, True), 'c': (False, True, True)},
            'b': {'g': (False, True, True), 'h': (False, True, False)}
        }

        typespec = 'dict[str, dict[str, tuple[bool]]]'
        assert typechecks.conforms_to_typerule(data, typespec)

    def test_conforms_dict_empty(self):
        assert typechecks.conforms_to_typerule(dict(), 'dict[str, str]')

    def test_conforms_list_dict_tuple_dict_float(self):
        data = [
            {'a': ({'b': 5.67, 'c': 7.401}, {'g': 1.1, 'h': 2.5}),
             'b': ({'e': 5.67, 'd': 7.4, 'b': 1.2},)},
            {'t': ({'o': 5.67, 'q': 7.401}, {'a': 1.1, 'l': 2.5}),
             'h': ({'d': 5.67, 'w': 7.4, 'p': 1.2},),
             'u': ({'e': 90.9, 'k': 45.90},)}
        ]

        typespec = 'list[dict[str, tuple[dict[str, float]]]]'
        assert typechecks.conforms_to_typerule(data, typespec)

    def test_conforms_list_empty(self):
        assert typechecks.conforms_to_typerule(list(), 'list[str]')

    def test_conforms_invalid_typespec(self):
        data = {
            'a': {'b': (False, True), 'c': (False, True, True)},
            'b': {'g': (False, True, True), 'h': (False, True, False)}
        }

        typespec = 'dict[str, dict[str, tuple[bool]'

        assert typechecks.conforms_to_typerule(data, typespec) is False

    # def test_conforms_tuple_int_or_tuple_float(self):
    #     data1 = (100, 50, 60)
    #     data2 = (100.5, 50.7, 60.123)

    #     typespec = 'tuple[int]|tuple[float]'

    #     assert database.conforms_to_typerule(data1, typespec)
    #     assert database.conforms_to_typerule(data2, typespec)

    # def test_conforms_tuple_int_or_list_str(self):
    #     data1 = (100, 50, 60)
    #     data2 = ['a', 'b', 'cthe']

    #     typespec = 'tuple[int]|list[str]'

    #     assert database.conforms_to_typerule(data1, typespec)
    #     assert database.conforms_to_typerule(data2, typespec)

    # def test_conforms_tuple_list_bool_or_list_str(self):
    #     data1 = ([True, False, True], [True, False, True], [True, False])
    #     data2 = ['a', 'b', 'cthe']

    #     typespec = 'tuple[list[bool]]|list[str]'

    #     assert database.conforms_to_typerule(data1, typespec)
    #     assert database.conforms_to_typerule(data2, typespec)

    # def test_conforms_str_or_list_str(self):
    #     data1 = "goodnight"
    #     data2 = ['a', 'b', 'cthe']

    #     typespec = 'list[str]|str'

    #     assert database.conforms_to_typerule(data1, typespec)
    #     assert database.conforms_to_typerule(data2, typespec)

    # def test_conforms_str_or_int_or_list_str(self):
    #     data1 = "goodnight"
    #     data2 = ['a', 'b', 'cthe']
    #     data3 = 60

    #     typespec = 'list[str]|str|int'

    #     # assert database.conforms_to_typerule(data1, typespec)
    #     # assert database.conforms_to_typerule(data2, typespec)
    #     assert database.conforms_to_typerule(data3, typespec)
