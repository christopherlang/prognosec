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


class TestApplyTyperuleFunction:

    @pytest.fixture
    def apply_typerule(self):
        return typechecks.apply_typerule

    def test_module_has_function(self):
        assert hasattr(typechecks, 'apply_typerule')

    def test_singleton_input_int(self, apply_typerule):
        assert apply_typerule('50', 'int') == 50

    def test_singleton_input_float(self, apply_typerule):
        assert apply_typerule('50.7', 'float') == 50.7

    def test_singleton_input_date(self, apply_typerule):
        expected = datetime.date(2019, 1, 1)
        assert apply_typerule('2019-01-01', 'date') == expected

    def test_singleton_input_datetime(self, apply_typerule):
        expected = datetime.datetime(2019, 1, 1, 10, 20, 30)
        parsed_version = apply_typerule('2019-01-01T10:20:30',
                                        'datetime')
        assert parsed_version == expected

    def test_singleton_input_bool(self, apply_typerule):
        assert apply_typerule(True, 'bool') is True
        assert apply_typerule('true', 'bool') is True

    def test_tuple_singleton_input_str(self, apply_typerule):
        assert apply_typerule((1, 2), 'tuple[str]') == ('1', '2')

    def test_tuple_set_int(self, apply_typerule):
        dataseries = [set([1, 2]), set([3, 4]), set([5])]
        expected = (['1', '2'], ['3', '4'], ['5'])
        typespec = 'tuple[list[str]]'
        assert apply_typerule(dataseries, typespec) == expected

    def test_tuple_int(self, apply_typerule):
        parsed = apply_typerule(('10', '20', '30'), 'tuple[int]')
        expected = (10, 20, 30)
        assert parsed == expected

    def test_tuple_mixed_int(self, apply_typerule):
        parsed = apply_typerule(('10', '20', 30), 'tuple[str]')
        expected = ('10', '20', '30')
        assert parsed == expected

    def test_dict_str_int(self, apply_typerule):
        parsed = apply_typerule({'a': 50, 'b': '100'}, 'dict[str, int]')
        expected = {'a': 50, 'b': 100}
        assert parsed == expected

    def test_dict_str_tuple_int_or_float(self, apply_typerule):
        data = {
            'a': (50, 100),
            'b': (50, 100.65),
        }
        parsed = apply_typerule(data, 'dict[str, tuple[float]]')
        expected = {
            'a': (50.0, 100.0),
            'b': (50.0, 100.65),
        }

        assert parsed == expected

    def test_dict_str_tuple_int(self, apply_typerule):
        dataseries = {'a': tuple(['60']), 'b': tuple(['100'])}
        expected = {'a': tuple([60]), 'b': tuple([100])}
        typespec = 'dict[str, tuple[int]]'
        assert apply_typerule(dataseries, typespec) == expected

    def test_tuple_list_dict_str_int(self, apply_typerule):
        dataseries = [[{'a': '4', 'b': '6'}, {'c': '10'}], [{'z': '102'}]]
        expected = ([{'a': 4, 'b': 6}, {'c': 10}], [{'z': 102}])
        typespec = 'tuple[list[dict[str, int]]]'
        assert apply_typerule(dataseries, typespec) == expected

    def test_tuple_list_dict_str_set_int(self, apply_typerule):
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
        assert apply_typerule(dataseries, typespec) == expected


class TestConformsToTypespecFunction:

    def test_conforms_int(self):
        assert typechecks.conforms_typerule(50, 'int')

    def test_conforms_date(self):
        assert typechecks.conforms_typerule(datetime.date(2019, 1, 1), 'date')

    def test_conforms_datetime(self):
        dt = datetime.datetime(2019, 1, 1, 9, 10, 10)
        assert typechecks.conforms_typerule(dt, 'datetime')

    def test_conforms_datetime_utc(self):
        dt = datetime.datetime(2019, 1, 1, 9, 10, 10)
        assert typechecks.conforms_typerule(dt, 'datetime')

    def test_conforms_datetime_obj(self):
        the_datetime = datetime.datetime.utcnow().replace(microsecond=0)

        assert typechecks.conforms_typerule(the_datetime, 'datetime')

    def test_conforms_date_obj(self):
        the_date = datetime.datetime.utcnow().date()

        assert typechecks.conforms_typerule(the_date, 'date')

    def test_conforms_int_or_str(self):
        assert typechecks.conforms_typerule(50, 'int|str')
        assert typechecks.conforms_typerule('50', 'int|str')

    def test_conforms_int_or_str_float_input(self):
        assert typechecks.conforms_typerule(56.0, 'int|str') is False

    def test_conforms_list_int_or_str_singular_input(self):
        assert typechecks.conforms_typerule([50, 100, 500], 'list[int|str]')
        assert typechecks.conforms_typerule(['50', '100'], 'list[int|str]')

    def test_conforms_list_int_or_str_mixed_input(self):
        assert typechecks.conforms_typerule([50, '100', 500],
                                            'list[int|str]')

    def test_conforms_list_tuple_int_or_float(self):
        data = [
            (100, 200, 300),
            (24.3, 78.8, 400.10),
            (24.3, 100, 10)
        ]
        assert typechecks.conforms_typerule(data, 'list[tuple[int|float]]')

    def test_conforms_list_int(self):
        assert typechecks.conforms_typerule([50, 100], 'list[int]')

    def test_conforms_list_dict_int(self):
        data = [
            {'a': 500, 'b': 1090},
            {'c': 1000, 'd': 453, 'e': 9870}
        ]
        assert typechecks.conforms_typerule(data, 'list[dict[str, int]]')

    def test_conforms_tuple_list_str(self):
        data = (
            ['hey', 'this', 'is', 'test'],
            ['this', 'is', 'another'],
            ['singular']
        )
        assert typechecks.conforms_typerule(data, 'tuple[list[str]]')

    def test_conforms_tuple_empty(self):
        assert typechecks.conforms_typerule(tuple(), 'tuple[str]')

    def test_conforms_dict_tuple_bool(self):
        data = {
            'hey': (True, False, True),
            'there': (False, False, True, True, False)
        }
        assert typechecks.conforms_typerule(data, 'dict[str, tuple[bool]]')

    def test_conforms_dict_dict_float(self):
        data = {
            'a': {'b': 5.6, 'c': 7.8},
            'b': {'g': 5.6, 'h': 7.8}
        }

        typespec = 'dict[str, dict[str, float]]'
        assert typechecks.conforms_typerule(data, typespec)

    def test_conforms_dict_dict_tuple_bool(self):
        data = {
            'a': {'b': (False, True), 'c': (False, True, True)},
            'b': {'g': (False, True, True), 'h': (False, True, False)}
        }

        typespec = 'dict[str, dict[str, tuple[bool]]]'
        assert typechecks.conforms_typerule(data, typespec)

    def test_conforms_dict_empty(self):
        assert typechecks.conforms_typerule(dict(), 'dict[str, str]')

    def test_conforms_list_dict_tuple_dict_float(self):
        data = [
            {'a': ({'b': 5.67, 'c': 7.401}, {'g': 1.1, 'h': 2.5}),
             'b': ({'e': 5.67, 'd': 7.4, 'b': 1.2},)},
            {'t': ({'o': 5.67, 'q': 7.401}, {'a': 1.1, 'l': 2.5}),
             'h': ({'d': 5.67, 'w': 7.4, 'p': 1.2},),
             'u': ({'e': 90.9, 'k': 45.90},)}
        ]

        typespec = 'list[dict[str, tuple[dict[str, float]]]]'
        assert typechecks.conforms_typerule(data, typespec)

    def test_conforms_list_empty(self):
        assert typechecks.conforms_typerule(list(), 'list[str]')

    def test_conforms_invalid_typerule_missing_bracket(self):
        data = {
            'a': {'b': (False, True), 'c': (False, True, True)},
            'b': {'g': (False, True, True), 'h': (False, True, False)}
        }

        typespec = 'dict[str, dict[str, tuple[bool]'

        with pytest.raises(ValueError):
            typechecks.conforms_typerule(data, typespec)

    def test_invalid_typerule_keyword(self):
        with pytest.raises(ValueError):
            typechecks.conforms_typerule('hey', 'nope')

    def test_conforms_tuple_int_or_tuple_float(self):
        data1 = (100, 50, 60)
        data2 = (100.5, 50.7, 60.123)

        typerule = 'tuple[int]|tuple[float]'

        assert typechecks.conforms_typerule(data1, typerule)
        assert typechecks.conforms_typerule(data2, typerule)

    def test_conforms_tuple_int_or_list_str(self):
        data1 = (100, 50, 60)
        data2 = ['a', 'b', 'cthe']

        typespec = 'tuple[int]|list[str]'

        assert typechecks.conforms_typerule(data1, typespec)
        assert typechecks.conforms_typerule(data2, typespec)

    def test_conforms_tuple_list_bool_or_list_str(self):
        data1 = ([True, False, True], [True, False, True], [True, False])
        data2 = ['a', 'b', 'cthe']

        typerule = 'tuple[list[bool]]|list[str]'

        assert typechecks.conforms_typerule(data1, typerule)
        assert typechecks.conforms_typerule(data2, typerule)

    def test_conforms_str_or_list_str(self):
        data1 = "goodnight"
        data2 = ['a', 'b', 'cthe']

        typerule = 'list[str]|str'

        assert typechecks.conforms_typerule(data1, typerule)
        assert typechecks.conforms_typerule(data2, typerule)

    def test_conforms_str_or_int_or_list_str(self):
        data1 = "goodnight"
        data2 = ['a', 'b', 'cthe']
        data3 = 60

        typerule = 'list[str]|str|int'

        assert typechecks.conforms_typerule(data1, typerule)
        assert typechecks.conforms_typerule(data2, typerule)
        assert typechecks.conforms_typerule(data3, typerule)

    def test_singleton_int(self):
        assert typechecks.conforms_typerule(10, 'int') is True

    def test_singleton_str(self):
        assert typechecks.conforms_typerule('10', 'str') is True

    def test_singleton_bool(self):
        assert typechecks.conforms_typerule(True, 'bool') is True

    def test_singleton_float(self):
        assert typechecks.conforms_typerule(1.26, 'float') is True

    def test_singleton_int_or_float(self):
        assert typechecks.conforms_typerule(1.26, 'int|float') is True
        assert typechecks.conforms_typerule(5, 'int|float') is True

    def test_singleton_bool_or_str(self):
        assert typechecks.conforms_typerule(True, 'bool|str') is True
        assert typechecks.conforms_typerule("hey there", "bool|str") is True

    def test_singleton_primitive(self):
        assert typechecks.conforms_typerule(True, 'primitive') is True
        assert typechecks.conforms_typerule("hey there", "primitive") is True
        assert typechecks.conforms_typerule(5.46, "primitive") is True
        assert typechecks.conforms_typerule(10, "primitive") is True

    def test_singleton_date_obj(self):
        dt = datetime.date(2019, 1, 1)
        assert typechecks.conforms_typerule(dt, 'date') is True

    def test_singleton_datetime_obj(self):
        dt = datetime.datetime(2019, 1, 1, 10, 10, 10)
        assert typechecks.conforms_typerule(dt, 'datetime') is True

    def test_singleton_time_obj(self):
        dt = datetime.time(10, 10, 10)
        assert typechecks.conforms_typerule(dt, 'time') is True

    def test_sequence_list_int(self):
        assert typechecks.conforms_typerule([1, 2, 6], 'list[int]') is True

    def test_sequence_list_tuple_int(self):
        dat = [(1, 2, 3), (4, 5, 6)]
        assert typechecks.conforms_typerule(dat, 'list[tuple[int]]') is True

    def test_sequence_list_tuple_int_wrong_type(self):
        dat = [(1, '2'), (4, 5, 6)]
        assert typechecks.conforms_typerule(dat, 'list[tuple[int]]') is False

    def test_sequence_list_tuple_int_or_str(self):
        dat = [(1, '2'), (4, 5, 6)]
        typerule = 'list[tuple[int|str]]'
        assert typechecks.conforms_typerule(dat, typerule) is True

    def test_sequence_set_float(self):
        assert typechecks.conforms_typerule({9.8, 5.67}, 'set[float]') is True

    def test_or_sequence_list_int_or_set_str(self):
        dat1 = [1, 2, 3, 4]
        dat2 = {'hello', 'world', 'there'}

        assert typechecks.conforms_typerule(dat1, 'list[int]|set[str]') is True
        assert typechecks.conforms_typerule(dat2, 'list[int]|set[str]') is True

    def test_sequence_list_int_or_singleton_str(self):
        dat1 = [1, 2, 3, 4]
        dat2 = "hello world"

        assert typechecks.conforms_typerule(dat1, 'list[int]|str') is True
        assert typechecks.conforms_typerule(dat2, 'list[int]|str') is True

    def test_sequence_list_set_str_or_float(self):
        dat1 = [{'hello', 'world'}]
        dat2 = [5.4]
        typerule = 'list[set[str]]|list[float]'
        assert typechecks.conforms_typerule(dat1, typerule) is True
        assert typechecks.conforms_typerule(dat2, typerule) is True

    def test_sequence_tuple_float_or_int(self):
        typerule = 'tuple[int|float]'
        assert typechecks.conforms_typerule((1, 4, 4.312), typerule) is True

    def test_sequence_dict_str_float(self):
        typerule = 'dict[str, float]'
        assert typechecks.conforms_typerule({'hello': 5.43}, typerule) is True

    def test_sequence_dict_str_float_wrong_keytype(self):
        value = {'hello': 5.43, 566: 'world'}

        assert typechecks.conforms_typerule(value, 'dict[str, float]') is False

    def test_sequence_dict_str_or_int_float(self):
        value = {'hello': 5.43, 566: 'world'}
        typerule = 'dict[str|int, float|str]'
        assert typechecks.conforms_typerule(value, typerule) is True

    def test_sequence_list_tuple_int_or_list_float(self):
        value1 = [(1, 2), (3, 4)]
        value2 = [[5.45, 1.43, 5.69], [5.9, 17.8, 1.2]]
        typerule = 'list[tuple[int]|list[float]]'
        assert typechecks.conforms_typerule(value1, typerule) is True
        assert typechecks.conforms_typerule(value2, typerule) is True

    def test_sequence_list_tuple_set_list_int_float_int(self):
        value1 = [(1, 2), (3, 4)]
        value2 = [{5.45, 1.43, 5.69}, {5.9, 17.8, 1.2}]
        value3 = [[1, 2, 3], [6, 5, 7]]

        typerule = 'list[tuple[int]|set[float]|list[int]]'

        assert typechecks.conforms_typerule(value1, typerule) is True
        assert typechecks.conforms_typerule(value2, typerule) is True
        assert typechecks.conforms_typerule(value3, typerule) is True

    def test_sequence_dict_str_or_int_float_wrong_valuetype(self):
        value = {'hello': 5.43, 566: 'world'}
        typerule = 'dict[str|int, bool|str]'
        assert typechecks.conforms_typerule(value, typerule) is False

    def test_sequence_none(self):
        value = None

        assert typechecks.conforms_typerule(value, 'None|null') is True
