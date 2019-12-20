import pytest
import database
# from typing import Set, Dict, List, Sequence, Tuple


class TestMetaTemplateClass:

    @pytest.fixture
    def fix_meta_instance(self):
        return database.MetaTemplate(template_keys=('key1', 'key2', 'key3'))

    def test_instantiation(self):
        try:
            database.MetaTemplate(template_keys=('key1', 'key2', 'key3'))
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_has_add_method(self, fix_meta_instance):
        assert hasattr(fix_meta_instance, 'add_typespec')

    def test_adding_not_template_key(self, fix_meta_instance):
        with pytest.raises(ValueError):
            # Raises because 'hey' is not a template key
            fix_meta_instance.add_typespec(key='test', typerule=int,
                                           required=False, default=None)

    def test_adding_key_must_string(self, fix_meta_instance):
        with pytest.raises(TypeError):
            fix_meta_instance.add_typespec(key=100, typerule=int,
                                           required=False, default=None)

    def test_adding_typespec(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule=int,
                                       required=False, default=None)
        fix_meta_instance.add_typespec(key='key2', typerule=int,
                                       required=False, default=None)
        fix_meta_instance.add_typespec(key='key3', typerule=int,
                                       required=False, default=None)

        original_typespec = (int, False, None)

        assert original_typespec == fix_meta_instance.metadata['key2']

    def test_has_delete_method(self, fix_meta_instance):
        assert hasattr(fix_meta_instance, 'delete_typespec')

    def test_delete_typespec_wrong_key(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule=int,
                                       required=False, default=None)

        with pytest.raises(KeyError):
            fix_meta_instance.delete_typespec(key='hey')

    def test_extract_meta_incomplete(self, fix_meta_instance):
        # This should fail because we did not fill in all of the keys
        fix_meta_instance.add_typespec(key='key1', typerule=int,
                                       required=False, default=None)

        with pytest.raises(KeyError):
            fix_meta_instance.metadata

    def test_extract_meta_correct(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule=int,
                                       required=False, default=None)
        fix_meta_instance.add_typespec(key='key2', typerule=int,
                                       required=True, default=None)
        fix_meta_instance.add_typespec(key='key3', typerule=int,
                                       required=False, default="hello")

        originals = {
            'key1': (int, False, None),
            'key2': (int, True, None),
            'key3': (int, False, 'hello')
        }

        metadata_output = fix_meta_instance.metadata

        for key in fix_meta_instance.template_keys:
            assert metadata_output[key] == originals[key]


class TestParseTypespec:

    @pytest.fixture
    def parse_typesec(self):
        return database.parse_typespec

    def test_module_has_function(self):
        assert hasattr(database, 'parse_typespec')

    def test_singleton_input_int(self, parse_typesec):
        assert parse_typesec('50', 'int') == 50

    def test_singleton_input_float(self, parse_typesec):
        assert parse_typesec('50.7', 'float') == 50.7

    def test_singleton_input_bool(self, parse_typesec):
        assert parse_typesec(True, 'bool') is True
        assert parse_typesec('true', 'bool') is True

    def test_tuple_singleton_input_str(self, parse_typesec):
        assert parse_typesec((1, 2), 'tuple[str]') == ('1', '2')

    def test_tuple_set_int(self, parse_typesec):
        dataseries = [set([1, 2]), set([3, 4]), set([5])]
        expected = (['1', '2'], ['3', '4'], ['5'])
        typespec = 'tuple[list[str]]'
        assert database.parse_typespec(dataseries, typespec) == expected

    def test_dict_str_int(self, parse_typesec):
        dataseries = {'a': '60', 'b': '100'}
        expected = {'a': 60, 'b': 100}
        typespec = 'dict[str, int]'
        assert database.parse_typespec(dataseries, typespec) == expected

    def test_dict_str_tuple_int(self, parse_typesec):
        dataseries = {'a': tuple(['60']), 'b': tuple(['100'])}
        expected = {'a': tuple([60]), 'b': tuple([100])}
        typespec = 'dict[str, tuple[int]]'
        assert database.parse_typespec(dataseries, typespec) == expected

    def test_tuple_list_dict_str_int(self, parse_typesec):
        dataseries = [[{'a': '4', 'b': '6'}, {'c': '10'}], [{'z': '102'}]]
        expected = ([{'a': 4, 'b': 6}, {'c': 10}], [{'z': 102}])
        typespec = 'tuple[list[dict[str, int]]]'
        assert database.parse_typespec(dataseries, typespec) == expected

    def test_tuple_list_dict_str_set_int(self, parse_typesec):
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
        assert database.parse_typespec(dataseries, typespec) == expected
