import pytest
import json
import database
import os
import datetime
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
            fix_meta_instance.add_typespec(key='test', typerule='int',
                                           required=False, default=None)

    def test_adding_key_must_string(self, fix_meta_instance):
        with pytest.raises(TypeError):
            fix_meta_instance.add_typespec(key=100, typerule='int',
                                           required=False, default=None)

    def test_adding_typespec(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default=None)
        fix_meta_instance.add_typespec(key='key2', typerule='int',
                                       required=False, default=None)
        fix_meta_instance.add_typespec(key='key3', typerule='int',
                                       required=False, default=None)

        original_typespec = ('int', False, None)

        assert original_typespec == fix_meta_instance.metadata['key2']

    def test_has_delete_method(self, fix_meta_instance):
        assert hasattr(fix_meta_instance, 'delete_typespec')

    def test_delete_typespec_wrong_key(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default=None)

        with pytest.raises(KeyError):
            fix_meta_instance.delete_typespec(key='hey')

    def test_extract_meta_incomplete(self, fix_meta_instance):
        # This should fail because we did not fill in all of the keys
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default=None)

        with pytest.raises(KeyError):
            fix_meta_instance.metadata

    def test_extract_meta_correct(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default=None)
        fix_meta_instance.add_typespec(key='key2', typerule='int',
                                       required=True, default=None)
        fix_meta_instance.add_typespec(key='key3', typerule='int',
                                       required=False, default="hello")

        originals = {
            'key1': ('int', False, None),
            'key2': ('int', True, None),
            'key3': ('int', False, 'hello')
        }

        metadata_output = fix_meta_instance.metadata

        for key in fix_meta_instance.template_keys:
            assert metadata_output[key] == originals[key]

    def test_get_typerule(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default=None)
        fix_meta_instance.add_typespec(key='key2', typerule='tuple[int]',
                                       required=False, default=None)

        assert fix_meta_instance.get_typerule('key2') == 'tuple[int]'

    def test_get_default_none(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default=None)

        assert fix_meta_instance.get_default('key1') is None

    def test_get_default_str(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default='hello world')

        assert fix_meta_instance.get_default('key1') == 'hello world'

    def test_get_required(self, fix_meta_instance):
        fix_meta_instance.add_typespec(key='key1', typerule='int',
                                       required=False, default='hello world')

        assert fix_meta_instance.get_required('key1') is False


class TestParseTyperuleFunction:

    @pytest.fixture
    def parse_typerule(self):
        return database.parse_typerule

    def test_module_has_function(self):
        assert hasattr(database, 'parse_typerule')

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
        assert database.conforms_to_typerule(50, 'int')

    def test_conforms_date(self):
        assert database.conforms_to_typerule('2019-01-01', 'date')

    def test_conforms_datetime(self):
        assert database.conforms_to_typerule('2019-01-01T09:10:10',
                                             'datetime_second')

    def test_conforms_datetime_utc(self):
        assert database.conforms_to_typerule('2019-01-01T09:10:10Z',
                                             'datetime_second')

    def test_conforms_datetime_obj(self):
        the_datetime = datetime.datetime.utcnow().replace(microsecond=0)

        assert database.conforms_to_typerule(the_datetime, 'datetime_second')

    def test_conforms_date_obj(self):
        the_date = datetime.datetime.utcnow().date()

        assert database.conforms_to_typerule(the_date, 'datetime_second')

    def test_conforms_int_or_str(self):
        assert database.conforms_to_typerule(50, 'int|str')
        assert database.conforms_to_typerule('50', 'int|str')

    def test_conforms_int_or_str_float_input(self):
        assert database.conforms_to_typerule(56.0, 'int|str') is False

    def test_conforms_list_int_or_str_singular_input(self):
        assert database.conforms_to_typerule([50, 100, 500], 'list[int|str]')
        assert database.conforms_to_typerule(['50', '100'], 'list[int|str]')

    def test_conforms_list_int_or_str_mixed_input(self):
        assert database.conforms_to_typerule([50, '100', 500], 'list[int|str]')

    def test_conforms_list_tuple_int_or_float(self):
        data = [
            (100, 200, 300),
            (24.3, 78.8, 400.10),
            (24.3, 100, 10)
        ]
        assert database.conforms_to_typerule(data, 'list[tuple[int|float]]')

    def test_conforms_list_int(self):
        assert database.conforms_to_typerule([50, 100], 'list[int]')

    def test_conforms_list_dict_int(self):
        data = [
            {'a': 500, 'b': 1090},
            {'c': 1000, 'd': 453, 'e': 9870}
        ]
        assert database.conforms_to_typerule(data, 'list[dict[str, int]]')

    def test_conforms_tuple_list_str(self):
        data = (
            ['hey', 'this', 'is', 'test'],
            ['this', 'is', 'another'],
            ['singular']
        )
        assert database.conforms_to_typerule(data, 'tuple[list[str]]')

    def test_conforms_tuple_empty(self):
        assert database.conforms_to_typerule(tuple(), 'tuple[str]')

    def test_conforms_dict_tuple_bool(self):
        data = {
            'hey': (True, False, True),
            'there': (False, False, True, True, False)
        }
        assert database.conforms_to_typerule(data, 'dict[str, tuple[bool]]')

    def test_conforms_dict_dict_float(self):
        data = {
            'a': {'b': 5.6, 'c': 7.8},
            'b': {'g': 5.6, 'h': 7.8}
        }

        typespec = 'dict[str, dict[str, float]]'
        assert database.conforms_to_typerule(data, typespec)

    def test_conforms_dict_dict_tuple_bool(self):
        data = {
            'a': {'b': (False, True), 'c': (False, True, True)},
            'b': {'g': (False, True, True), 'h': (False, True, False)}
        }

        typespec = 'dict[str, dict[str, tuple[bool]]]'
        assert database.conforms_to_typerule(data, typespec)

    def test_conforms_dict_empty(self):
        assert database.conforms_to_typerule(dict(), 'dict[str, str]')

    def test_conforms_list_dict_tuple_dict_float(self):
        data = [
            {'a': ({'b': 5.67, 'c': 7.401}, {'g': 1.1, 'h': 2.5}),
             'b': ({'e': 5.67, 'd': 7.4, 'b': 1.2},)},
            {'t': ({'o': 5.67, 'q': 7.401}, {'a': 1.1, 'l': 2.5}),
             'h': ({'d': 5.67, 'w': 7.4, 'p': 1.2},),
             'u': ({'e': 90.9, 'k': 45.90},)}
        ]

        typespec = 'list[dict[str, tuple[dict[str, float]]]]'
        assert database.conforms_to_typerule(data, typespec)

    def test_conforms_list_empty(self):
        assert database.conforms_to_typerule(list(), 'list[str]')

    def test_conforms_invalid_typespec(self):
        data = {
            'a': {'b': (False, True), 'c': (False, True, True)},
            'b': {'g': (False, True, True), 'h': (False, True, False)}
        }

        typespec = 'dict[str, dict[str, tuple[bool]'

        assert database.conforms_to_typerule(data, typespec) is False

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


class TestMetaClass:

    @pytest.fixture
    def dbmeta(self):
        return database.Meta(template=database.MetaDatabaseTemplate())

    def test_create_new(self):
        try:
            database.Meta(template=database.MetaDatabaseTemplate())
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_raise_on_wrong_template_type(self):
        with pytest.raises(TypeError):
            database.Meta(template=50)

    def test_has_template_property(self, dbmeta):
        try:
            dbmeta.template
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_template_property_correct_type(self, dbmeta):
        assert isinstance(dbmeta.template, database.MetaTemplate)

    def test_has_metadata_attribute(self, dbmeta):
        try:
            dbmeta._metadata
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_metadata_attribute_match_template_key(self, dbmeta):
        instance_keys = list(dbmeta._metadata.keys())
        template_keys = database.MetaDatabaseTemplate.template_keys.keys()
        template_keys = list(template_keys)

        assert instance_keys == template_keys

    def test_metadata_attribute_match_template_defaults(self, dbmeta):
        instance_values = list(dbmeta._metadata.values())
        template_values = database.MetaDatabaseTemplate.template_keys.values()
        template_values = [i[2] for i in template_values]

        assert instance_values == template_values

    def test_replace_key_value(self, dbmeta):
        dbmeta.replace(key='name', value='database')

    def test_replace_wrong_key(self, dbmeta):
        with pytest.raises(KeyError):
            dbmeta.replace(key='BOOGABOOGA', value='database')

    def test_replace_wrong_value_type(self, dbmeta):
        with pytest.raises(TypeError):
            dbmeta.replace(key='name', value=500)

    def test_extract_metadata_invalid(self, dbmeta):
        # Cannot get metadata since it is not filled defined by template
        with pytest.raises(KeyError):
            dbmeta.metadata

    def test_has_edit_method(self, dbmeta):
        assert hasattr(dbmeta, 'edit')

    def test_edit_wrong_key(self, dbmeta):
        with pytest.raises(KeyError):
            dbmeta.edit('followme', 700)

    def test_edit_wrong_value(self, dbmeta):
        with pytest.raises(TypeError):
            dbmeta.edit('name', 800)

    def test_edit_tuple_new(self, dbmeta):
        dbmeta.edit('tables', ('table1',))

        assert dbmeta._metadata['tables'] == ('table1',)

    def test_edit_tuple_existing(self, dbmeta):
        dbmeta.edit('tables', ('table1',))
        dbmeta.edit('tables', ('table12',))

        assert dbmeta._metadata['tables'] == ('table1', 'table12')

    def test_edit_dict_existing(self, dbmeta):
        dbmeta.edit('table_meta_locations', {'tab1': '500'})
        dbmeta.edit('table_meta_locations', {'tab2': 'free', 'tab3': 'ee'})

        expected = {'tab1': '500', 'tab2': 'free', 'tab3': 'ee'}
        assert dbmeta._metadata['table_meta_locations'] == expected

    def test_edit_dict_key_overwrite(self, dbmeta):
        dbmeta.edit('table_meta_locations', {'tab1': '500', 'tab2': 'booga'})
        dbmeta.edit('table_meta_locations', {'tab2': 'free', 'tab3': 'ee'})

        expected = {'tab1': '500', 'tab2': 'free', 'tab3': 'ee'}
        assert dbmeta._metadata['table_meta_locations'] == expected

    def test_has_reset_method(self, dbmeta):
        dbmeta.edit('name', 'placeholder')
        assert dbmeta._metadata['name'] == 'placeholder'

        dbmeta.reset('name')
        assert dbmeta._metadata['name'] is dbmeta.template.get_default('name')

    def test_reset_wrong_key(self, dbmeta):
        with pytest.raises(KeyError):
            dbmeta.reset('name2222')

    def test_create_meta_existing_metadata(self):
        meta = database.Meta(template=database.MetaDatabaseTemplate(),
                             metadata={'name': 'new_database',
                                       'tables': ('tab1', 'tab2')})

        assert meta._metadata['name'] == 'new_database'
        assert meta._metadata['tables'] == ('tab1', 'tab2')
        assert meta._metadata['total_table_records'] == 0


class TestInitDatabaseFunction:

    @pytest.fixture
    def rootdir(self, tmp_path):
        # Directory to save the database directory
        return tmp_path

    def test_run_init(self, rootdir):
        try:
            database.init_database(rootdir=rootdir, dbdir_name='database')
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_creates_database_directory(self, rootdir):
        assert os.path.exists(os.path.join(rootdir, 'database')) is False

        database.init_database(rootdir=rootdir, dbdir_name='database')

        assert os.path.exists(os.path.join(rootdir, 'database'))

    def test_database_directory_already_exists(self, rootdir):
        os.mkdir(os.path.join(rootdir, 'database'))

        with pytest.raises(FileExistsError):
            database.init_database(rootdir=rootdir, dbdir_name='database')

    def test_created_database_meta(self, rootdir):
        database.init_database(rootdir=rootdir, dbdir_name='database')

        metafile_loc = os.path.join(rootdir, 'database', 'database_meta.json')

        assert os.path.exists(metafile_loc)

    def test_created_stores_directory(self, rootdir):
        database.init_database(rootdir=rootdir, dbdir_name='database')

        storesdir = os.path.join(rootdir, 'database', 'stores')

        assert os.path.exists(storesdir)

    def test_raises_on_missing_rootdir(self, rootdir):
        fakepath = os.path.join(rootdir, 'continue')
        with pytest.raises(FileNotFoundError):
            database.init_database(rootdir=fakepath, dbdir_name='database')

    def test_raise_on_existing_dbdir(self, rootdir):
        database.init_database(rootdir=rootdir, dbdir_name='database')

        with pytest.raises(FileExistsError):
            database.init_database(rootdir=rootdir, dbdir_name='database')

    def test_meta_written(self, rootdir):
        tmp = database.init_database(rootdir=rootdir, dbdir_name='database')
        metaloc = tmp.metadata['database_meta_location']
        with open(metaloc, 'r', encoding='utf-8') as f:
            loaded = json.load(f)

            dbtemp = database.MetaDatabaseTemplate()
            for key in loaded:
                typerule = dbtemp.get_typerule(key)
                loaded[key] = database.parse_typerule(loaded[key], typerule)

        assert tmp.metadata == loaded


class TestInitTableFunction:

    @pytest.fixture
    def dbpath(self, tmp_path):
        database.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbmeta(self, dbpath):
        tmp = database.DatabaseManager(dbpath=dbpath)

        return tmp.meta

    def test_typerule_checking(self, dbmeta):
        with pytest.raises(TypeError):
            database.init_table(
                name=600, columns=('x1', 'x2', 'x3'),
                datatypes=('int', 'str', 'int'), keys=('x1',),
                dbmeta=dbmeta)

    def test_raise_on_existing_table(self, dbmeta):
        dbmeta.edit('tables', ('new_table',))

        with pytest.raises(ValueError):
            database.init_table(
                name='new_table', columns=('x1', 'x2', 'x3'),
                datatypes=('int', 'str', 'int'), keys=('x1',),
                dbmeta=dbmeta)

    def test_table_directory_created(self, dbmeta):
        tblmeta = database.init_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            dbmeta=dbmeta)

        assert os.path.exists(tblmeta.metadata['table_directory'])

    def test_meta_file_created(self, dbmeta):
        tblmeta = database.init_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            dbmeta=dbmeta)

        metaloc = tblmeta.metadata['index_file_location']
        with open(metaloc, 'r', encoding='utf-8') as f:
            loaded = json.load(f)

            dbtemp = database.MetaTableTemplate()
            for key in loaded:
                # breakpoint()
                typerule = dbtemp.get_typerule(key)

                # TODO
                # This is a bypass for foreign key, which can hold None
                # Check init_table function for the associated TODO there
                if key == 'foreign':
                    pass
                else:
                    loaded[key] = database.parse_typerule(loaded[key],
                                                          typerule)

        assert tblmeta.metadata == loaded



class TestDatabaseManagerClass:

    @pytest.fixture
    def dbpath(self, tmp_path):
        database.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbman_instance(self, dbpath):
        return database.DatabaseManager(dbpath=dbpath)

    def test_create_instance(self, dbpath):
        database.DatabaseManager(dbpath=dbpath)

    def test_has_metadata_property(self, dbman_instance):
        assert hasattr(dbman_instance, 'meta')

    def test_all_metadata_conforms(self, dbman_instance):
        template = database.MetaDatabaseTemplate()
        for key in dbman_instance.meta.metadata:
            typerule = template.get_typerule(key)
            value = dbman_instance.meta.metadata[key]
            assert database.conforms_to_typerule(value, typerule)

    def test_create_new_table(self, dbman_instance):
        dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'), keys=('x1',))

    # def test_create_new_table_typerule_checks(self, dbman_instance):
    #     with pytest.raises(ValueError):
    #         dbman_instance.create_table(
    #             name=600, columns=('x1', 'x2', 'x3'), keys=('x1',))
