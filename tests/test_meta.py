import pytest
from filebase import meta
from progutils import typechecks


class TestMetaTemplateClass:

    @pytest.fixture
    def fix_meta_instance(self):
        return meta.MetaTemplate(template_keys=('key1', 'key2', 'key3'))

    def test_instantiation(self):
        try:
            meta.MetaTemplate(template_keys=('key1', 'key2', 'key3'))
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


class TestMetaClass:

    @pytest.fixture
    def dbmeta(self):
        return meta.Meta(template=meta.MetaDatabaseTemplate())

    def test_create_new(self):
        try:
            meta.Meta(template=meta.MetaDatabaseTemplate())
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_raise_on_wrong_template_type(self):
        with pytest.raises(TypeError):
            meta.Meta(template=50)

    def test_has_template_property(self, dbmeta):
        try:
            dbmeta.template
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_template_property_correct_type(self, dbmeta):
        assert isinstance(dbmeta.template, meta.MetaTemplate)

    def test_has_metadata_attribute(self, dbmeta):
        try:
            dbmeta._metadata
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_metadata_attribute_match_template_key(self, dbmeta):
        instance_keys = list(dbmeta._metadata.keys())
        template_keys = meta.MetaDatabaseTemplate.template_keys.keys()
        template_keys = list(template_keys)

        assert instance_keys == template_keys

    def test_metadata_attribute_match_template_defaults(self, dbmeta):
        instance_values = list(dbmeta._metadata.values())
        template_values = meta.MetaDatabaseTemplate.template_keys.values()
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
        dbmeta = meta.Meta(template=meta.MetaDatabaseTemplate(),
                           metadata={'name': 'new_database',
                                     'tables': ('tab1', 'tab2')})

        assert dbmeta._metadata['name'] == 'new_database'
        assert dbmeta._metadata['tables'] == ('tab1', 'tab2')
        assert dbmeta._metadata['total_table_records'] == 0
