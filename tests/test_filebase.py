import pytest
import json
import numpy
import pickle
import itertools
from filebase import filebase
from filebase import meta
from progutils import typechecks
import os
import datetime
# from typing import Set, Dict, List, Sequence, Tuple


class TestInitDatabaseFunction:

    @pytest.fixture
    def rootdir(self, tmp_path):
        # Directory to save the database directory
        return tmp_path

    def test_run_init(self, rootdir):
        try:
            filebase.init_database(rootdir=rootdir, dbdir_name='database')
        except AttributeError as e:
            pytest.fail(e.args[0])

    def test_creates_database_directory(self, rootdir):
        assert os.path.exists(os.path.join(rootdir, 'database')) is False

        filebase.init_database(rootdir=rootdir, dbdir_name='database')

        assert os.path.exists(os.path.join(rootdir, 'database'))

    def test_database_directory_already_exists(self, rootdir):
        os.mkdir(os.path.join(rootdir, 'database'))

        with pytest.raises(FileExistsError):
            filebase.init_database(rootdir=rootdir, dbdir_name='database')

    def test_created_database_meta(self, rootdir):
        filebase.init_database(rootdir=rootdir, dbdir_name='database')

        metafile_loc = os.path.join(rootdir, 'database', 'database_meta.json')

        assert os.path.exists(metafile_loc)

    def test_created_stores_directory(self, rootdir):
        filebase.init_database(rootdir=rootdir, dbdir_name='database')

        storesdir = os.path.join(rootdir, 'database', 'stores')

        assert os.path.exists(storesdir)

    def test_raises_on_missing_rootdir(self, rootdir):
        fakepath = os.path.join(rootdir, 'continue')
        with pytest.raises(FileNotFoundError):
            filebase.init_database(rootdir=fakepath, dbdir_name='database')

    def test_raise_on_existing_dbdir(self, rootdir):
        filebase.init_database(rootdir=rootdir, dbdir_name='database')

        with pytest.raises(FileExistsError):
            filebase.init_database(rootdir=rootdir, dbdir_name='database')

    def test_meta_written(self, rootdir):
        tmp = filebase.init_database(rootdir=rootdir, dbdir_name='database')
        metaloc = tmp.metadata['database_meta_location']
        with open(metaloc, 'r', encoding='utf-8') as f:
            loaded = json.load(f)

            dbtemp = meta.MetaDatabaseTemplate()
            for key in loaded:
                typerule = dbtemp.get_typerule(key)
                loaded[key] = typechecks.apply_typerule(loaded[key], typerule)

        assert tmp.metadata == loaded


class TestDatabaseManagerClass:

    @pytest.fixture
    def dbpath(self, tmp_path):
        filebase.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbman_instance(self, dbpath):
        return filebase.DatabaseManager(dbpath=dbpath)

    def test_create_instance(self, dbpath):
        filebase.DatabaseManager(dbpath=dbpath)

    def test_has_metadata_property(self, dbman_instance):
        assert hasattr(dbman_instance, 'meta')

    def test_all_metadata_conforms(self, dbman_instance):
        template = meta.MetaDatabaseTemplate()
        for key in dbman_instance.meta.metadata:
            typerule = template.get_typerule(key)
            value = dbman_instance.meta.metadata[key]
            assert typechecks.conforms_typerule(value, typerule)

    def test_create_table_monolithic(self, dbman_instance):
        tbl = dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='monolithic')

        assert isinstance(tbl, filebase.TableMonolithic)

    def test_create_table_spliced(self, dbman_instance):
        tbl = dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='spliced', splice_keys=('x1', 'x2'))

        assert isinstance(tbl, filebase.TableSpliced)

    def test_tables_property_same_as_meta(self, dbman_instance):
        dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='spliced', splice_keys=('x1', 'x2'))
        dbman_instance.create_table(
            name='new_table_2', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='spliced', splice_keys=('x1', 'x2'))

        prop_tables = sorted(dbman_instance.tables)
        meta_tables = sorted(dbman_instance.meta['tables'])
        assert prop_tables == meta_tables

    def test_deleting_tables_meta_updated(self, dbman_instance):
        dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='spliced', splice_keys=('x1', 'x2'))
        dbman_instance.create_table(
            name='new_table_2', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='monolithic', splice_keys=('x1', 'x2'))

        assert len(dbman_instance.tables) == 2

        dbman_instance.delete_table(tblname='new_table')

        assert len(dbman_instance.tables) == 1

    def test_deleting_tables_dir_deleted(self, dbman_instance):
        dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='spliced', splice_keys=('x1', 'x2'))
        dbman_instance.create_table(
            name='new_table_2', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='monolithic', splice_keys=('x1', 'x2'))

        tbldir = dbman_instance.get_table('new_table').meta['table_directory']

        assert os.path.exists(tbldir) is True

        dbman_instance.delete_table(tblname='new_table')

        assert os.path.exists(tbldir) is not True

    def test_deleting_tables_nrecords_correct(self, dbman_instance):
        tbl = dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='spliced', splice_keys=('x1', 'x2'))

        dat = [
            (1, '2', 15),
            (1, '15', 20)
        ]

        tbl.add(dat)

        assert dbman_instance.meta['total_table_records'] == len(dat)

        dbman_instance.delete_table('new_table')

        assert dbman_instance.meta['total_table_records'] == 0


class TestTableEditingMonolithic:

    @pytest.fixture
    def dbpath(self, tmp_path):
        filebase.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbman(self, dbpath):
        return filebase.DatabaseManager(dbpath=dbpath)

    @pytest.fixture
    def tbl(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3', 'x4'),
            datatypes=('int', 'str', 'str', 'int'), keys=('x1',),
            storage_type='monolithic')

        return tbl

    @pytest.fixture
    def tbl2(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3', 'x4'),
            datatypes=('int', 'str', 'str', 'int'), keys=('x1', 'x2'),
            storage_type='monolithic')

        return tbl

    @pytest.fixture
    def data_twokey(self):
        dat = [
            (1, 'a', 'apple', 0),
            (1, 'b', 'pineapple', 1),
            (1, 'c', 'forest', 2),
            (2, 'd', 'woods', 3),
            (2, 'e', 'energy', 4),
            (3, 'f', 'down', 5),
            (3, 'g', 'home', 6),
            (3, 'h', 'ingot', 7),
            (3, 'j', 'ending', 8),
            (4, 'k', 'fire', 9)
        ]

        return dat

    @pytest.fixture
    def data_onekey(self):
        dat = [
            (1, 'a', 'apple', 0),
            (2, 'b', 'pineapple', 1),
            (3, 'c', 'forest', 2),
            (4, 'd', 'woods', 3),
            (5, 'e', 'energy', 4),
            (6, 'f', 'down', 5),
            (7, 'g', 'home', 6),
            (8, 'h', 'ingot', 7),
            (9, 'j', 'ending', 8),
            (10, 'k', 'fire', 9)
        ]

        return dat

    def test_column_select_one_column(self, tbl, data_onekey):
        tbl.add(data_onekey)

        assert len(tbl.select(columns='x1')) == 10
        assert sum([len(i) for i in tbl.select(columns='x1')]) == 10

    def test_column_select_one_column_correct_values(self, tbl, data_onekey):
        tbl.add(data_onekey)

        tbl.select(columns='x1') == [i[:1] for i in data_onekey]


class TestTableEditingSpliced:

    @pytest.fixture
    def dbpath(self, tmp_path):
        filebase.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbman(self, dbpath):
        return filebase.DatabaseManager(dbpath=dbpath)

    @pytest.fixture
    def tbl(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3', 'x4'),
            datatypes=('int', 'str', 'str', 'int'), keys=('x1',),
            storage_type='spliced', splice_keys=('x1',))

        return tbl

    @pytest.fixture
    def tbl2(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3', 'x4'),
            datatypes=('int', 'str', 'str', 'int'), keys=('x1', 'x2'),
            storage_type='spliced', splice_keys=('x1', 'x2'))

        return tbl

    @pytest.fixture
    def tbl3(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3', 'x4'),
            datatypes=('int', 'str', 'str', 'int'), keys=('x1', 'x2'),
            storage_type='spliced', splice_keys=('x1',))

        return tbl

    @pytest.fixture
    def data_twokey(self):
        dat = [
            (1, 'a', 'apple', 0),
            (1, 'b', 'pineapple', 1),
            (1, 'c', 'forest', 2),
            (2, 'd', 'woods', 3),
            (2, 'e', 'energy', 4),
            (3, 'f', 'down', 5),
            (3, 'g', 'home', 6),
            (3, 'h', 'ingot', 7),
            (3, 'j', 'ending', 8),
            (4, 'k', 'fire', 9)
        ]

        return dat

    @pytest.fixture
    def data_onekey(self):
        dat = [
            (1, 'a', 'apple', 0),
            (2, 'b', 'pineapple', 1),
            (3, 'c', 'forest', 2),
            (4, 'd', 'woods', 3),
            (5, 'e', 'energy', 4),
            (6, 'f', 'down', 5),
            (7, 'g', 'home', 6),
            (8, 'h', 'ingot', 7),
            (9, 'j', 'ending', 8),
            (10, 'k', 'fire', 9)
        ]

        return dat

    def test_column_select_one_column(self, tbl2, data_twokey):
        # breakpoint()
        tbl2.add(data_twokey)

        assert len(tbl2.select(columns='x1', splice_value=(1, 'a'))) == 1

        sizes = [len(i) for i in tbl2.select(columns='x1',
                                             splice_value=(1, 'b'))]
        assert sum(sizes) == 1

    def test_column_select_one_column_correct_values(self, tbl3, data_twokey):
        tbl3.add(data_twokey)

        selected_columns = tbl3.select(columns='x1', splice_value=(1,))

        assert selected_columns == [i[:1] for i in data_twokey][:3]


class TestTableMonolithicClass:

    @pytest.fixture
    def dbpath(self, tmp_path):
        filebase.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbman(self, dbpath):
        return filebase.DatabaseManager(dbpath=dbpath)

    @pytest.fixture
    def tbl(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'str'), keys=('x1',),
            storage_type='monolithic')

        return tbl

    @pytest.fixture
    def tbl2(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'str'), keys=('x1', 'x2'),
            storage_type='monolithic')

        return tbl

    def test_tbl_instantiation(self, tbl):
        tbl

    def test_meta_property(self, tbl):
        assert tbl.meta

    def test_meta_database_property(self, tbl):
        assert tbl.meta_database

    def test_table_directory_exists_from_meta(self, tbl):
        assert os.path.exists(tbl.meta['table_directory'])

    def test_table_meta_file_exists_from_meta(self, tbl):
        assert os.path.exists(tbl.meta['meta_file_location'])

    def test_has_correct_columns(self, tbl):
        assert tbl.columns == ('x1', 'x2', 'x3')

    def test_has_correct_datatypes(self, tbl):
        assert tbl.datatypes == ('int', 'str', 'str')

    def test_has_correct_last_modified_type(self, tbl):
        assert isinstance(tbl.last_modified, datetime.datetime)

    def test_has_correct_keys(self, tbl):
        assert tbl.keys == ('x1',)

    def test_has_correct_enforce_integrity(self, tbl):
        assert tbl.enforce_integrity is True

    def test_has_correct_nrecords(self, tbl):
        assert tbl.nrecords == 0

    def test_has_correct_storage_type(self, tbl):
        assert tbl.storage_type == 'monolithic'

    def test_has_filenames_no_records(self, tbl):
        assert tbl.filenames == []

    def test_adding_one_record_and_updates_meta(self, tbl):
        tbl.add(records=(1, 'hello', 'world'))

        assert tbl.nrecords == 1
        assert tbl.meta_database['total_table_records'] == 1

        with open(tbl.meta['meta_file_location'], 'r',
                  encoding='utf-8') as f:
            metafile = json.load(f)

            dbtemp = meta.MetaTableTemplate()
            for key in metafile:
                # breakpoint()
                typerule = dbtemp.get_typerule(key)

                # TODO
                # This is a bypass for foreign key, which can hold None
                # Check init_table function for the associated TODO there
                if key == 'foreign' or key == 'splice_keys':
                    pass
                else:
                    metafile[key] = typechecks.apply_typerule(metafile[key],
                                                              typerule)

        assert tbl.meta.metadata == metafile

        dbmeta_loc = tbl.meta_database['database_meta_location']
        with open(dbmeta_loc, 'r', encoding='utf-8') as f:
            metafile = json.load(f)

            dbtemp = meta.MetaDatabaseTemplate()
            for key in metafile:
                # breakpoint()
                typerule = dbtemp.get_typerule(key)

                # TODO
                # This is a bypass for foreign key, which can hold None
                # Check init_table function for the associated TODO there
                if key == 'foreign':
                    pass
                else:
                    metafile[key] = typechecks.apply_typerule(metafile[key],
                                                              typerule)

        assert tbl.meta_database.metadata == metafile

    def test_adding_three_record_and_updates_meta(self, tbl):
        tbl.add(records=[(1, 'hello', 'world'),
                         (2, 'data', 'record')])
        tbl.add(records=(3, 'beep', 'boop'))

        assert tbl.nrecords == 3
        assert tbl.meta_database['total_table_records'] == 3

        with open(tbl.meta['meta_file_location'], 'r',
                  encoding='utf-8') as f:
            metafile = json.load(f)

            dbtemp = meta.MetaTableTemplate()
            for key in metafile:
                # breakpoint()
                typerule = dbtemp.get_typerule(key)

                # TODO
                # This is a bypass for foreign key, which can hold None
                # Check init_table function for the associated TODO there
                if key == 'foreign' or key == 'splice_keys':
                    pass
                else:
                    metafile[key] = typechecks.apply_typerule(metafile[key],
                                                              typerule)

        assert tbl.meta.metadata == metafile

        dbmeta_loc = tbl.meta_database['database_meta_location']
        with open(dbmeta_loc, 'r', encoding='utf-8') as f:
            metafile = json.load(f)

            dbtemp = meta.MetaDatabaseTemplate()
            for key in metafile:
                # breakpoint()
                typerule = dbtemp.get_typerule(key)

                # TODO
                # This is a bypass for foreign key, which can hold None
                # Check init_table function for the associated TODO there
                if key == 'foreign':
                    pass
                else:
                    metafile[key] = typechecks.apply_typerule(metafile[key],
                                                              typerule)

        assert tbl.meta_database.metadata == metafile

    def test_adding_records_same_as_records_property_monolithic(
            self, tbl):
        dat = [
            (1, 'hello', 'world'),
            (2, 'data', 'record')
        ]

        tbl.add(records=dat)

        assert tbl.records == dat

    def test_adding_wrong_record_types(self, tbl):
        dat = [
            (1, 'hello', 'world'),
            [2, 'data', 'record']
        ]

        with pytest.raises(TypeError):
            tbl.add(records=dat)

    def test_check_index_size(self, tbl):
        dat = [
            (1, 'hello', 'world'),
            (2, 'data', 'record')
        ]

        tbl.add(records=dat)
        # Keyed on 'x1' (first element) should expect two indices
        len(dat) == len(tbl.index)

    def test_check_index_keys_and_values(self, tbl):
        dat = [
            (1, 'hello', 'world'),
            (2, 'data', 'record')
        ]

        tbl.add(records=dat)
        # Keyed on 'x1' (first element) should expect two indices
        assert all([(i[0],) == j for i, j in zip(dat, tbl.index)])

    def test_multi_key_check_index_size(self, tbl2):
        dat = [
            (1, 'hello', 'world'),
            (2, 'data', 'record')
        ]

        tbl2.add(records=dat)
        # Keyed on 'x1' (first element) should expect two indices
        len(dat) == len(tbl2.index)

    def test_check_index_multi_keys_and_values(self, tbl2):
        dat = [
            (1, 'hello', 'world'),
            (2, 'data', 'record')
        ]

        tbl2.add(records=dat)
        assert all([i[:2] == j for i, j in zip(dat, tbl2.index)])

    def test_check_file_index_keys_and_values(self, tbl2):
        dat = [
            (1, 'hello', 'world'),
            (2, 'data', 'record')
        ]

        tbl2.add(records=dat)

        with open(tbl2.meta['index_file_location'], 'rb') as f:
            fileindex = pickle.load(f)

        assert all([i[:2] == j for i, j in zip(dat, fileindex)])
        assert fileindex == tbl2.index

    def test_file_index_same_as_instance_index(self, tbl2):
        dat = [
            (1, 'hello', 'world'),
            (2, 'data', 'record'),
            (5, 'data', 'record')
        ]

        tbl2.add(records=dat)

        with open(tbl2.meta['index_file_location'], 'rb') as f:
            fileindex = pickle.load(f)

        assert fileindex == tbl2.index

    def test_with_two_raise_error(self, tbl):
        dat = [
            (1, 'hello', 'world'),
            (1, 'hello', 'world'),
            (2, 'data', 'record')
        ]

        # In keys should always be unique per row
        with pytest.raises(IndexError):
            tbl.add(records=dat)


class TestTableSplicedClass:

    @pytest.fixture
    def dbpath(self, tmp_path):
        filebase.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbman(self, dbpath):
        return filebase.DatabaseManager(dbpath=dbpath)

    @pytest.fixture
    def tbl(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3', 'x4'),
            datatypes=('int', 'str', 'str', 'int'), keys=('x1', 'x2'),
            storage_type='spliced', splice_keys=('x1',))

        return tbl

    @pytest.fixture
    def tbl_two_splice(self, dbman):
        tbl = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3', 'x4'),
            datatypes=('int', 'str', 'str', 'int'), keys=('x1', 'x2'),
            storage_type='spliced', splice_keys=('x1', 'x2'))

        return tbl

    @pytest.fixture
    def data(self):
        dat = [
            (1, 'a', 'apple', 0),
            (1, 'b', 'pineapple', 1),
            (1, 'c', 'forest', 2),
            (2, 'd', 'woods', 3),
            (2, 'e', 'energy', 4),
            (3, 'f', 'down', 5),
            (3, 'g', 'home', 6),
            (3, 'h', 'ingot', 7),
            (3, 'j', 'ending', 8),
            (4, 'k', 'fire', 9)
        ]

        return dat

    @pytest.fixture
    def data_dupe(self, data):
        data[1] = data[1][:1] + ('a',) + data[1][2:]

        return data

    def test_instantiation(self, tbl):
        tbl

    def test_has_correct_number_of_files(self, tbl, data):
        tbl.add(data)
        splice_keys = [i[:1] for i in data]
        splice_keys = set(splice_keys)

        spliced_files = os.listdir(tbl.meta['table_directory'])
        spliced_files = [i for i in spliced_files
                         if i.startswith('recs') and i.endswith('json')]

        assert len(splice_keys) == len(spliced_files)

    def test_has_correct_index_length(self, tbl, data):
        tbl.add(data)
        splice_keys = [i[:1] for i in data]
        splice_keys = set(splice_keys)

        assert len(splice_keys) == len(tbl.index)

    def test_has_correct_index_values(self, tbl, data):
        tbl.add(data)
        splice_keys = [i[:1] for i in data]
        splice_keys = set(splice_keys)

        assert all([i in tbl.index.keys() for i in splice_keys])

    def test_has_correct_file_index_length(self, tbl, data):
        tbl.add(data)
        splice_keys = [i[:1] for i in data]
        splice_keys = set(splice_keys)

        with open(tbl.meta['index_file_location'], 'rb') as f:
            fileindex = pickle.load(f)

        assert len(splice_keys) == len(fileindex)

    def test_has_correct_file_index_values(self, tbl, data):
        tbl.add(data)
        splice_keys = [i[:1] for i in data]
        splice_keys = set(splice_keys)

        with open(tbl.meta['index_file_location'], 'rb') as f:
            fileindex = pickle.load(f)

        assert all([i in fileindex.keys() for i in splice_keys])

    def test_raise_on_duplicate_key_input(self, tbl, data_dupe):
        with pytest.raises(IndexError):
            tbl.add(data_dupe)

    def test_raise_on_duplicate_key_multiadd(self, tbl, data):
        tbl.add(data)

        with pytest.raises(IndexError):
            new_data = [(1, 'c', 'happy', 11)]
            tbl.add(new_data)

    def test_datatype_checking_of_records(self, tbl):
        data = [
            (1, 'a', 'beta', 5),
            (2, 'b', 'alpha', 6),
            ('3', 'c', 'gamma', 10)
        ]

        with pytest.raises(TypeError):
            tbl.add(data)

    def test_correct_length_per_record(self, tbl):
        data = [
            (1, 'a', 'beta', 5),
            (2, 'b', 'alpha', 6),
            (3, 'c', 'gamma', 10, 15)
        ]

        with pytest.raises(ValueError):
            tbl.add(data)

    def test_splice_file_length_same(self, tbl, data):
        tbl.add(data)

        tbl.add([(1, 'z', 'mountain', 50)])

        assert len(tbl.filenames) == 4

    def test_splice_filenames_is_correct(self, tbl, data):
        tbl.add(data)

        splice_names = [i[0] for i in data]
        splice_names = list(set(splice_names))
        splice_names.sort()

        expected_names = ['recs__' + str(i) + '__spliced.json'
                          for i in splice_names]

        assert expected_names == sorted(tbl.filenames)

    def test_two_splice_file_length_same(self, tbl_two_splice, data):
        tbl_two_splice.add(data)

        tbl_two_splice.add([(1, 'z', 'mountain', 50)])

        assert len(tbl_two_splice.filenames) == 11

    def test_two_splice_filenames_is_correct(self, tbl_two_splice, data):
        tbl_two_splice.add(data)

        splice_names = [i[:2] for i in data]
        splice_names = list(set(splice_names))
        splice_names.sort()

        expected_names = ['recs__' + str(i) + '_' + str(j) + '__spliced.json'
                          for i, j in splice_names]

        assert expected_names == sorted(tbl_two_splice.filenames)

    def test_two_splice_has_correct_index_length(self, tbl_two_splice, data):
        tbl_two_splice.add(data)
        splice_keys = [i[:2] for i in data]
        splice_keys = set(splice_keys)

        assert len(splice_keys) == len(tbl_two_splice.index)

    def test_two_splice_has_correct_index_values(self, tbl_two_splice, data):
        tbl_two_splice.add(data)
        splice_keys = [i[:2] for i in data]
        splice_keys = set(splice_keys)

        assert all([i in tbl_two_splice.index.keys() for i in splice_keys])

    def test_two_splice_has_correct_file_index_length(self, tbl_two_splice,
                                                      data):
        tbl_two_splice.add(data)
        splice_keys = [i[:2] for i in data]
        splice_keys = set(splice_keys)

        with open(tbl_two_splice.meta['index_file_location'], 'rb') as f:
            fileindex = pickle.load(f)

        assert len(splice_keys) == len(fileindex)

    def test_two_splice_has_correct_file_index_values(self, tbl_two_splice,
                                                      data):
        tbl_two_splice.add(data)
        splice_keys = [i[:2] for i in data]
        splice_keys = set(splice_keys)

        with open(tbl_two_splice.meta['index_file_location'], 'rb') as f:
            fileindex = pickle.load(f)

        assert all([i in fileindex.keys() for i in splice_keys])

    def test_nrecords_adding_correctly(self, tbl, data):
        assert tbl.meta['nrecords'] == 0
        tbl.add(data)

        assert tbl.meta['nrecords'] == len(data)

        dat = [
            (1, 'e', 'body', 15)
        ]

        tbl.add(dat)

        assert tbl.meta['nrecords'] == (len(data) + 1)


class TestRetriever:

    @pytest.fixture
    def array_list(self):
        return list(range(10))

    @pytest.fixture
    def array_tuple(self):
        return tuple(range(10))

    @pytest.fixture
    def array_ndarray(self):
        return numpy.array(list(range(10)))

    def test_return_same_type_list(self, array_list):
        getter = filebase.retriever((1, 2))

        assert isinstance(getter(array_list), list)

    def test_return_same_type_tuple(self, array_tuple):
        getter = filebase.retriever((1, 2))

        assert isinstance(getter(array_tuple), tuple)

    def test_return_same_type_ndarray(self, array_ndarray):
        getter = filebase.retriever((1, 2))

        assert isinstance(getter(array_ndarray), numpy.ndarray)

    def test_returning_correct_values_list(self, array_list):
        getter = filebase.retriever((1, 2))
        assert getter(array_list) == [1, 2]

        getter = filebase.retriever((1, 6))
        assert getter(array_list) == [1, 6]

    def test_returning_correct_values_tuple(self, array_tuple):
        getter = filebase.retriever((1, 2))
        assert getter(array_tuple) == (1, 2)

        getter = filebase.retriever((1, 6))
        assert getter(array_tuple) == (1, 6)

    def test_works_with_list_sorting_int(self):
        a_list = [(5, 1), (1, 15), (60, 31)]
        getter = filebase.retriever(0)
        a_list.sort(key=getter)

        assert a_list == [(1, 15), (5, 1), (60, 31)]

    def test_works_with_list_sorting_str(self):
        a_list = [('b', 1), ('y', 15), ('a', 31)]
        getter = filebase.retriever(0)
        a_list.sort(key=getter)

        assert a_list == [('a', 31), ('b', 1), ('y', 15)]

    def test_works_with_groupby_int(self):
        a_list = [(5, 1), (5, 15), (60, 31)]
        getter = filebase.retriever(0)

        for key, rows in itertools.groupby(a_list, getter):
            rows = list(rows)
            if key == 5:
                assert len(rows) == 2
            elif key == 60:
                assert len(rows) == 1

    def test_works_with_groupby_multi(self):
        a_list = [(5, 1, 10), (5, 1, 70), (60, 31, 10)]
        getter = filebase.retriever((0, 1))

        for key, rows in itertools.groupby(a_list, getter):
            rows = list(rows)
            if key == (5, 1):
                assert len(rows) == 2
            elif key == (60, 31):
                assert len(rows) == 1
