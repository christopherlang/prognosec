import pytest
import json
from filebase import filebase
from filebase import meta
from progutils import typechecks
import os
# import datetime
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
                loaded[key] = typechecks.parse_typerule(loaded[key], typerule)

        assert tmp.metadata == loaded


class TestInitTableFunction:

    @pytest.fixture
    def dbpath(self, tmp_path):
        filebase.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbmeta(self, dbpath):
        tmp = filebase.DatabaseManager(dbpath=dbpath)

        return tmp.meta

    def test_typerule_checking(self, dbmeta):
        with pytest.raises(TypeError):
            filebase.init_table(
                name=600, columns=('x1', 'x2', 'x3'),
                datatypes=('int', 'str', 'int'), keys=('x1',),
                dbmeta=dbmeta)

    def test_raise_on_existing_table(self, dbmeta):
        dbmeta.edit('tables', ('new_table',))

        with pytest.raises(ValueError):
            filebase.init_table(
                name='new_table', columns=('x1', 'x2', 'x3'),
                datatypes=('int', 'str', 'int'), keys=('x1',),
                dbmeta=dbmeta)

    def test_table_directory_created(self, dbmeta):
        tblmeta = filebase.init_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            dbmeta=dbmeta)

        assert os.path.exists(tblmeta.metadata['table_directory'])

    def test_meta_file_created(self, dbmeta):
        tblmeta = filebase.init_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            dbmeta=dbmeta)

        metaloc = tblmeta.metadata['index_file_location']
        with open(metaloc, 'r', encoding='utf-8') as f:
            loaded = json.load(f)

            dbtemp = meta.MetaTableTemplate()
            for key in loaded:
                # breakpoint()
                typerule = dbtemp.get_typerule(key)

                # TODO
                # This is a bypass for foreign key, which can hold None
                # Check init_table function for the associated TODO there
                if key == 'foreign':
                    pass
                else:
                    loaded[key] = typechecks.parse_typerule(loaded[key],
                                                            typerule)

        assert tblmeta.metadata == loaded


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
            assert typechecks.conforms_to_typerule(value, typerule)

    def test_create_table(self, dbman_instance):
        tbl = dbman_instance.create_table(
            name='new_table', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'int'), keys=('x1',),
            storage_type='monolithic')

        assert isinstance(tbl, filebase.Table)

    # def test_get_table_return_Table_instance(self, dbman_instance):
    #     filebase.init_table(
    #         name='new_table', columns=('x1', 'x2', 'x3'),
    #         datatypes=('int', 'str', 'int'), keys=('x1',),
    #         dbmeta=dbman_instance.meta)

    #     assert isinstance(dbman_instance.get_table('table1'), filebase.Table)


class TestTableClass:

    @pytest.fixture
    def dbpath(self, tmp_path):
        filebase.init_database(tmp_path, 'database')

        return os.path.join(tmp_path, 'database')

    @pytest.fixture
    def dbman(self, dbpath):
        return filebase.DatabaseManager(dbpath=dbpath)

    @pytest.fixture
    def tbl(self, dbman):
        tblmeta = dbman.create_table(
            name='table1', columns=('x1', 'x2', 'x3'),
            datatypes=('int', 'str', 'str'), keys=('x1',),
            storage_type='monolithic')

        return filebase.Table(meta=tblmeta, dbman=dbman)

    def test_tbl_instantiation(self, tbl):
        tbl
