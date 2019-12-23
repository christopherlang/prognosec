import json
import os
import copy
from typing import Sequence, Tuple
from progutils import progutils
from progutils import typechecks
from filebase import _meta as meta


class DatabaseManager:

    def __init__(self, dbpath):
        self._metatemplate = meta.MetaDatabaseTemplate()
        self._meta = self._load_metafile(dbpath=dbpath)

    @property
    def meta(self):
        return self._meta

    @property
    def template(self):
        return self._metatemplate

    def _load_metafile(self, dbpath):
        metafileloc = os.path.join(dbpath, 'database_meta.json')
        with open(metafileloc, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        for key in metadata:
            typerule = self._metatemplate.get_typerule(key)
            meta_value = metadata[key]
            meta_value = typechecks.parse_typerule(meta_value, typerule)
            metadata[key] = meta_value

        return meta.Meta(template=self.template, metadata=metadata)

    def create_table(
            self, name: str, columns: Tuple[str], datatypes: Tuple[str],
            keys: Tuple[str], foreign: Tuple[str] = None,
            storage_type: str = 'singular'):

        if name in self.meta.metadata['tables']:
            raise ValueError(f"Table '{name}' already exists")

        if storage_type not in ['monolithic', 'rolling', 'spliced']:
            vals = ['monolithic', 'rolling', 'spliced']
            raise ValueError(f"Only {vals} are valid 'storage_type'")

        storesdir = self.meta.metadata['database_data_root_directory']
        tbldir = os.path.join(storesdir, 'table__' + name)

        os.mkdir(tbldir)

        # Generate a new Meta object
        tblmeta = meta.Meta(template=meta.MetaTableTemplate())
        nowtime = progutils.utcnow()
        tblmeta.edit('name', name)
        tblmeta.edit('created', nowtime)
        tblmeta.edit('last_modified', nowtime)
        tblmeta.edit('columns', columns)
        tblmeta.edit('datatypes', datatypes)
        tblmeta.edit('keys', keys)
        tblmeta.edit('enforce_integrity', True)
        tblmeta.edit('nrecords', 0)
        tblmeta.edit(
            'database_directory', self.meta.metadata['database_directory'])
        tblmeta.edit('table_directory', tbldir)
        tblmeta.edit(
            'index_file_location', os.path.join(tbldir, 'metadata.json'))
        tblmeta.edit('storage_type', storage_type)

        # TODO
        # This is a bypass due to the conform_typerule issues when setting
        # things to `None`, when the typerule itself doesn't allow for `None`
        # e.g. typerule is tuple[int] but since it is optional you can set it
        # to `None`. But typerule doesn't allow it
        # A bypass is also placed in the test file, method:
        #   TestInitTableFunction::test_meta_file_created
        tblmeta._metadata['foreign'] = foreign
        tblmeta._keys_edited['foreign'] = True

        tblmeta.write(tblmeta.metadata['index_file_location'])

        return tblmeta

    # def get_table(self, name, columns, keys):
    #     pass
        # tblinstance = Table(name=name, columns=columns, keys=keys)


class Table:

    def __init__(self, meta: meta.Meta, dbman: meta.Meta):
        pass


def init_database(rootdir, dbdir_name='database'):
    rootdir = os.path.abspath(rootdir)
    dbdir_path = os.path.join(rootdir, dbdir_name)
    stores_path = os.path.join(dbdir_path, 'stores')

    if os.path.exists(rootdir) is False:
        raise FileNotFoundError(f"Directory '{rootdir}' does not exist")

    if os.path.exists(dbdir_path) is True:
        msg = f"Database directory '{dbdir_path}' already exists"
        raise FileExistsError(msg)

    os.mkdir(dbdir_path)
    os.mkdir(stores_path)

    # Generate a new Meta object
    dbmeta = meta.Meta(template=meta.MetaDatabaseTemplate())
    nowtime = progutils.utcnow()
    dbmeta.edit('name', dbdir_name)
    dbmeta.edit('tables', tuple())
    dbmeta.edit('collections', tuple())
    dbmeta.edit('created', nowtime)
    dbmeta.edit('last_modified', nowtime)
    dbmeta.edit('total_table_records', 0)
    dbmeta.edit('total_documents', 0)
    dbmeta.edit('database_directory', dbdir_path)
    dbmeta.edit('database_root_directory', rootdir)
    dbmeta.edit('database_data_root_directory', stores_path)
    dbmeta.edit('table_meta_locations', dict())
    dbmeta.edit('collection_meta_locations', dict())

    dbmeta_file_loc = os.path.join(dbdir_path, 'database_meta.json')
    dbmeta.edit('database_meta_location', dbmeta_file_loc)

    dbmeta.write(dbmeta_file_loc)

    return dbmeta


@typechecks.typecheck(name=str)
def init_table(name, columns, datatypes, keys, dbmeta: meta.Meta,
               foreign: Tuple[str] = None, storage_type: str = 'singular'):
    if name in dbmeta.metadata['tables']:
        raise ValueError(f"Table '{name}' already exists")

    storesdir = dbmeta.metadata['database_data_root_directory']
    tbldir = os.path.join(storesdir, 'table__' + name)

    os.mkdir(tbldir)

    # Generate a new Meta object
    tblmeta = meta.Meta(template=meta.MetaTableTemplate())
    nowtime = progutils.utcnow()
    tblmeta.edit('name', name)
    tblmeta.edit('created', nowtime)
    tblmeta.edit('last_modified', nowtime)
    tblmeta.edit('columns', columns)
    tblmeta.edit('datatypes', datatypes)
    tblmeta.edit('keys', keys)
    tblmeta.edit('enforce_integrity', True)
    tblmeta.edit('nrecords', 0)
    tblmeta.edit('database_directory', dbmeta.metadata['database_directory'])
    tblmeta.edit('table_directory', tbldir)
    tblmeta.edit('index_file_location', os.path.join(tbldir, 'metadata.json'))
    tblmeta.edit('storage_type', storage_type)

    # TODO
    # This is a bypass due to the conform_typerule issues when setting things
    # to `None`, when the typerule itself doesn't allow for `None`
    # e.g. typerule is tuple[int] but since it is optional you can set it
    # to `None`. But typerule doesn't allow it
    # A bypass is also placed in the test file, method:
    #   TestInitTableFunction::test_meta_file_created
    tblmeta._metadata['foreign'] = foreign
    tblmeta._keys_edited['foreign'] = True

    tblmeta.write(tblmeta.metadata['index_file_location'])

    return tblmeta
