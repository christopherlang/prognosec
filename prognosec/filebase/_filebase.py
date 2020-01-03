import json
import os
import shutil
import abc
import pickle
import itertools
import queue
import threading
from typing import Sequence, Tuple, List, Dict, Union, Callable
import numpy
from progutils import progutils
from progutils import typechecks
from filebase import _meta as meta


class DatabaseManager:

    def __init__(self, dbpath):
        self._metatemplate = meta.MetaDatabaseTemplate()
        self._meta = self._load_dbmeta(dbpath=dbpath)

    @property
    def meta(self):
        return self._meta

    @property
    def template(self):
        return self._metatemplate

    @property
    def tables(self):
        stores_directory = self.meta['database_data_root_directory']
        table_directories = os.listdir(stores_directory)
        table_directories = [i.replace('table__', '')
                             for i in table_directories]

        return tuple(table_directories)

    def _load_dbmeta(self, dbpath):
        metafileloc = os.path.join(dbpath, 'database_meta.json')
        with open(metafileloc, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        for key in metadata:
            typerule = self._metatemplate.get_typerule(key)
            meta_value = metadata[key]
            meta_value = typechecks.apply_typerule(meta_value, typerule)
            metadata[key] = meta_value

        return meta.Meta(template=self.template, metadata=metadata)

    def _load_tblmeta(self, tblname):
        tbl_dir = self.meta['database_data_root_directory']
        tablepath = os.path.join(tbl_dir, 'table__' + tblname)
        metapath = os.path.join(tablepath, 'metadata.json')

        with open(metapath, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        tbl_meta_template = meta.MetaTableTemplate()
        for key in metadata:
            typerule = tbl_meta_template.get_typerule(key)

            if (key != 'foreign') and (key != 'splice_keys'):
                meta_value = metadata[key]
                meta_value = typechecks.apply_typerule(meta_value, typerule)
                metadata[key] = meta_value

            elif key == 'foreign':
                metadata[key] = dict()

            elif key == 'splice_keys':
                metadata[key] = tuple(metadata[key])

        return meta.Meta(template=tbl_meta_template, metadata=metadata)

    def create_table(
            self, name: str, columns: Tuple[str], datatypes: Tuple[str],
            keys: Tuple[str], foreign: Tuple[str] = None,
            storage_type: str = 'monolithic', splice_keys: Tuple[str] = None):

        if name in self.meta['tables']:
            raise ValueError(f"Table '{name}' already exists")

        if storage_type not in ['monolithic', 'rolling', 'spliced']:
            vals = ['monolithic', 'rolling', 'spliced']
            raise ValueError(f"Only {vals} are valid 'storage_type'")

        storesdir = self.meta['database_data_root_directory']
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
            'index_file_location', os.path.join(tbldir, 'table_index.pkl'))
        tblmeta.edit(
            'meta_file_location', os.path.join(tbldir, 'metadata.json'))
        tblmeta.edit('storage_type', storage_type)
        # tblmeta.edit('splice_key', splice_key)

        # TODO
        # This is a bypass due to the conform_typerule issues when setting
        # things to `None`, when the typerule itself doesn't allow for `None`
        # e.g. typerule is tuple[int] but since it is optional you can set it
        # to `None`. But typerule doesn't allow it
        # A bypass is also placed in the test file, method:
        #   TestInitTableFunction::test_meta_file_created
        tblmeta._metadata['foreign'] = foreign
        tblmeta._keys_edited['foreign'] = True

        tblmeta._metadata['splice_keys'] = splice_keys
        tblmeta._keys_edited['splice_keys'] = True

        tblmeta.write(tblmeta.metadata['meta_file_location'])

        if storage_type == 'spliced':
            tblinstance = TableSpliced(
                table_meta=tblmeta, database_meta=self.meta)
        else:
            tblinstance = TableMonolithic(
                table_meta=tblmeta, database_meta=self.meta)

        # Update a few metadata
        # Table metdata
        tblinstance.meta.write(tblinstance.meta['meta_file_location'])
        # database metadata
        self.meta.edit('tables', (name,))
        self.meta.edit('last_modified', nowtime)
        self.meta.edit(
            'table_meta_locations', {name: tblmeta['meta_file_location']})

        return tblinstance

    def get_table(self, tblname):
        if tblname not in self.tables:
            raise ValueError(f"Table '{tblname}' does not exist")

        tblmeta = self._load_tblmeta(tblname)

        if tblmeta['storage_type'] == 'monolithic':
            tblinstance = TableMonolithic(
                table_meta=tblmeta, database_meta=self.meta)
        elif tblmeta['storage_type'] == 'spliced':
            tblinstance = TableSpliced(
                table_meta=tblmeta, database_meta=self.meta)

        return tblinstance

    def delete_table(self, tblname):
        if tblname not in self.tables:
            raise ValueError(f"Table '{tblname}' does not exist")

        tblmeta = self._load_tblmeta(tblname)
        tbl_nrecords = tblmeta['nrecords']

        tbldir = tblmeta['table_directory']
        shutil.rmtree(tbldir)

        tblnames = self.meta['tables']
        tblnames = tuple(i for i in tblnames if i != tblname)
        self.meta.edit('tables', tblnames)

        self.meta.edit('last_modified', progutils.utcnow())

        total_recs = self.meta['total_table_records'] - tbl_nrecords
        self.meta.edit('total_table_records', total_recs)


def retriever(indices):
    if isinstance(indices, int):
        indices = (indices,)
    elif typechecks.conforms_typerule(indices, 'tuple[int]'):
        pass
    elif typechecks.conforms_typerule(indices, 'list[int]'):
        indices = tuple(indices)
    else:
        raise TypeError("'indices' should be int, or list/tuple of int")

    def getter(array):

        output = [array[i] for i in indices]

        if isinstance(array, tuple):
            output = tuple(output)
        elif isinstance(array, list):
            pass
        if isinstance(array, numpy.ndarray):
            output = numpy.array(output)

        return output

    return getter


class TableAbstract(abc.ABC):

    def __init__(self, table_meta: meta.Meta, database_meta: meta.Meta):
        self._meta = table_meta
        self._meta_database = database_meta

        # Create or update index json file
        keys_index = [self.columns.index(i) for i in self.keys]
        self._keys_getter = retriever(keys_index)

        self._records = self._load_records()
        self._index = self._load_index()

    @property
    def meta(self):
        return self._meta

    @property
    def index(self):
        return self._index

    @index.setter
    @abc.abstractmethod
    def index(self, index: Dict):
        self._index = index

    @property
    def meta_database(self):
        return self._meta_database

    @property
    def columns(self):
        return self.meta['columns']

    @property
    def datatypes(self):
        return self.meta['datatypes']

    @property
    def last_modified(self):
        return self.meta['last_modified']

    @property
    def keys(self):
        return self.meta['keys']

    @property
    def enforce_integrity(self):
        return self.meta['enforce_integrity']

    @property
    def nrecords(self):
        return self.meta['nrecords']

    @property
    def storage_type(self):
        return self.meta['storage_type']

    @property
    def records(self):
        return self._records

    @property
    def filenames(self):
        table_filenames = os.listdir(self.meta['table_directory'])
        table_filenames = [i for i in table_filenames
                           if i.startswith('recs__') and i.endswith('.json')]

        return table_filenames

    @abc.abstractmethod
    def _load_records(self) -> List[Tuple]:
        pass

    @abc.abstractmethod
    def _load_index(self) -> Dict:
        pass

    @abc.abstractmethod
    def _save_records(self):
        pass

    @abc.abstractmethod
    def _save_index(self):
        pass

    @abc.abstractmethod
    def add(self, records: Union[List[Tuple], Tuple]):
        self._new_records_checks(records)

    @abc.abstractmethod
    def select(self, columns: Union[Tuple[str], str]):
        pass

    @abc.abstractclassmethod
    def retrieve(self):
        pass

    @abc.abstractmethod
    def _update_index(self):
        pass

    @abc.abstractmethod
    def _get_filename(self) -> str:
        pass

    @abc.abstractmethod
    def _get_filepath(self) -> str:
        pass

    def _new_records_checks(self, records: List[Tuple]):
        # typerule = 'list[tuple[str|int|float|bool]]'
        # if typechecks.conforms_typerule(records, typerule) is False:
        #     raise TypeError("'records' should be list of tuples")

        # Type check each record, each element
        for records_i in range(len(records)):
            a_record = records[records_i]

            if len(a_record) != len(self.columns):
                msg = f"Record index {records_i} "
                msg += f"is not of length {len(self.columns)}"

                raise ValueError(msg)

            for elem_i, typerule in zip(range(len(a_record)), self.datatypes):
                elem = a_record[elem_i]
                # expected_type = typechecks.typeconverts(typerule)

                if elem is not None:
                    # if isinstance(elem, expected_type) is False:
                    if typechecks.conforms_typerule(elem, typerule) is False:
                        msg = f"Record index {records_i}, "
                        msg += f"element index {elem_i}, "
                        msg += f"value {elem}, is not of expected type "
                        msg += f"'{typerule}'"

                        raise TypeError(msg)


class TableMonolithic(TableAbstract):
    """

    Index Structure: dict[tuple[<keys>], tuple[row_index]]
    """

    def __init__(self, table_meta: meta.Meta, database_meta: meta.Meta):
        super().__init__(table_meta, database_meta)

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, index):
        typerule = "dict[tuple[int|str|float|bool], tuple[int]]"
        if typechecks.conforms_typerule(index, typerule) is False:
            raise TypeError('Index does not conform to Monolithic index')

        self._index = index

    def _get_filename(self):
        return f"recs__{self.meta['name']}" + '__monolithic.json'

    def _get_filepath(self):
        filename = self._get_filename()
        filepath = os.path.join(self.meta['table_directory'], filename)

        return filepath

    def _load_records(self):
        tbldir = self.meta['table_directory']

        if self._get_filename() in self.filenames:
            filepath = os.path.join(tbldir, self._get_filename())

            with open(filepath, 'r', encoding='utf-8') as f:
                output = [tuple(i) for i in json.load(f)]
        else:
            output = list()

        return output

    def _load_index(self):
        indexfile_loc = self.meta['index_file_location']

        if os.path.exists(indexfile_loc) is True:
            with open(indexfile_loc, 'rb') as f:
                output = pickle.load(f)
        else:
            output = dict()

        return output

    def _save_records(self):
        if self.records:
            with open(self._get_filepath(), 'w', encoding='utf-8') as f:
                json.dump(self.records, f, indent=None)

    def _save_index(self):
        indexfile_loc = self.meta['index_file_location']
        with open(indexfile_loc, 'wb') as f:
            pickle.dump(self.index, f)

    def _update_index(self):
        mono_index = generate_index_monolithic(
            self.records, self.columns, self.keys, self._keys_getter)

        self.index = mono_index

    def add(self, records: Union[List[Tuple], Tuple],
            integrity_method: bool = 'raise'):
        if isinstance(records, tuple):
            records = [records]

        self._new_records_checks(records)

        if self.meta['enforce_integrity'] is True:

            for rec in records:

                if (self._keys_getter(rec) in self.index.keys()) is True:
                    if integrity_method == 'raise':
                        msg = f"index '{self._keys_getter(rec)}' "
                        msg += "already exists"
                        raise IndexError(msg)
                    elif integrity_method == 'skip':
                        pass
                else:
                    self.records.append(rec)
                    self._update_index()
        else:
            self.records.extend(records)
            self._update_index()

        # Update meta records
        self.meta.edit('nrecords', len(self.records))
        self.meta.edit('last_modified', progutils.utcnow())
        self.meta.write(self.meta['meta_file_location'])

        nnew_db_records = self.meta_database['total_table_records']
        nnew_db_records += len(records)
        self.meta_database.edit('total_table_records', nnew_db_records)
        self.meta_database.edit('last_modified', progutils.utcnow())
        dbmeta_loc = self.meta_database['database_meta_location']
        self.meta_database.write(dbmeta_loc)

        self._save_records()
        self._save_index()

    def select(self, columns: Union[Tuple[str], str]):
        if isinstance(columns, str):
            columns = (columns,)

        colindex = [self.columns.index(i) for i in columns]
        output = [tuple(i[j] for j in colindex) for i in self.records]

        return output

    def retrieve(self):
        return self.records


class TableSpliced(TableAbstract):
    """

    Index Structure: dict[tuple[<splice keys>], <path to spliced file>]
    """

    def __init__(self, table_meta: meta.Meta, database_meta: meta.Meta):
        super().__init__(table_meta, database_meta)

        self._splice_keys = self.meta['splice_keys']
        splice_key_index = [self.columns.index(i) for i in self._splice_keys]
        self._splice_key_getter = retriever(splice_key_index)

        # Create thread pool for writing spliced json
        self._thread_pool = ThreadPoolQueue(num_threads=50)

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, index):
        typerule = "dict[tuple[int|str|float|bool], str]"
        if typechecks.conforms_typerule(index, typerule) is False:
            raise TypeError('Index does not conform to Spliced index')

        self._index = index

    def _get_filename(self, splice_values: Tuple):
        splice_filename = '_'.join([str(i) for i in splice_values])
        splice_filename = 'recs__' + splice_filename + '__spliced.json'

        return splice_filename

    def _get_filepath(self, splice_values: Tuple):
        tbldir = self.meta['table_directory']
        filename = self._get_filename(splice_values)
        filepath = os.path.join(tbldir, filename)

        return filepath

    def _load_records(self, splice_values: Tuple = None) -> List[Tuple]:
        # breakpoint()
        if splice_values is None:
            output = list()
        else:
            filepath = self._get_filepath(splice_values)

            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    loaded_recs = json.load(f)
                    output = [tuple(i) for i in loaded_recs]
            else:
                output = list()

        return output

    def _load_index(self) -> Dict:
        indexfile_loc = self.meta['index_file_location']

        if os.path.exists(indexfile_loc) is True:
            with open(indexfile_loc, 'rb') as f:
                output = pickle.load(f)

        else:
            output = dict()

        return output

    def _splice_grouper(self, records: List[Tuple] = None):
        if records is None and len(self.records) == 0:
            raise ValueError("Internal records is empty")

        if records is not None:
            records = sorted(records, key=self._splice_key_getter)
        else:
            records = sorted(self.records, key=self._splice_key_getter)

        output = itertools.groupby(records, self._splice_key_getter)

        return output

    def _save_records(self, records: List[Tuple] = None):
        if records is None:
            records = self.records

        if len(records) == 0:
            raise ValueError("Internal records is empty")

        for splice_values, spliced_records in self._splice_grouper(records):
            spliced_records = [i for i in spliced_records]
            filepath = self._get_filepath(splice_values)

            existing_recs = self._load_records(filepath)
            existing_recs.extend(spliced_records)

            self._thread_pool.add_package(
                record_writer, records=existing_recs, filepath=filepath)

    def _save_index(self):
        indexfile_loc = self.meta['index_file_location']

        with open(indexfile_loc, 'wb') as f:
            pickle.dump(self.index, f)

    def _update_index(self, records: List[Tuple] = None):
        for splice_values, _ in self._splice_grouper(records):
            filepath = self._get_filepath(splice_values)

            if splice_values in self.index.keys():
                pass
            else:
                self.index[splice_values] = filepath

    def add(self, records: Union[List[Tuple], Tuple],
            integrity_method: bool = 'raise'):
        self._new_records_checks(records)

        self._records = list()
        num_new_recs = 0
        for splice_values, spliced_records in self._splice_grouper(records):

            existing_recs = self._load_records(splice_values)
            spliced_records = [i for i in spliced_records]

            if self.meta['enforce_integrity'] is True:
                new_keys = [self._keys_getter(i) for i in spliced_records]
                if len(new_keys) != len(set(new_keys)):
                    if integrity_method == 'raise':
                        raise IndexError("Records are not key unique")
                    elif integrity_method == 'skip':
                        continue

                new_keys = set(new_keys)

                existing_keys = [self._keys_getter(i) for i in existing_recs]
                existing_keys = set(existing_keys)

                if any([i in existing_keys for i in new_keys]) is True:
                    if integrity_method == 'raise':
                        raise IndexError("Records are not key unique")
                    elif integrity_method == 'skip':
                        continue

            new_records = existing_recs + spliced_records
            self._records.extend(new_records)
            num_new_recs += len(spliced_records)

        self._update_index()

        # Update meta records
        nrecords = self.meta['nrecords'] + num_new_recs
        self.meta.edit('nrecords', nrecords)
        self.meta.edit('last_modified', progutils.utcnow())
        self.meta.write(self.meta['meta_file_location'])

        nnew_db_records = self.meta_database['total_table_records']
        nnew_db_records += len(records)
        self.meta_database.edit('total_table_records', nnew_db_records)
        self.meta_database.edit('last_modified', progutils.utcnow())
        dbmeta_loc = self.meta_database['database_meta_location']
        self.meta_database.write(dbmeta_loc)

        self._save_records()
        self._save_index()

    def select(self, columns: Union[Tuple[str], str],
               splice_value: Tuple = None):
        if isinstance(columns, str):
            columns = (columns,)

        colindex = [self.columns.index(i) for i in columns]
        records = self._load_records(splice_value)

        output = [tuple(i[j] for j in colindex) for i in records]

        return output

    def retrieve(self, splice_value: Tuple):
        output = self.select(columns=self.columns, splice_value=splice_value)

        return output


def record_writer(records, filepath):

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(records, f)


class PackageWorker(threading.Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, package_queue: queue.Queue):
        threading.Thread.__init__(self)
        self._package_queue = package_queue
        self.daemon = True  # This is a attribute from Thread class
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self._package_queue.get()

            try:

                func(*args, **kwargs)

            except Exception:
                # An exception happened in this thread
                pass

            finally:
                # Mark this task as done, whether an exception happened or not
                self._package_queue.task_done()


class ThreadPoolQueue:
    """ Pool of threads consuming write packages from a queue """

    def __init__(self, num_threads: int):
        self._package_queue = queue.Queue(num_threads)

        for _ in range(num_threads):
            PackageWorker(self._package_queue)

    def add_package(self, func: Callable, *args, **kwargs):
        """ Add a write package to the queue """
        self._package_queue.put((func, args, kwargs))

    def add_packages(self, func: Callable, packages):
        """ Add a list of packages to the queue """
        for package in packages:
            self.add_package((func, package))

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self._package_queue.join()


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


def generate_index_monolithic(records, columns, keys, keys_getter):
    """Create a monolithic index

    A monolithic index is a `dict`, keyed on keys using `tuples`. The value
    is the row index.

    Provide the FULL record list, as this function regenerates the whole index
    as row indices will keep changing as we sort the record list prior.
    """

    records_enumerated = enumerate(records)

    def keys_grouper(x):
        return keys_getter(x[1])

    mono_index = dict()
    for key, rows in itertools.groupby(records_enumerated, keys_grouper):
        row_indices = tuple(i for i, _ in rows)
        mono_index[key] = row_indices

    return mono_index
