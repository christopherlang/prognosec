# def meta_template_table():
#     table_meta_template = {
#         "name": ("str", "required"),
#         "created": ("ISO8601|DATETSZ", "required"),
#         "last_modified": ("ISO8601|DATETSZ", "required"),
#         "columns": ("tuple[str]", "required"),
#         "datatypes": ("tuple[dtypes]", "required"),
#         "keys": ("tuple[str]", "required"),
#         "foreign": ("dict[str, str]", "optional"),
#         "enforce_integrity": ("bool", "required"),
#         "nrecords": ("int", "required"),
#         "database_directory": ("str", "required"),
#         "table_directory": ("str", "required"),
#         "index_file_location": ("str", "required"),
#         "storage_type": ("str", "required")
#     }

#     return copy.deepcopy(table_meta_template)


# def meta_template_database():
#     database_meta_template = {
#         "name": ("str", "required"),
#         "tables": ("tuple[str]", "required"),
#         "collections": ("tuple[str]", "required"),
#         "created": ("ISO8601|DATETSZ", "required"),
#         "last_modified": ("ISO8601|DATETSZ", "required"),
#         "total_table_records": ("int", "required"),
#         "total_documents": ("int", "required"),
#         "database_directory": ("str", "required"),
#         "root_directory": ("str", "required"),
#         "table_meta_locations": ("dict[str, str]", "required"),
#         "collection_meta_locations": ("dict[str, str]", "required")
#     }

#     return copy.deepcopy(database_meta_template)


# def is_spec_valid(spec):
#     return True


# def isostr2dt(isostring, hastime=True, hasmicro=True):
#     iso_format = "%Y-%m-%d"

#     if isostring.endswith('Z'):
#         isostring = isostring.replace('Z', '')

#     if hastime is True:
#         iso_format += "T%H:%M:%S"

#     if hasmicro is True:
#         iso_format += ".%f"
#     else:
#         isostring = isostring.partition(".")[0]

#     output = datetime.datetime.strptime(isostring, iso_format)

#     return output


# typeconverts = {
#     "str": str,
#     "float": float,
#     "int": int,
#     'bool': bool,
#     "ISO8601|DATETSZ": functools.partial(isostr2dt, hastime=True,
#                                          hasmicro=False),
#     "ISO8601|DATETMZ": functools.partial(isostr2dt, hastime=True,
#                                          hasmicro=False),
#     "ISO8601|DATE___": functools.partial(isostr2dt, hastime=False,
#                                          hasmicro=False),
#     "tuple": tuple,
#     "list": list,
#     "set": set,
#     'dict': dict,
# }


# class DatabaseManager:
#     def __init__(self, db_directory):
#         db_directory = os.path.abspath(db_directory)
#         stores_directory = os.path.join(db_directory, 'stores')

#         if os.path.exists(db_directory) is False:
#             msg = f"Database directory '{db_directory}' "
#             msg += "does not exist"
#             raise FileNotFoundError(msg)

#         if os.path.exists(stores_directory) is False:
#             store_path = os.path.join(db_directory, 'stores')
#             msg = f"Stores directory '{store_path}' does not exist"
#             raise FileNotFoundError(msg)

#         self._database_directory = db_directory
#         self._stores_directory = stores_directory
#         self._meta_location = os.path.join(self._database_directory,
#                                            'database_meta.json')

#         with open(self._meta_location, 'r', encoding='utf-8') as f:
#             self._database_meta = typeconvert_meta(json.load(f))

#     @property
#     def database_directory(self):
#         return self._database_directory

#     @property
#     def stores_directory(self):
#         return self._stores_directory

#     @property
#     def database_meta(self):
#         return self._database_meta

#     @property
#     def stores(self):
#         dbdir = self.stores_directory
#         stores_dirname = os.listdir(dbdir)
#         stores_dirname = [i for i in stores_dirname
#                           if os.path.isdir(os.path.join(dbdir, i))]

#         stores = dict()
#         for a_name in stores_dirname:
#             store_name = a_name[::-1].rsplit('_', 1)[0][::-1]
#             store_type = a_name[::-1].rsplit('_', 1)[1][::-1]

#             try:
#                 stores[store_type].append(store_name)
#             except KeyError:
#                 stores[store_type] = [store_name]

#         if stores:
#             return stores
#         else:
#             return None

#     def update_meta(self, key, value):
#         if key not in self.database_meta.keys():
#             raise KeyError(f"key '{key}' does not exist in 'database_meta'")

#         self.database_meta[key] = apply_typesec(
#             key, value, meta_template_database(), self.database_meta[key]
#         )

#         if key in ('created', 'last_modified'):
#             # Convert string datetime back to python datetime
#             self.database_meta[key] = typeconverts['ISO8601|DATETSZ'](value)

#         meta_output = copy.deepcopy(self.database_meta)
#         meta_output['created'] = meta_output['created'].isoformat() + 'Z'
#         meta_output['last_modified'] = meta_output['last_modified'].isoformat() + 'Z'

#         with open(self._meta_location, 'w', encoding='utf-8') as f:
#             json.dump(meta_output, f)

#     def create_table(self, tblname, columns, keys, datatypes, foreign=None,
#                      enforce_integrity=True, overwrite=False):
#         """Create a new table store

#         Parameters
#         ----------
#         tblname : str
#         columns : tuple[str]
#         keys : tuple[str]
#         datatypes : tuple[str]
#         foreign : dict[str, dict], optional
#             Not implemented yet
#         overwrite : bool, Optional
#             If the table already exists (by name), overwrite it. This deletes
#             all existing data in that store
#         """
#         dbdir = self._database_directory
#         metadata = generate_new_table_schema(
#             dbdir=dbdir, tblname=tblname, columns=columns, keys=keys,
#             datatypes=datatypes, foreign=foreign,
#             enforce_integrity=enforce_integrity, overwrite=overwrite)

#         meta_save_location = metadata['to_save_location']
#         metadata = metadata['meta']
#         # Create directories for storing tables
#         os.mkdir(metadata['table_directory'])

#         with open(meta_save_location, 'w', encoding='utf-8') as f:
#             json.dump(metadata, f)

#         with open(metadata['index_file_location'], 'w', encoding='utf-8') as f:
#             json.dump({}, f)

#         output = {
#             'meta': metadata,
#             'table_directory': metadata['table_directory'],
#             'table_meta_file': meta_save_location
#         }

#         # tables
#         # last_modified
#         self.update_meta('tables', (tblname,))
#         self.update_meta('last_modified', datetime.datetime.utcnow())

#         return output

#     def store_table_exists(self, tblname):
#         return tblname in self.stores

#     # def get_table(self, tblname):


#     def _verify_database_integrity(self):
#         # Verify integrity of database meta file 'database_meta.json'
#         pass


# class Table:

#     def __init__(self, table_meta):
#         if isinstance(table_meta, str) is True:
#             with open(table_meta, 'r', encoding='utf-8') as f:
#                 self._meta = json.load(f)
#         else:
#             self._meta = table_meta

#         self._rootdir = table_meta['table_directory']

#     @property
#     def meta(self):
#         return self._meta


# def init_database(rootdir):

#     rootdir = os.path.expanduser(rootdir)
#     rootdir = os.path.realpath(rootdir)
#     dbdir = os.path.join(rootdir, "database")
#     storedir = os.path.join(dbdir, "stores")
#     metadir = os.path.join(dbdir, "database_meta.json")

#     # path validations
#     if os.path.exists(rootdir) is False:
#         raise FileNotFoundError(f"Directory '{rootdir}' does not exist")

#     if os.path.exists(dbdir) is True:
#         raise FileExistsError(f"Database directory '{dbdir}' already exists")

#     new_meta = meta_template_database()
#     new_meta['name'] = "database"
#     new_meta['tables'] = tuple()
#     new_meta['collections'] = tuple()
#     new_meta['created'] = datetime.datetime.utcnow().isoformat() + "Z"
#     new_meta['last_modified'] = new_meta['created']
#     new_meta['total_table_records'] = 0
#     new_meta['total_documents'] = 0
#     new_meta['root_directory'] = rootdir
#     new_meta['database_directory'] = dbdir
#     new_meta['stores_directory'] = storedir
#     new_meta['table_meta_locations'] = dict()
#     new_meta['collection_meta_locations'] = dict()

#     # Create directories
#     os.mkdir(dbdir)
#     os.mkdir(storedir)

#     with open(metadir, 'w', encoding='utf-8') as f:
#         json.dump(new_meta, f)

#     return new_meta


# def generate_new_table_schema(dbdir, tblname, columns, keys, datatypes,
#                               storage_type='singleton', foreign=None,
#                               enforce_integrity=None, overwrite=False):
#     dbdir = os.path.expanduser(dbdir)
#     dbdir = os.path.realpath(dbdir)

#     tbdir = os.path.join(dbdir, "stores", "table_" + tblname)
#     metaloc = os.path.join(tbdir, "table_schema.json")
#     idxloc = os.path.join(tbdir, "index.json")

#     # path validations
#     if os.path.exists(dbdir) is False:
#         msg = f"'{dbdir}' does not exist. Was a database created?"
#         raise FileNotFoundError(msg)

#     if os.path.exists(tbdir) is True and overwrite is False:
#         msg = f"Table '{tblname}' already exists in '{dbdir}'"
#         raise FileExistsError(msg)

#     # kwargs validations
#     if len(datatypes) != len(columns):
#         raise ValueError("Number of 'datatypes' must equal to 'columns'")

#     if len(keys) > len(columns):
#         msg = "Cannot have more 'keys' than there are 'columns'"
#         raise ValueError(msg)

#     if all([i in columns for i in keys]) is False:
#         raise ValueError("All key names must be found in 'columns'")

#     new_meta = meta_template_table()
#     new_meta['name'] = tblname
#     new_meta['database_directory'] = dbdir
#     new_meta['table_directory'] = tbdir
#     new_meta['index_file_location'] = idxloc
#     new_meta['created'] = datetime.datetime.utcnow().isoformat() + "Z"
#     new_meta['last_modified'] = new_meta['created']
#     new_meta['columns'] = columns
#     new_meta['datatypes'] = datatypes
#     new_meta['keys'] = keys
#     new_meta['nrecords'] = 0

#     try:
#         new_meta['foreign'] = foreign
#     except KeyError:
#         new_meta['foreign'] = None

#     try:
#         new_meta['enforce_integrity'] = enforce_integrity
#     except KeyError:
#         new_meta['enforce_integrity'] = True

#     # Dealing with storage types
#     new_meta['storage_type'] = storage_type

#     return {'meta': new_meta, 'to_save_location': metaloc}


# def typeconvert_meta(metadata, template_type='database'):
#     if template_type == 'database':
#         meta_template = meta_template_database()
#     elif template_type == 'table':
#         meta_template = meta_template_table()
#     else:
#         raise ValueError("Template type not supported")

#     meta = copy.deepcopy(metadata)

#     for key_name in meta_template:
#         meta[key_name] = apply_typesec(key_name, meta[key_name], meta_template)

#     return meta

# # TODO omg
# # this function is for converting JSON output TO python representation
# def apply_typesec(key, new_value, template, old_value=None):
#     typespec, _ = template[key]
#     typespec = parse_typespec(typespec)
#     new_value = copy.deepcopy(new_value)
#     old_value = copy.deepcopy(old_value)

#     if typespec[0] in ('set', 'tuple', 'list'):

#         mainspec = typeconverts[typespec[0]]
#         secondaryspec = typeconverts[typespec[1]]

#         if isinstance(new_value, (set, tuple, list)):

#             if old_value is not None:
#                 if isinstance(new_value, set):
#                         new_value.update(old_value)
#                 else:
#                     new_value = new_value + old_value

#         else:
#             if old_value is not None:
#                 if typespec[0] == 'set':
#                     old_value.add(new_value)
#                     new_value = old_value
#                 else:
#                     new_value = [new_value] + old_value

#         output = [secondaryspec(i) for i in new_value]
#         output = mainspec(output)

#     elif typespec[0] == 'dict':

#         keyspec = typeconverts[typespec[1]]
#         valuespec = typeconverts[typespec[2]]

#         if old_value is not None:
#             new_value.update(old_value)

#         output = {keyspec(k): valuespec(v) for k, v in new_value.items()}

#     else:

#         mainspec = typeconverts[typespec[0]]
#         output = mainspec(new_value)

#     return output


# def parse_typespec(typespec):
#     if typespec.find('[') >= 0:
#         # Usually a sequence of some type, or a dict
#         seq_type, inner_types = typespec.rsplit('[')
#         inner_types = inner_types.replace(']', '')
#         inner_types = inner_types.replace(' ', '')

#         if inner_types.find(',') >= 0:
#             # There is more than one inner types
#             # Final inner_types should contain at most 2 types
#             inner_types = inner_types.rsplit(',')

#             output = (seq_type,) + tuple(inner_types)
#         else:
#             # Only one inner type was found
#             # e.g. tuple(str)
#             output = (seq_type, inner_types, None)
#     else:
#         output = (typespec, None, None)

#     return output
