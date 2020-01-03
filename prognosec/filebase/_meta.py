import json
import copy
import collections
import datetime
from typing import Sequence, Tuple
from progutils import typechecks


class MetaTemplate:
    """Meta file template

    Meant only for subclassing.

    A template instance stores the requirements for a specific meta file for
    JSON storage. Keys, data types it can hold, whether it is required, etc.
    are defined in templates.

    Attributes
    ----------
    metadata : collections.OrderedDict
        A dictionary representation of the template. Can only be accessed if
        all template keys are added to the template
    template_keys : tuple[str]
        The required keys to be filled in
    frozen : bool
        Whether or not if you can edit the template instance. If `True`, than
        no additional key/values can be added or removed

    Parameters
    ----------
    template_keys: array_like[str]
        The required keys to be added to the template
    """
    @typechecks.typecheck(template_keys=(list, tuple))
    def __init__(self, template_keys: Sequence[str]):
        if all([isinstance(i, str) for i in template_keys]) is False:
            raise TypeError("All values in 'template_keys' must be string")

        self._metadata = collections.OrderedDict()
        self._template_keys = tuple(template_keys)
        self._frozen = False

    @property
    def metadata(self):

        if self._is_template_filled() is False:
            meta_keys = self._metadata.keys()
            meta_key_in_template = [i in meta_keys for i in self.template_keys]

            key_iter = zip(self.template_keys, meta_key_in_template)
            missing_keys = [i for i, j in key_iter if j is False]

            msg = "Cannot extract metadata. Template keys "
            msg += "(" + ", ".join(missing_keys) + ") are missing"

            raise KeyError(msg)

        else:
            return self._metadata

    @property
    def template_keys(self):
        return self._template_keys

    @property
    def frozen(self):
        return self._frozen

    @frozen.setter
    def frozen(self, frozen_state: bool):
        if frozen_state is True:
            if self._is_template_filled() is True:
                self._frozen = True
            else:
                raise TypeError("Cannot be frozen as template is not filled")
        else:
            self._frozen = False

    def _is_template_filled(self):
        meta_keys = self._metadata.keys()
        meta_key_in_template = [i in meta_keys for i in self.template_keys]

        return all(meta_key_in_template)

    @typechecks.typecheck(key=str, required=bool)
    def add_typespec(self, key: str, typerule, required=False, default=None):
        if self.frozen is False:
            if key not in self.template_keys:
                msg = f"key '{key}' is not a valid key. "
                msg += "See 'template_keys' property"
                raise ValueError(msg)

            typespec = (typerule, required, default)
            self._metadata[key] = typespec
        else:
            raise TypeError("Not supported as template has been frozen")

    @typechecks.typecheck(key=str)
    def delete_typespec(self, key: str):
        if self.frozen is False:
            del self._metadata[key]
        else:
            raise TypeError("Not supported as template has been frozen")

    @typechecks.typecheck(key=str)
    def get_typerule(self, key):
        if key not in self.template_keys:
            msg = f"key '{key}' is not a valid key. "
            msg += "See 'template_keys' property"
            raise KeyError(msg)

        return self._metadata[key][0]

    @typechecks.typecheck(key=str)
    def get_default(self, key):
        if key not in self.template_keys:
            msg = f"key '{key}' is not a valid key. "
            msg += "See 'template_keys' property"
            raise KeyError(msg)

        return self._metadata[key][2]

    @typechecks.typecheck(key=str)
    def get_required(self, key):
        if key not in self.template_keys:
            msg = f"key '{key}' is not a valid key. "
            msg += "See 'template_keys' property"
            raise KeyError(msg)

        return self._metadata[key][1]


class MetaDatabaseTemplate(MetaTemplate):
    template_keys = {
        'name': ("str", True, None),
        'tables': ("tuple[str]", True, None),
        'collections': ("tuple[str]", True, None),
        'created': ("datetime_second", True, None),
        'last_modified': ("datetime_second", True, None),
        'total_table_records': ("int", True, 0),
        'total_documents': ("int", True, 0),
        'database_directory': ("str", True, None),
        'database_root_directory': ("str", True, None),
        'database_meta_location': ('str', True, None),
        'database_data_root_directory': ('str', True, None),
        'table_meta_locations': ("dict[str, str]", True, None),
        'collection_meta_locations': ("dict[str, str]", True, None)
    }

    def __init__(self):
        template_key_names = MetaDatabaseTemplate.template_keys.keys()
        template_key_names = list(template_key_names)
        super().__init__(template_keys=template_key_names)

        for a_key, rules in MetaDatabaseTemplate.template_keys.items():
            typerule = rules[0]
            value_requirement = rules[1]
            default_value = rules[2]

            self.add_typespec(key=a_key, typerule=typerule,
                              required=value_requirement,
                              default=default_value)

        self.frozen = True


class MetaTableTemplate(MetaTemplate):
    template_keys = {
        'name': ("str", True, None),
        'created': ("datetime_second", True, None),
        'last_modified': ("datetime_second", True, None),
        'columns': ("tuple[str]", True, None),
        'datatypes': ("tuple[str]", True, None),
        'keys': ("tuple[str]", True, None),
        'foreign': ("dict[str, str]", False, None),
        'enforce_integrity': ("bool", True, None),
        'nrecords': ("int", True, None),
        'database_directory': ("str", True, None),
        'table_directory': ("str", True, None),
        'index_file_location': ("str", True, None),
        'meta_file_location': ('str', True, None),
        'storage_type': ("str", True, None),
        'splice_keys': ("tuple[str]", False, None)
    }

    def __init__(self):
        template_key_names = MetaTableTemplate.template_keys.keys()
        template_key_names = list(template_key_names)

        super().__init__(template_keys=template_key_names)

        for a_key, rules in MetaTableTemplate.template_keys.items():
            typerule = rules[0]
            value_requirement = rules[1]
            default_value = rules[2]

            self.add_typespec(key=a_key, typerule=typerule,
                              required=value_requirement,
                              default=default_value)

        self.frozen = True


class Meta:

    @typechecks.typecheck(template=MetaTemplate)
    def __init__(self, template: MetaTemplate, metadata: dict = None,
                 metapath: str = None):
        self._template = template

        temp_items = self._template.metadata.items()
        self._metadata = {k: v[2] for k, v in temp_items}
        self._keys_edited = {k: False for k, v in temp_items}

        if metadata is not None:
            for key in metadata:
                self.edit(key, metadata[key])

        if metapath is not None:
            metadata = self.read(metapath)

            for key in metadata:
                self.edit(key, metadata[key])

    @property
    def metadata(self):
        if self.is_valid is False:
            raise KeyError("Not all required keys has been filled")

        return self._metadata

    @property
    def template(self):
        return self._template

    @property
    def is_valid(self):
        return all(self._keys_edited.values())

    def replace(self, key: str, value):
        if key not in self._metadata.keys():
            raise KeyError(f"Template did not define a key '{key}'")

        template_typerule = self.template.get_typerule(key)
        if typechecks.conforms_typerule(value, template_typerule) is False:
            msg = f"value '{value}' does not conform to typerule "
            msg += f"'{template_typerule}'"
            raise TypeError(msg)

        self._metadata[key] = value
        self._keys_edited[key] = True
        # if self.template.get_default(key) is None:
        #     if value is not None:
        #         self._keys_edited[key] = True
        # else:
        #     if value != self.template.get_default(key):
        #         self._keys_edited[key] = True

    def edit(self, key: str, value):
        if key not in self._metadata.keys():
            raise KeyError(f"Template did not define a key '{key}'")

        template_typerule = self.template.get_typerule(key)
        template_required = self.template.get_required(key)
        if template_required is False and value is None:
            # TODO: Bandaid for foreign key
            # raise TypeError
            pass
        elif (typechecks.conforms_typerule(value, template_typerule) is
                False):
            msg = f"value '{value}' does not conform to typerule "
            msg += f"'{template_typerule}'"
            raise TypeError(msg)

        if template_typerule.find("[") >= 0:

            rootstruct = template_typerule[:template_typerule.find("[")]
            currvalue = self._metadata[key]

            if rootstruct == 'list' and isinstance(currvalue, list):
                self._metadata[key].extend(value)
            elif rootstruct == 'tuple' and isinstance(currvalue, tuple):
                self._metadata[key] = self._metadata[key] + value
            elif rootstruct == 'set' and isinstance(currvalue, set):
                self._metadata[key].union(value)
            elif rootstruct == 'dict' and isinstance(currvalue, dict):
                self._metadata[key].update(value)
            elif rootstruct in ['list', 'tuple', 'set', 'dict']:
                # Capture instances where currvalue is not any of the structs
                self._metadata[key] = value
            else:
                raise TypeError("Sequence-like object not supported")

        else:
            # Capture singletons e.g. 'int', 'float'
            self._metadata[key] = value

        self._keys_edited[key] = True
        # if self.template.get_default(key) is None:
        #     if value is not None:
        #         self._keys_edited[key] = True
        # else:
        #     if value != self.template.get_default(key):
        #         self._keys_edited[key] = True

    def reset(self, key: str):
        self._metadata[key] = self.template.get_default(key)

    def read(self, path: str):
        with open(path, 'r', encoding='utf-8') as f:
            read_metadata = json.load(f)

        return read_metadata

    def write(self, path: str):
        metadata = copy.deepcopy(self.metadata)

        for key in metadata:
            value = metadata[key]

            if isinstance(value, (datetime.datetime, datetime.date)):
                typerule = self.template.get_typerule(key)

                if typerule == 'datetime_second':
                    strformat = '%Y-%m-%dT%H:%M:%SZ'
                elif typerule == 'datetime_microsecond':
                    strformat = '%Y-%m-%dT%H:%M:%S.%fZ'
                elif typerule == 'datetime':
                    strformat = '%Y-%m-%d'
                else:
                    raise TypeError('Datetime typerule is invalid')

                metadata[key] = value.strftime(format=strformat)

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=0, ensure_ascii=False)

    def __getitem__(self, key):
        return self.metadata[key]

    def __setitem__(self, key, value):
        self.replace(key, value)
