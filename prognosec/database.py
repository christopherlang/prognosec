# import json
import datetime
import functools
import re
# import collections
import os
# import sys
# import operator
# import copy
from typing import Sequence
import collections
from progutils import progutils

BRACKETREGEX = re.compile(r"\[[a-zA-Z0-9,\|\[\]]+\]", flags=re.I)
WHITESPACEREGEX = re.compile(r"\s+")


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
    @progutils.typecheck(template_keys=(list, tuple))
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

    @progutils.typecheck(key=str, required=bool)
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

    @progutils.typecheck(key=str)
    def delete_typespec(self, key: str):
        if self.frozen is False:
            del self._metadata[key]
        else:
            raise TypeError("Not supported as template has been frozen")

    @progutils.typecheck(key=str)
    def get_typerule(self, key):
        if key not in self.template_keys:
            msg = f"key '{key}' is not a valid key. "
            msg += "See 'template_keys' property"
            raise KeyError(msg)

        return self._metadata[key][0]

    @progutils.typecheck(key=str)
    def get_default(self, key):
        if key not in self.template_keys:
            msg = f"key '{key}' is not a valid key. "
            msg += "See 'template_keys' property"
            raise KeyError(msg)

        return self._metadata[key][2]

    @progutils.typecheck(key=str)
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
        'created': ("ISO8601|DATETSZ", True, None),
        'last_modified': ("ISO8601|DATETSZ", True, None),
        'total_table_records': ("int", True, None),
        'total_documents': ("int", True, None),
        'database_directory': ("str", True, None),
        'root_directory': ("str", True, None),
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
        'created': ("ISO8601|DATETSZ", True, None),
        'last_modified': ("ISO8601|DATETSZ", True, None),
        'columns': ("tuple[str]", True, None),
        'datatypes': ("tuple[dtypes]", True, None),
        'keys': ("tuple[str]", True, None),
        'foreign': ("dict[str, str]", False, None),
        'enforce_integrity': ("bool", True, None),
        'nrecords': ("int", True, None),
        'database_directory': ("str", True, None),
        'table_directory': ("str", True, None),
        'index_file_location': ("str", True, None),
        'storage_type': ("str", True, None)
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

    @progutils.typecheck(template=MetaTemplate)
    def __init__(self, template):
        self._template = template

        temp_items = self._template.metadata.items()
        self._metadata = {k: v[2] for k, v in temp_items}
        self._keys_edited = {k: False for k, v in temp_items}

    @property
    def metadata(self):
        if all(self._keys_edited.values()) is False:
            raise KeyError("Not all required keys has been filled")

        return self._metadata

    @property
    def template(self):
        return self._template

    def replace(self, key, value):
        if key not in self._metadata.keys():
            raise KeyError(f"Template did not define a key '{key}'")

        template_typerule = self.template.get_typerule(key)
        if conforms_to_typerule(value, template_typerule) is False:
            msg = f"value '{value}' does not conform to typerule "
            msg += f"'{template_typerule}'"
            raise TypeError(msg)

        self._metadata[key] = value
        self._keys_edited[key] = True

    def edit(self, key, value):
        if key not in self._metadata.keys():
            raise KeyError(f"Template did not define a key '{key}'")

        template_typerule = self.template.get_typerule(key)
        if conforms_to_typerule(value, template_typerule) is False:
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

    def reset(self, key):
        self._metadata[key] = self.template.get_default(key)


def init_database(rootdir, dbdir_name='database'):
    dbdir_path = os.path.join(rootdir, dbdir_name)
    os.mkdir(dbdir_path)

    # Generate a new Meta object
    # Meta


def parse_typerule(value, typerule):
    typerule = typerule.replace(' ', '').lower()

    if typerule.find('[') == -1:  # Did NOT find another left bracket
        value = typeconverts(typerule)(value)
    else:
        data_struct, typerule = typerule.split("[", 1)
        typerule = typerule[:-1]  # Remove right bracket (assoc. above)

        if data_struct in ['set', 'tuple', 'list']:

            value = [parse_typerule(i, typerule) for i in value]
            value = typeconverts(data_struct)(value)

        elif data_struct == 'dict':

            keyspec, valuespec = typerule.split(",", 1)
            key_converter = typeconverts(keyspec)

            if valuespec.find('[') == -1:
                value = {key_converter(k): typeconverts(valuespec)(v)
                         for k, v in value.items()}
            else:
                value = {key_converter(k): parse_typerule(v, valuespec)
                         for k, v in value.items()}

        else:
            raise TypeError(f"'{data_struct}' not supported")

    return value


def conforms_to_typerule(value, typerule):
    typerule = typerule.replace(' ', '').lower()

    def check_if_value_conforms(value, typerule):
        if typerule.find('[') == -1:
            is_of_type = isinstance(value, typeconverts(typerule))
        else:
            data_struct, typerule = typerule.split("[", 1)
            typerule = typerule[:-1]  # Remove right bracket (assoc. above)

            is_of_type = isinstance(value, typeconverts(data_struct))

            if data_struct in ['set', 'tuple', 'list']:

                elem_of_type = [conforms_to_typerule(i, typerule)
                                for i in value]
                is_of_type = is_of_type and all(elem_of_type)

            elif data_struct == 'dict':

                keyspec, valuespec = typerule.split(",", 1)

                key_of_type = [conforms_to_typerule(i, keyspec)
                               for i in value]
                value_of_type = [conforms_to_typerule(i, valuespec)
                                 for i in value.values()]

                is_of_type = (is_of_type and all(key_of_type) and
                              all(value_of_type))

        return is_of_type

    try:
        output = check_if_value_conforms(value=value, typerule=typerule)
    except (KeyError, TypeError, AttributeError):
        output = False

    return output


def isostr2dt(isostring, hastime=True, hasmicro=True):
    iso_format = "%Y-%m-%d"

    if isostring.endswith('Z'):
        isostring = isostring.replace('Z', '')

    if hastime is True:
        iso_format += "T%H:%M:%S"

    if hasmicro is True:
        iso_format += ".%f"
    else:
        isostring = isostring.partition(".")[0]

    output = datetime.datetime.strptime(isostring, iso_format)

    return output


def typeconverts(typespec):
    typers = {
        "str": str,
        "float": float,
        "int": int,
        'bool': bool,
        "ISO8601|DATETSZ": functools.partial(isostr2dt, hastime=True,
                                             hasmicro=False),
        "ISO8601|DATETMZ": functools.partial(isostr2dt, hastime=True,
                                             hasmicro=False),
        "ISO8601|DATE___": functools.partial(isostr2dt, hastime=False,
                                             hasmicro=False),
        "tuple": tuple,
        "list": list,
        "set": set,
        'dict': dict,
    }

    return typers[typespec]
