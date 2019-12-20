# import json
import datetime
import functools
import re
# import collections
# import os
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


def parse_typespec(value, typespec):
    typespec = typespec.replace(' ', '').lower()

    if typespec.find('[') == -1:  # Did NOT find another left bracket
        value = typeconverts(typespec)(value)
    else:
        data_struct, typespec = typespec.split("[", 1)
        typespec = typespec[:-1]  # Remove right bracket (assoc. above)

        if data_struct in ['set', 'tuple', 'list']:

            value = [parse_typespec(i, typespec) for i in value]
            value = typeconverts(data_struct)(value)

        elif data_struct == 'dict':

            keyspec, valuespec = typespec.split(",", 1)
            key_converter = typeconverts(keyspec)

            if valuespec.find('[') == -1:
                value = {key_converter(k): typeconverts(valuespec)(v)
                         for k, v in value.items()}
            else:
                value = {key_converter(k): parse_typespec(v, valuespec)
                         for k, v in value.items()}

        else:
            raise TypeError(f"'{data_struct}' not supported")

    return value


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
