import functools
import inspect
import pandas
from progutils import progutils


def typecheck(**kwtypes):
    # Does not work with callable/functions unless they're a class itself
    # Check to make sure all kwtypes values are actually types
    for pname in kwtypes.keys():
        types = kwtypes[pname]

        if isinstance(types, type):
            continue
        elif isinstance(types, tuple):
            for a_type in types:
                if isinstance(a_type, type) is False:
                    msg = "Parameter '{0}' typecheck value '{1}' is not a type"
                    msg = msg.format(pname, a_type)
                    raise TypeError(msg)
        else:
            msg = "Only 'type' or 'tuple[type]' are allowed in 'typecheck'"
            raise TypeError(msg)

    def check_instance(func):

        funcparams = inspect.signature(func)
        funcparams = [i.name for i in funcparams.parameters.values()]
        funcparams_np = enumerate(funcparams)
        funcparams_np = {param_name: i for i, param_name in funcparams_np}

        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            for param_name, expected_types in kwtypes.items():
                # breakpoint()
                # NOTE: This only captures user-provided arguments. If the
                # parameter has a default and the user doesn't override,
                # It will not show up
                param_was_provided = False
                try:

                    if param_name in kwargs:
                        value = kwargs[param_name]
                    else:
                        value = args[funcparams_np[param_name]]

                    param_was_provided = True

                except IndexError:
                    value = None

                if param_was_provided is True:
                    if isinstance(value, expected_types) is False:

                        if progutils.is_tuple_or_list(expected_types) is False:
                            ex_types = [expected_types]
                        else:
                            ex_types = expected_types

                        type_names = ['None' if el is None else el.__name__
                                      for el in ex_types]

                        if type_names:
                            type_names = ["'" + i + "'" for i in type_names]

                            if len(type_names) > 1:
                                type_names = (", ".join(type_names[:-1]) +
                                              ", or " + type_names[-1])
                            else:
                                # Should be just one type name
                                type_names = ", ".join(type_names)

                            msg = "'{0}' is not of type {1}"
                            msg = msg.format(param_name, type_names)
                        else:
                            msg = "'{0}' is note the correct type"
                            msg = msg.format(param_name)

                        raise TypeError(msg)
                else:
                    continue

            return func(*args, **kwargs)

        return func_wrapper

    return check_instance


def parse_typerule(value, typerule):
    typerule = typerule.replace(' ', '').lower()

    if typerule.find('[') == -1:  # Did NOT find another left bracket

        if typerule.find('|') >= 0:
            typerules = typerule.split('|')
            if typerule.find('~') >= 0:
                # Has OR operator AND has indicator of primary typerule
                typerule = [i for i in typerules if i.find('~') >= 0][0]
                typerule = typerule.replace('~', '')
            else:
                typerule = [i for i in typerules][0]

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

                if valuespec.find('|') >= 0:
                    valuespecs = valuespec.split('|')
                    if valuespec.find('~') >= 0:
                        # Has OR operator AND has indicator of primary typerule
                        valuespec = [i for i in valuespecs
                                     if i.find('~') >= 0][0]
                        valuespec = valuespec.replace('~', '')
                    else:
                        valuespec = [i for i in valuespecs][0]

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
        # breakpoint()
        if typerule.find('[') == -1:
            if typerule.find('|') == -1:
                typerules = [typerule]
            else:
                typerules = typerule.replace('~', '').split('|')

            is_of_types = list()
            for a_typerule in typerules:

                if a_typerule == 'date':
                    is_of_types.append(
                        progutils.is_datetime_valid(
                            value, hastime=False, hasmicro=False))
                elif a_typerule == 'datetime_second':
                    is_of_types.append(
                        progutils.is_datetime_valid(
                            value, hastime=True, hasmicro=False))
                elif a_typerule == 'datetime_microsecond':
                    is_of_types.append(
                        progutils.is_datetime_valid(
                            value, hastime=True, hasmicro=True))
                else:
                    is_of_types.append(
                        isinstance(value, typeconverts(a_typerule)))

            is_of_type = any(is_of_types)

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


def typeconverts(typespec):
    typers = {
        "str": str,
        "float": float,
        "int": int,
        'bool': bool,
        "datetime_second": functools.partial(progutils.isostr2dt, hastime=True,
                                             hasmicro=False),
        "datetime_microsecond": functools.partial(progutils.isostr2dt,
                                                  hastime=True,
                                                  hasmicro=True),
        "date": functools.partial(progutils.isostr2dt, hastime=False,
                                  hasmicro=False),
        "tuple": tuple,
        "list": list,
        "set": set,
        'dict': dict,
        'None': lambda x: type(None)
    }

    return typers[typespec]


# Type check decorators
def dec_does_series_name_exist(func):
    def check_if_series_exist(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError:
            raise KeyError(f"'{kwargs['series_name']}' does not exist")

    return check_if_series_exist


def typecheck_datetime_like(param_name):
    index = (pandas.DatetimeIndex, pandas.PeriodIndex, pandas.TimedeltaIndex)
    index = {param_name: index}

    return typecheck(**index)