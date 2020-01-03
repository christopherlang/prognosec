import functools
import datetime
import inspect
import typing
import pandas
import numpy
from progutils import progutils


# Constants and predefinitions
_ISO8601_CONVERT = progutils.isostr2dt
_D_CONV = functools.partial(_ISO8601_CONVERT, hastime=False, hasmicro=False)
_DT_CONV = functools.partial(_ISO8601_CONVERT, hastime=True, hasmicro=False)
_DTS_CONV = functools.partial(_ISO8601_CONVERT, hastime=True, hasmicro=False)
_DTMS_CONV = functools.partial(_ISO8601_CONVERT, hastime=True, hasmicro=True)


def returner(x):
    return


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


def apply_typerule(value, typerule):

    typerule = typerule.replace(' ', '').lower()

    if typerule.count("|") > 0:
        raise ValueError('typerule conversion does not support OR operator')

    def typerule_converter(value, typerule):
        if typerule.count("[") != typerule.count("]"):
            raise ValueError("Invalid typerule specification")

        if typerule.find("[") >= 0:
            seqtype, elemtype = typerule.split("[", 1)
            elemtype = elemtype.rsplit("]", 1)[0]  # Remove right bracket

            if seqtype == 'dict':

                dict_keytype, dict_valuetype = elemtype.split(',', 1)

                result = dict()
                for dict_key in value:
                    new_key = typerule_converter(dict_key, dict_keytype)
                    new_value = typerule_converter(value[dict_key],
                                                   dict_valuetype)
                    result[new_key] = new_value

            else:
                seq_converter = types_converters(seqtype)

                result = [typerule_converter(i, elemtype) for i in value]
                result = seq_converter(result)

        else:

            result = types_converters(typerule)(value)

        return result

    return typerule_converter(value=value, typerule=typerule)


def conforms_typerule(value, typerule):

    typerule = typerule.replace(' ', '').replace('~', '').lower()

    def conformance(value, typerule):

        if typerule.count("[") != typerule.count("]"):
            raise ValueError("Invalid typerule specification")

        output = list()

        if typerule.find("[") >= 0 and typerule.find("]|") == -1:

            seqtype, elemtype = typerule.split("[", 1)
            elemtype = elemtype.rsplit("]", 1)[0]  # Remove right bracket

            if seqtype == 'dict':

                dict_keys = set(value.keys())
                dict_values = list(value.values())
                dict_keytype, dict_valuetype = elemtype.split(',', 1)

                keys_isof = [conformance(i, dict_keytype) for i in dict_keys]
                values_isof = [conformance(i, dict_valuetype)
                               for i in dict_values]

                output.extend(keys_isof)
                output.extend(values_isof)

            else:

                if isinstance(value, types_str2object(seqtype)) is True:
                    output.append(True)  # For the sequence itself
                    elem_isof_type = [conformance(i, elemtype)
                                      for i in value]
                    output.extend(elem_isof_type)  # For each elem in sequence
                else:
                    output.append(False)

        elif typerule.find("]|") >= 0:

            sub_typerules = _seq_or_split(typerule)

            sub_isof = [conformance(value, i) for i in sub_typerules]
            output.append(any(sub_isof))

        elif typerule.find("|") >= 0:

            typerules = typerule.split("|")

            isof_typerules = [conformance(value, i) for i in typerules]
            isof_typerules = any(isof_typerules)
            output.append(isof_typerules)

        else:

            if typerule == 'primitive':

                primitive_types = [int, float, bool, str]
                isof_primitive = [isinstance(value, i)
                                  for i in primitive_types]
                isof_primitive.append(value is None)

                isof_primitive = any(isof_primitive)

                output.append(isof_primitive)

            else:

                valtype = types_str2object(typerule)

                if valtype is None:
                    output.append(value is None)
                else:
                    output.append(isinstance(value, valtype))

        return all(output)

    return conformance(value=value, typerule=typerule)


def _seq_or_split(typerule):

    output = list()

    try:
        or_split = typerule.index(']|')
    except ValueError:
        return [typerule]

    left_rule = typerule[:or_split]
    right_rule = typerule[(or_split + 2):]

    # Fix left rule
    if left_rule.count('[') != left_rule.count(']'):
        nleft = left_rule.count("[") - left_rule.count("]")
        nleft = "".join(["]" for _ in range(nleft)])
        left_rule += nleft

    output.append(left_rule)

    # fix right rule
    if right_rule.count('[') < right_rule.count(']'):
        # missing a sequence, or some type at left rule
        missing_left = right_rule.count(']') - right_rule.count('[')

        for i in range(missing_left):
            sub_left_rule = left_rule[:left_rule.index('[')]
            right_rule = sub_left_rule + "[" + right_rule

            # Fix left rule for looping
            index_from = left_rule.index('[') + 1
            index_to = left_rule.rindex(']')
            left_rule = left_rule[index_from:index_to]

            right_rule = _seq_or_split(right_rule)
            output.extend(right_rule)

    else:
        output.append(right_rule)

    return output


def types_str2object(typerule):

    typerules = {
        'int': int,
        'str': str,
        'bool': bool,
        'float': float,
        'set': set,
        'list': list,
        'tuple': tuple,
        'dict': dict,
        'ndarray': numpy.ndarray,
        'date': datetime.date,
        'datetime': datetime.datetime,
        'datetime_second': datetime.datetime,
        'datetime_microsecond': datetime.datetime,
        'time': datetime.time,
        'none': None,
        'null': None,
        'function': typing.Callable,
        'callable': typing.Callable
    }

    try:
        output = typerules[typerule]
    except KeyError:
        raise ValueError('Invalid typerule specification')

    return output


def types_converters(typerule):

    typerules = {
        'int': int,
        'str': str,
        'bool': bool,
        'float': float,
        'set': set,
        'list': list,
        'tuple': tuple,
        'dict': dict,
        'ndarray': numpy.array,
        'date': _D_CONV,
        'datetime': _DT_CONV,
        'datetime_second': _DTS_CONV,
        'datetime_microsecond': _DTMS_CONV,
        'time': datetime.time,
        'none': returner,
        'null': returner
    }

    try:
        output = typerules[typerule]
    except KeyError:
        raise ValueError('Invalid typerule specification')

    return output


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
