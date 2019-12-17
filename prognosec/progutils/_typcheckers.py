import functools
import inspect


def typecheck(**kwtypes):
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

                # NOTE: This only captures user-provided arguments. If the
                # parameter has a default and the user doesn't override,
                # It will not show up
                param_was_provided = False
                try:
                    if param_name in kwargs:
                        value = kwargs[param_name]
                    else:
                        value = args[funcparams_np[param_name]]
                    print('hello')
                    param_was_provided = True
                except IndexError:
                    value = None

                if param_was_provided is True:
                    if isinstance(value, expected_types) is not True:
                        type_names = list()

                        for expected_type in expected_types:
                            if expected_type is None:
                                type_names.append('None')
                            elif isinstance(expected_type, type):
                                type_names.append(expected_type.__name__)

                        if type_names:
                            type_names = ["'" + i + "'" for i in type_names]
                            type_names = (", ".join(type_names[:-1]) +
                                          ", or " + type_names[-1])

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
