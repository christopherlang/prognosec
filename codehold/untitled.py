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
                        is_datetime_valid(
                            value, hastime=False, hasmicro=False))
                elif a_typerule == 'datetime_second':
                    is_of_types.append(
                        is_datetime_valid(
                            value, hastime=True, hasmicro=False))
                elif a_typerule == 'datetime_microsecond':
                    is_of_types.append(
                        is_datetime_valid(
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