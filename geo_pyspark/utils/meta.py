import inspect
import types

from geo_pyspark.exceptions import InvalidParametersException


class MultiMethod:
    """
    Represents a single multimethod.
    """

    def __init__(self, name):
        self._methods = {}
        self.__name__ = name
        self._is_static = False

    def register(self, meth):
        """
        Register a new method as a multimethod
        :param meth:
        :return:
        """
        if str(meth).startswith("<classmethod") or str(meth).startswith("<staticmethod"):
            sig = inspect.signature(meth.__get__(self))
            self._is_static = True
        else:
            sig = inspect.signature(meth)

        # Build a type-signature from the method's annotations
        types = []
        for name, parm in sig.parameters.items():
            if name == 'self':
                continue
            if name == "args" or name == "kwargs":
                raise InvalidParametersException("Can not be used with args and kwargs")

            if name == 'cls':
                continue

            if parm.annotation is inspect.Parameter.empty:
                raise InvalidParametersException(
                    'Argument {} must be annotated with a type'.format(name)
                )
            if not isinstance(parm.annotation, type):
                raise InvalidParametersException(
                    'Argument {} annotation must be a type'.format(name)
                )
            if parm.default is not inspect.Parameter.empty:
                self._methods[tuple(types)] = meth
            types.append((name, parm.annotation))

        self._methods[tuple(types)] = meth

    def __call__(self, *args, **kwargs):
        """
        Call a method based on type signature of the arguments
        :param args:
        :param kwargs:
        :return:
        """
        if self._is_static:
            types_from_args = tuple(type(arg) for arg in args)
        else:
            types_from_args = tuple(type(arg) for arg in args[1:])

        number_of_arguments = len(types_from_args)
        methods_shortened_to_args = [
            [tuple(tp[1] for tp in types[:number_of_arguments]), types, method]
            for types, method in self._methods.items()
        ]

        methods_wich_are_correct = [
            el[1:] for el in methods_shortened_to_args if types_from_args == el[0]
        ]
        if methods_wich_are_correct:
            for correct_params, method in methods_wich_are_correct:
                if len(correct_params[number_of_arguments:]) != kwargs.__len__():
                    continue
                else:
                    for name, param in correct_params[number_of_arguments:]:
                        try:
                            value = kwargs[name]
                        except KeyError:
                            break
                        if type(value) == param:
                            pass
                        else:
                            break
                    else:
                        if self._is_static:
                            return method.__get__(self).__call__(*args, **kwargs)
                        return method(*args, **kwargs)

            raise InvalidParametersException("No matching method for given types found")

        else:
            raise InvalidParametersException("No matching method for given types found")

    def __get__(self, instance, cls):
        """
        Descriptor method needed to make calls work in a class
        :param instance:
        :param cls:
        :return:
        """
        if instance is not None:
            return types.MethodType(self, instance)
        else:
            return self


class MultiDict(dict):
    """
    Special dictionary to build multimethods in a metaclass
    """
    def __setitem__(self, key, value):
        if key in self:
            # If key already exists, it must be a multimethod or callable
            current_value = self[key]
            if isinstance(current_value, MultiMethod):
                current_value.register(value)
            else:
                mvalue = MultiMethod(key)
                mvalue.register(current_value)
                mvalue.register(value)
                super().__setitem__(key, mvalue)
        else:
            super().__setitem__(key, value)


class MultipleMeta(type):
    """
    Metaclass that allows multiple dispatch of methods
    """

    def __new__(cls, clsname, bases, clsdict):
        return type.__new__(cls, clsname, bases, dict(clsdict))

    @classmethod
    def __prepare__(cls, clsname, bases):
        return MultiDict()
