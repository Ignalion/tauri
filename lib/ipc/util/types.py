import inspect
import operator
from .colls import filter_dict


def term(name):
    return type(name, (), {})()


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OpFunctor(object):

    def __getattr__(self, item):
        return self.__apply__(getattr, self, item)

    def __getitem__(self, other):
        return self.__apply__(operator.getitem, self, other)

    def __contains__(self, other):
        return self.__apply__(operator.contains, self, other)

    def __lt__(self, other):
        return self.__apply__(operator.lt, self, other)

    def __le__(self, other):
        return self.__apply__(operator.le, self, other)

    def __eq__(self, other):
        return self.__apply__(operator.eq, self, other)

    def __ne__(self, other):
        return self.__apply__(operator.ne, self, other)

    def __gt__(self, other):
        return self.__apply__(operator.gt, self, other)

    def __ge__(self, other):
        return self.__apply__(operator.ge, self, other)

    def __not__(self):
        return self.__apply__(operator.not_, self)

    def __add__(self, item):
        return self.__apply__(operator.add, self, item)

    def __and__(self, other):
        return self.__apply__(operator.and_, self, other)

    def __div__(self, other):
        return self.__apply__(operator.truediv, self, other)

    def __mul__(self, other):
        return self.__apply__(operator.mul, self, other)

    def __len__(self):
        return self.__apply__(len, self)

    def __neg__(self):
        return self.__apply__(operator.neg, self)

    def __or__(self, other):
        return self.__apply__(operator.or_, self, other)

    def __pos__(self):
        return self.__apply__(operator.pos, self)

    def __pow__(self, other):
        return self.__apply__(operator.pow, self, other)

    def __sub__(self, other):
        return self.__apply__(operator.sub, self, other)

    def __xor__(self, other):
        return self.__apply__(operator.xor, self, other)


def is_namespace(value):
    try:
        classname = value.__class__.__name__
    except:
        classname = None
    return classname == 'Namespace'


def namespace(*slots, **kwargs):  # TODO rename to struct
    class Namespace(object):
        __slots__ = set(slots) | set(kwargs.keys())

        def __init__(self, **kwargs):
            for k, v in list(kwargs.items()):
                setattr(self, k, v)

        def to_dict(self):
            def _to_dict(item):
                if isinstance(item, (list, tuple)):
                    return type(item)(list(map(_to_dict, item)))
                elif isinstance(item, dict):
                    return type(item)((k, _to_dict(v)) for k, v in list(item.items()))
                elif is_namespace(item):
                    return {k: _to_dict(getattr(item, k, None))
                            for k in item.__slots__}
                else:
                    return item

            return _to_dict(self)

        def __repr__(self):
            res = []
            for k in self.__slots__:
                res.append(str(k) + ': ' + repr(getattr(self, k, None)))
            return '{' + ', '.join(res) + '}'

    return Namespace(**kwargs)


def enum2dict(enum):
    return {k: v for k, v in enum.__dict__.items() if k.isupper()}


def multidict(seq):
    d = {}
    for k, v in seq:
        d.setdefault(k, []).append(v)
    return d


def dummy2Dict(dummy):
    addKeyAs = None

    if isinstance(dummy, list):
        dummy = dict(enumerate(dummy))
        addKeyAs = '_id'

    if dummy.__class__.__name__ == 'Dummy':
        dummy = dummy.__dict__

    if hasattr(dummy, 'items'):
        d = {k: dummy2Dict(v) for k, v in list(dummy.items())}
        for k, v in list(d.items()):
            if addKeyAs and isinstance(v, dict):
                v[addKeyAs] = k
        return d

    else:
        return dummy


class Namespace(object):
    """simple attribute container"""

    def __update__(self, new, keys=None):
        if isinstance(new, dict):
            update = new
        else:
            update = {name:value for name, value in inspect.getmembers(new)
                      if not name.startswith('__')}
        if keys:
            update = filter_dict(keys, update)
        self.__dict__.update(update)


class ReprHook(str):
    """Used essentially to allow passing multi-line strings as KeyError arg.
    (https://github.com/python/cpython/blob/2.7/Objects/exceptions.c#L1264)
    """
    def __repr__(self):
        return str(self)
