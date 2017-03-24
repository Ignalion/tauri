import collections
import functools
import itertools
from functools import reduce


# merge primitives:

class wrapper(object):
    def __init__(self, obj):
        self.obj = obj


class replace(wrapper):
    """Replace content (no recursive merge)"""


class consume(list):
    """Right list will extend this one without explicit extend() wrap"""


class extend(list):
    """Extend existent list (do not replace)"""


class extendleft(list):
    """Extend existent list, but place new content before original"""


class superset(set):
    """Will include all upcoming sets at the same path"""


class unite(set):
    """Make a union with the set leftward"""


class complement(set):
    """Make a complement of the set leftward"""


class MISSED(object):
    """Internal token to indicate missed key"""


class DEFAULT(object):
    """Do not change anything, use original value"""


def merge(*items, **kwargs):

    mergers = kwargs.get('mergers', {})
    strict = kwargs.get('strict', False)

    def _merge_dicts(d1, d2, path):
        return type(d1)(itertools.chain(
            iter(d1.items()),
            ((k, _update_value(d1.get(k, MISSED), v, path + [k]))
             for k, v in d2.items())))

    def _update_value(old, new, path):

        def error(msg):
            msg = msg.format(old=old, path='.'.join(map(str, path)), new=new)
            return ValueError(msg)

        merger = mergers.get(tuple(path))
        if merger:
            new = merger(old, new)

        if new is DEFAULT:
            if old is MISSED:
                return None
            return old

        elif strict and old is MISSED:
            raise KeyError('strict mode: can\'t set %s' % path)

        elif isinstance(new, replace):
            return new.obj

        elif isinstance(new, (extend, extendleft)) or isinstance(old, consume):
            if old is MISSED:
                return type(new)(new)
            elif not isinstance(old, list) or not isinstance(new, list):
                raise error('Unable to merge different types {old} and {new}'
                            ' at {path}' )
            elif isinstance(new, extendleft):
                return type(old)(new + old)
            else:
                return type(old)(old + new)

        elif isinstance(new, (unite, complement)) or isinstance(old, superset):
            if old is MISSED:
                return type(new)(new)
            elif not isinstance(old, set) or not isinstance(new, set):
                raise error('Unable to merge different types {old} and {new}'
                            ' at {path}' )
            elif isinstance(new, complement):
                return old - new
            else:
                return old | new

        elif old is MISSED:
            return new

        elif isinstance(new, dict):
            if not isinstance(old, dict):
                if strict:
                    raise error('Unable to merge non-dict object {old}'
                                ' at {path} and {new}')
                else:
                    return new
            return _merge_dicts(old, new, path)

        else:
            return new

    return reduce(functools.partial(_update_value, path=[]), items)


def walk(coll, predicate=None, ext_getters=None):

    getters = {
        tuple: lambda x: enumerate(x),
        list: lambda x: enumerate(x),
        collections.Mapping: lambda x: iter(x.items())
    }
    if ext_getters:
        getters.update(ext_getters)

    seen = set()

    def _get_item_getter(obj):
        for getter_type in getters:
            if isinstance(obj, getter_type):
                return getters[getter_type]

    def _walk(obj, path):
        getter = _get_item_getter(obj)
        if getter:
            seen.add(id(obj))
            for k, v in getter(obj):
                if not predicate or predicate(v):
                    yield path + (k,), v
                if id(v) not in seen:
                    for result in _walk(v, path + (k,)):
                        yield result
            seen.remove(id(obj))

    return ((path, value) for path, value in _walk(coll, ()))


def is_namedtuple(obj):
    return isinstance(obj, tuple) and hasattr(obj, '_fields')


def get_child(d, k):
    if isinstance(d, collections.Mapping):
        return d[k]
    elif isinstance(d, collections.Sequence) and not is_namedtuple(d):
        return d[int(k)]
    else:
        return getattr(d, k)


def get_path(coll, path, **kwargs):
    try:
        return reduce(get_child, path, coll)
    except (IndexError, KeyError, AttributeError) as e:
        if 'default' not in kwargs:
            raise e.__class__('path not fund: {}'.format(path))
        else:
            return kwargs['default']


def del_path(coll, path):
    del get_path(coll, path[:-1])[path[-1]]


def set_path(coll, path, value):
    target = get_path(coll, path[:-1])
    index = path[-1]
    try:
        target[index] = value
    except TypeError:
        if type(target) is tuple and len(path) > 1:
            set_path(coll, path[:-1], target[:index] + (value,) + target[index + 1:])
        else:
            raise


def update(coll, item, f):
    if isinstance(coll, (list, tuple)):
        return type(coll)(itertools.chain(coll[:item], (f(coll[item]),), coll[item+1:]))
    else:
        return type(coll)((k, f(v) if k == item else v) for k, v in coll.items())


def update_path(coll, path, f):
    if len(path) > 1:
        f = lambda x, p=path[1:], f=f: update_path(x, p, f)
    return update(coll, path[0], f)


def replace_item(coll, old_key, new_key, f):
    return type(coll)(
        ((new_key, f(v)) if k == old_key else (k, v))
        for k, v in coll.items())


def map_dict(f, d):
    return {k: f(v) for k, v in d.items()}


def split_dict(key_list_or_func, original):
    selected, passed = {}, {}
    key = key_list_or_func if callable(key_list_or_func) \
        else lambda x: x in key_list_or_func
    for k, v in original.items():
        (selected if key(k) else passed)[k] = v
    return selected, passed


def filter_dict(key_list_or_func, original):
    return split_dict(key_list_or_func, original)[0]


def nest(path, cls, *args, **kwargs):
    if not path:
        return cls(*args, **kwargs)
    return cls([(path[0], nest(path[1:], cls, *args, **kwargs))])


def swap(d):
    return {v: k for k, v in d.items()
            if isinstance(v, collections.Hashable)}


def flatten(xs):
    return list(itertools.chain.from_iterable(xs))


def byteify(obj):
    if isinstance(obj, dict):
        return {byteify(key): byteify(value)
                for key, value in obj.items()}
    elif isinstance(obj, list):
        return [byteify(element) for element in obj]
    elif isinstance(obj, str):
        return obj.encode('utf-8')
    else:
        return obj
