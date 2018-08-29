import logging
from collections import OrderedDict
from functools import partial
from itertools import chain

from .funcs import repr_function
from ..util.types import OpFunctor, term

# -- core: ---------------------------------------------------------------------

EMPTY = term('EMPTY')
EVALUATING = term('EVALUATING')


class Lazy(OpFunctor):

    __slots__ = ['f', 'args', 'kwargs', 'cache', '__apply__']

    def __init__(self, f, *args, **kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.cache = EMPTY
        self.__apply__ = lambda f, *args: lazy(f)(*args)

    def __iter__(self):
        if self.cache is EMPTY:
            self.cache = EVALUATING
            try:
                for f, args, kwargs in eval((self.f, self.args, self.kwargs)):
                    for maybe_lazy in f(*args, **kwargs):
                        for result in eval(maybe_lazy):
                            self.cache = result
                            yield result
            except Exception:
                # this will give us kind of meaningful traceback
                logging.error('error resolving %s', self)
                raise
            finally:
                self.cache = EMPTY
        elif self.cache is EVALUATING:
            raise ReferenceError('circular reference at: {}'.format(self))
        else:
            yield self.cache

    def __call__(self, *args, **kwargs):
        return Lazy(iapply, self, *args, **kwargs)

    def __repr__(self):
        def _add_label(repr_, label='lazy:'):
            return label + repr_ if not repr_.startswith(label) else repr_

        f, args = self.f, self.args

        if f is singleton:
            return _add_label(repr(args[0]))
        if f is iapply:
            f, args = args[0], args[1:]
        if isinstance(f, Lazy):
            if f.f is singleton:
                f = f.args[0]
            else:
                f = repr(f)

        return _add_label(repr_function(f, *args, **self.kwargs))


def eval(item):
    if isinstance(item, Lazy):
        for result in item:
            yield result
    elif isinstance(item, (list, tuple)):
        for elements in iproduct(*list(map(_evaluator, item))):
            if hasattr(item, '_fields'):  # namedtuple, need to unpack values
                yield type(item)(*elements)
            else:
                yield type(item)(elements)
    elif isinstance(item, dict):
        for keys in iproduct(*list(map(_evaluator, list(item.keys())))):
            for values in iproduct(*list(map(_evaluator, list(item.values())))):
                yield type(item)(list(zip(keys, values)))
    else:
        yield item


def _evaluator(x):
    return lambda: eval(x)


def iapply(f, *args, **kwargs):
    yield f(*args, **kwargs)


def iproduct(*iter_getters):
    def _iproduct(iter_getters, stack=()):
        if not iter_getters:
            yield stack
        else:
            for item in iter_getters[0]():
                for x in _iproduct(iter_getters[1:], stack + (item,)):
                    yield x
    return _iproduct(iter_getters)


def lazy_generator(f):
    return partial(Lazy, f)


def singleton(x):
    yield x


lazy = lazy_generator(singleton)
foreach = lazy_generator(chain)


def copy(obj):
    if not isinstance(obj, Lazy):
        return obj
    return Lazy(copy(obj.f),
                *list(map(copy, obj.args)),
                **dict(list(zip(list(map(copy, list(obj.kwargs.keys()))),
                           list(map(copy, list(obj.kwargs.values())))))))


# -- aliases: ------------------------------------------------------------------

def get(l):
    results = list(eval(l))
    assert results, 'empty lazy object'
    assert len(results) < 2, 'lazy object has more than one state'
    return results[0]


class DictProxy(object):
    def __init__(self, d):
        self.d = d

    def __getitem__(self, item):
        x = self.d[item]
        if isinstance(x, type(self.d)):
            x = DictProxy(x)
        return x

    def __getattr__(self, item):
        return getattr(self.d, item)

    def __repr__(self):
        return '{...}'


def lazy_dict(*a, **k):
    new = dict(*a, **k)
    return lazy(DictProxy(new)), new


def lazy_ordered_dict(*a, **k):
    new = OrderedDict(*a, **k)
    return lazy(DictProxy(new)), new
