import asyncio
import operator
from functools import partial

# from twisted.internet.defer import inlineCallbacks, returnValue, gatherResults
# from twisted.internet.defer import maybeDeferred

from .util.asyncs import blockingDeferredCall
from .util.funcs import repr_args
from .util.colls import map_dict
from .util.types import OpFunctor


class Operation(object):

    __slots__ = ['__f', '__args', '__kwargs']

    def __init__(self, f=None, *args, **kwargs):
        if not f: assert not (args or kwargs), 'got args for empty function'
        self.__f = f
        self.__args = args
        self.__kwargs = kwargs

    def __getstate__(self):
        return self.__f, self.__args, self.__kwargs

    def __setstate__(self, state):
        f, args, kwargs = state
        self.__init__(f, *args, **kwargs)

    def __eval__(self, root):
        if self.__f is None:
            return root
        get = lambda x: x.__eval__(root) if isinstance(x, self.__class__) else x
        return get(self.__f)(*list(map(get, self.__args)),
                             **map_dict(get, self.__kwargs))

    async def __update__(self, f):
        def upd(func):
            _f = partial(
                lambda x: x.__update__(f) if isinstance(x, self.__class__) else f(x),
                func)
            return asyncio.coroutine(_f)()
        f_ = await upd(self.__f)
        args = await asyncio.gather(*list(map(upd, self.__args)))
        kw_values = await asyncio.gather(*list(map(upd, list(self.__kwargs.values()))))
        kwargs = dict(list(zip(self.__kwargs, kw_values)))
        return self.__class__(f_, *args, **kwargs)

    def __repr__(self):
        try:
            if self.__f is None:
                return '{}:<...>'.format(self.__class__.__name__)
            elif self.__f is getattr:
                return '{}.{}'.format(*self.__args[:2])
            elif self.__f is setattr:
                return '{}.{} = {}'.format(*self.__args[:3])
            elif self.__f is operator.getitem:
                return '{}[{}]'.format(*list(map(repr, self.__args[:2])))
            elif self.__f is operator.setitem:
                return '{}[{}] = {}'.format(*list(map(repr, self.__args[:3])))
            else:
                return '{}({})'.format(
                    getattr(self.__f, '__name__', repr(self.__f)),
                    repr_args(*self.__args, **self.__kwargs))
        except:
            return object.__repr__(self)


class Proxy(OpFunctor):

    __slots__ = ['__target__', '__op__', '__apply__']

    def __init__(self, target=None, operation=None):
        self.__target__ = target or (lambda x: x)
        self.__op__ = operation or Operation()
        self.__apply__ = apply

    def __wrap__(self, *args, **kwargs):
        return self.__class__(self.__target__, Operation(*args, **kwargs))

    async def __send__(self, **kwargs):
        op = await self.__op__.__update__(partial(_un_proxy, self))
        cor = asyncio.coroutine(self.__target__)(op, **kwargs)
        return await cor

    def __invert__(self):
        return self.__send__()

    def __call__(self, *args, **kwargs):
        return self.__wrap__(self.__op__, *args, **kwargs)

    def __setattr__(self, key, value):
        if key in self.__slots__:
            object.__setattr__(self, key, value)
        else:
            apply(setattr, self, key, value).__send__()

    def __setitem__(self, key, value):
        apply(operator.setitem, self, key, value).__send__()


class BlockingProxy(Proxy):
    __slots__ = Proxy.__slots__

    __send__ = blockingDeferredCall(Proxy.__send__)


def apply(f, proxy, *args, **kwargs):
    return proxy.__wrap__(f, proxy.__op__, *args, **kwargs)


def _un_proxy(sender, x):
    if isinstance(x, sender.__class__):
        if x.__target__ is sender.__target__:
            return x.__op__.__update__(partial(_un_proxy, sender))
        else:
            return x.__send__()
    else:
        return x


if __name__ == '__main__':
    import pickle
    s = Proxy()
    chain = s.strip().split('.')[0]
    chain = pickle.loads(pickle.dumps(~chain))
    print(chain)
    print((chain << 'hello.world'))
