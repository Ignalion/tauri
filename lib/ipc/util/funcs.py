import functools
import itertools
import logging
import random
import operator
import time
import traceback
from functools import reduce


def compose(*fs):
    pair = lambda f, g: lambda *a, **kw: f(g(*a, **kw))
    return reduce(pair, fs)


def rpartial(func, *args):
    return lambda *a: func(*(a + args))


def select(iterable, getter, limit=None, **kwarg):
    assert len(kwarg) == 1, 'Need exactly one kwarg to select'
    (name, value), = list(kwarg.items())
    check = value if callable(value) else functools.partial(operator.eq, value)
    selected = filter(lambda x: check(getter(x, name)), iterable)
    return itertools.islice(selected, limit)


select_objects = functools.partial(select, getter=getattr)
select_dicts = functools.partial(select, getter=operator.getitem)


def filter_all(filters, iterable):
    for f in filters:
        iterable = filter(f, iterable)
    return iterable


# FIXME Make include have higher priority than exclude
def filter_patterns(iterable, matcher, include=None, exclude=None):
    match = lambda patterns, name: any(matcher(pat, name) for pat in patterns)
    return filter_all((lambda item: not include or match(include, item),
                       lambda item: not exclude or not match(exclude, item)),
                      iterable)


def safeRandomSample(population, k):
    return random.sample(population, k) if len(population) >= k else list(population)


def repr_args(*args, **kwargs):
    return ', '.join(
        list(map(repr, args)) +
        ['='.join((str(k), repr(v))) for k, v in list(kwargs.items())]
    )


def repr_function(func, *args, **kwargs):
    if func is getattr:
        return '{}.{}'.format(repr(args[0]), args[1])
    elif func is setattr:
        return '{}.{} = {}'.format(repr(args[0]), args[1], repr(args[2]))
    elif func is operator.getitem:
        return '{}[{}]'.format(*list(map(repr, args[:2])))
    elif func is operator.setitem:
        return '{}[{}] = {}'.format(*list(map(repr, args[:3])))
    elif isinstance(func, functools.partial):
        # unpack partial objects
        args = func.args + tuple(args)
        kwargs = dict(list((func.keywords or {}).items()) + list(kwargs.items()))
        return repr_function(func.func, *args, **kwargs)

    elif isinstance(func, str):
        func_repr = func
    else:
        func_repr = getattr(func, '__name__', repr(func))
        if hasattr(func, 'im_class'):
            func_repr = '.'.join((getattr(func.__self__.__class__, '__name__', ''),
                                  func_repr))
    return '{}({})'.format(func_repr, repr_args(*args, **kwargs))


def timed(f):
    @functools.wraps(f)
    def wrap(*a, **kw):
        t1 = time.time()
        res = f(*a, **kw)
        logging.debug(
            '%s call took %f s.', repr_function(f, *a, **kw), time.time() - t1)
        return res
    return wrap


def log_exc(logger_or_f=None):

    def decorator(f):
        @functools.wraps(f)
        def wrap(*args, **kwargs):
            try:
                f(*args, **kwargs)
            except:
                logger.error('Traceback in %s:\n%s',
                             repr_function(f, *args, **kwargs),
                             traceback.format_exc())
                raise
        return wrap

    if isinstance(logger_or_f, logging.Logger):
        logger = logger_or_f
        return decorator
    else:
        logger = logging.getLogger()
        return decorator(logger_or_f)
