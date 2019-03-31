import cProfile
import functools
import logging

from utils.debug import pstats2png


def profile(f):
    def decorator(f):
        @functools.wraps(f)
        async def wrapper(*args, **kwargs):
            prof = cProfile.Profile()
            prof.enable()
            retval = await f(*args, **kwargs)
            prof.disable()
            prof.dump_stats(fname + '.profile')
            try:
                pstats2png(fname + '.profile')
            except:
                logging.exception('failed to create profile png:')
            return retval
        return wrapper

    if isinstance(f, str):
        fname = f
        return decorator
    else:
        fname = '.'.join((f.__module__, f.__name__))
        return decorator(f)
