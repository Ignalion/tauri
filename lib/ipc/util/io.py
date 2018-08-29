import json
import io
from functools import wraps


def with_file(filepath, create=None, ro=False, serializer=json):

    def decorator(f):

        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                with open(filepath, 'rb') as file_:
                    data = serializer.load(file_)
            except IOError:
                if create:
                    data = create()
                else:
                    raise
            result = f(data, *args, **kwargs)
            if not ro:
                with open(filepath, 'wb') as file_:
                    serializer.dump(data, file_, indent=2)
            return result

        return wrapper

    return decorator


def streamify(s):
    stream = io.StringIO()
    stream.write(s)
    stream.seek(0)
    return stream
