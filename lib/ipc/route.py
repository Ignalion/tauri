import os
import socket
import getpass

ROUTE_DELIMITER = ':'


def get_route(component, proc_id=None, token=None):

    if proc_id is None:
        proc_id = str(os.getpid())

    if token is None:
        short_host = socket.gethostname().split('.')[0]
        try:
            import pwd
            username = pwd.getpwuid(os.getuid()).pw_name
        except ImportError:
            username = getpass.getuser()
        token = '@'.join([username, short_host])

    return ROUTE_DELIMITER.join(
        filter(bool, list(map(str, [token, component, proc_id]))))


def join(*routes):
    return ROUTE_DELIMITER.join(map(str, routes))


def split(route, maxsplit=-1):
    return route.split(ROUTE_DELIMITER, maxsplit)


def rsplit(route, maxsplit=-1):
    return route.rsplit(ROUTE_DELIMITER, maxsplit)


def get_host_route(route):
    return split(route)[0]


def normalize(router, route):
    r = route.split(ROUTE_DELIMITER)
    if (len(r) == 1 and '@' not in r[0]) or (len(r) == 2 and r[-1].isdigit()):
        route = join(get_host_route(router.gw.route), *r)
    return route


def subroutes(route):
    def _subroutes(route):
        if not route:
            return []
        else:
            return [route] + _subroutes(route.rpartition(ROUTE_DELIMITER)[0])

    r = split(route)
    assert r[-1].isdigit()
    return _subroutes(route)
