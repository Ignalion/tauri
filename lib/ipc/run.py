#!/usr/bin/env python27
import imp
import os
import sys

from twisted.internet import reactor

from testcore.ipc import get_router

if __name__ == '__main__':
    try:
        args = iter(sys.argv[1:])
        component = next(args)
        exchange = next(args)

    except StopIteration:
        print(('Usage: ' + sys.argv[0] + \
              ' <component> <exchange> [<module>:<name>, ...]'))
    else:

        router = get_router(component=component, exchange=exchange)

        for module in args:
            if ':' in module:
                name, module = module.split(':')
            else:
                name, _ = os.path.splitext(os.path.basename(module))

            print(("Serving %s as '%s' at %s:%s ..." %
                  (module, name, exchange, router.gw.route)))
            source = imp.load_source(name, module)
            print(source)
            setattr(router.handlers, name, source)

        router.start()
        reactor.run()
