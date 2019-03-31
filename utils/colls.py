def attrs(obj):
    return list(iattrs(obj))


def iattrs(obj):
    return (prop for prop in dir(obj) if not prop.startswith('__'))
