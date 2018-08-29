from pprint import pprint

import pytest

from testcore.util import colls
from testcore.util.lazy import foreach, lazy_dict, get, lazy


def test_merge():
    # basic
    assert colls.merge({1: 1}, {2: 2}) == {1: 1, 2: 2}
    assert colls.merge({1: 1, 2: 2}, {2: 3}) == {1: 1, 2: 3}
    assert colls.merge({1: 1, 2: {3: 3}}, {2: {4: 4}}) == {1: 1, 2: {3: 3, 4: 4}}

    # strict mode
    assert colls.merge({1:1}, {1:2}, strict=True) == {1:2}
    with pytest.raises(KeyError):
        colls.merge({1:1}, {2:2}, strict=True)

    assert colls.merge({1: None}, {1: {}}) == {1: {}}

    with pytest.raises(ValueError):
        colls.merge({1: None}, {1: {}}, strict=True)

    assert colls.merge([1], [2, 3]) == [2, 3]
    assert colls.merge([1], colls.extend([2, 3])) == [1, 2, 3]
    assert colls.merge([1], colls.extendleft([2, 3])) == [2, 3, 1]
    assert colls.merge(colls.consume([1]), [2, 3]) == [1, 2, 3]

    assert colls.merge({1}, {2, 3}) == {2, 3}
    assert colls.merge({1}, colls.unite({2, 3})) == {1, 2, 3}
    assert colls.merge({1, 2}, colls.complement({2, 3})) == {1}
    assert colls.merge(colls.superset(), {2, 3}, {1}) == {1, 2, 3}


def test_lazy():
    assert list(foreach([1, 2, 3]) + 1) == [2, 3, 4]

    x, X = lazy_dict()
    X.update({1:1, 2:x[1]+1})
    assert get(X) == {1:1, 2:2}

    X[1] = 2
    assert get(X) == {1:2, 2:3}

    X[x[2]] = 4
    assert get(X) == {1:2, 2:3, 3:4}

    lf = lazy(lambda x: lambda y: x + y)(1)(foreach([1, 2, 3]))
    assert repr(lf) == "lazy:<lambda>(1)(lazy:chain([1, 2, 3]))"
    assert list(lf) == [2, 3, 4]

    _, d = lazy_dict()
    d.update({
        'key': 'value',
        'two': _['key'] * 2,
        'big': _['two'].upper(),
        'nested': {
            'sparse': lazy(list)(_['key'])
        },
        'map': lazy(map)(str.upper, _['nested']['sparse']),
        'deeper': _['map'][0].join(['_', '_']),
        'x': _['nested'].copy()
    })
    print('lazy:')
    pprint(d)
    print('real:')
    pprint(get(d))

    assert get(d) == {
        'big': 'VALUEVALUE',
        'deeper': '_V_',
        'key': 'value',
        'map': ['V', 'A', 'L', 'U', 'E'],
        'nested': {'sparse': ['v', 'a', 'l', 'u', 'e']},
        'two': 'valuevalue',
        'x': {'sparse': ['v', 'a', 'l', 'u', 'e']}
    }

    d['key'] = 'spam'
    pprint(get(d))
    assert repr(d['nested']) == \
        "{'sparse': lazy:list(lazy:{...}['key'])}"

    assert get(d) == {
        'big': 'SPAMSPAM',
         'deeper': '_S_',
         'key': 'spam',
         'map': ['S', 'P', 'A', 'M'],
         'nested': {'sparse': ['s', 'p', 'a', 'm']},
         'two': 'spamspam',
         'x': {'sparse': ['s', 'p', 'a', 'm']}
    }
