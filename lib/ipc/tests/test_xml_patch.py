import pytest
from xml.etree import ElementTree as etree

from testcore.util.xml import patch_element, replace, append, REMOVE, Tag, prettify


@pytest.mark.parametrize('before, patch, expected', [
    (
        '<root></root>',
        {'test': 'true'},
        '<root><test>true</test></root>',
    ),
    (
        '<root><test>true</test></root>',
        {'test': 'false'},
        '<root><test>false</test></root>',
    ),
    (
        '<root><outer><first>1</first></outer></root>',
        {'outer': {'second': 2}},
        '<root><outer><first>1</first><second>2</second></outer></root>',
    ),
    ( # with xpath:
        '<root><outer><first>1</first></outer></root>',
        {Tag('outer/first'): 2},
        '<root><outer><first>2</first></outer></root>',
    ),
    (
        '<root><outer><first>1</first></outer></root>',
        {'outer': replace({Tag('second', attr='x'): 2})},
        '<root><outer><second attr="x">2</second></outer></root>',
    ),
    (
        '<root><outer><first>1</first></outer></root>',
        {'outer': {'first': REMOVE, 'second': 2}},
        '<root><outer><second>2</second></outer></root>',
    ),
    (
        '<root><outer><inner>1</inner><inner>2</inner></outer></root>',
        {'outer': {'inner': 3}},
        '<root><outer><inner>3</inner><inner>3</inner></outer></root>',
    ),
    (
        '<root><outer>'
        '<inner>1</inner><inner>2</inner>'
        '</outer></root>',

        {'outer': append({'inner': 3})},

        '<root><outer>'
        '<inner>1</inner><inner>2</inner><inner>3</inner>'
        '</outer></root>',
    ),
    (
        '<root><test>true</test></root>',
        replace({}),
        '<root></root>',
    ),
    (
        '<root><id>1</id><id>2</id></root>',
        append([(Tag('id'), 3), (Tag('id'), 4)]),
        '<root><id>1</id><id>2</id><id>3</id><id>4</id></root>'
    )
])
def test_patch(before, patch, expected):
    element = etree.fromstring(before)
    patch_element(element, patch)
    assert prettify(element) == prettify(etree.fromstring(expected))
