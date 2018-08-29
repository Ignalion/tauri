

import logging
from collections import OrderedDict

from xml.dom import minidom
from xml.etree import ElementTree as etree

log = logging.getLogger(__name__)


class replace(OrderedDict):
    """Replace corresponding section"""


class append(OrderedDict):
    """Append to corresponding tag"""


class REMOVE(object):
    """Remove corresponding tag"""


class Tag(object):
    def __init__(self, name, **kwargs):
        self.name = name
        self.attrs = kwargs


def patch_element(element, patch, _loc=''):

    processed = set()
    ordered = sorted if not isinstance(patch, OrderedDict) else lambda _: _

    if isinstance(patch, replace):
        element.clear()

    for tag, content in ordered(iter(patch.items())):

        if isinstance(tag, str):
            tag = Tag(tag)

        full_path = '/'.join((_loc, tag.name))

        if isinstance(patch, append):
            matched = set()
        else:
            matched = set(element.findall(tag.name)) - processed

        if len(matched) > 1:
            log.warning('more than one element selected for: %s', full_path)
        elif not matched:
            # adding new element if there is nothing to update
            try:
                child = etree.Element(tag.name)
                # hack to validate tag name, ET doesn't do this by default:
                etree.fromstring(etree.tostring(child))
            except Exception:
                # e.g. if tag.name is in XPath format
                log.error('%s: was not found and can not be created', full_path)
                raise
            element.append(child)
            matched = {child}

        processed |= matched

        for child in matched:

            child.attrib.clear()
            child.attrib.update(tag.attrs)

            if isinstance(content, dict):
                patch_element(child, content, full_path)

            elif content is REMOVE:
                logging.debug('removing: %s', full_path)
                element.remove(child)

            else:
                logging.debug('setting text: %s = %s', full_path, content)
                child.text = str(content)


def prettify(elem):
    """Return a pretty-printed XML string for the Element."""
    rough_string = etree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    ''.splitlines()
    return '\n'.join(
        [x for x in reparsed.toprettyxml(indent="\t").splitlines() if x.strip()])