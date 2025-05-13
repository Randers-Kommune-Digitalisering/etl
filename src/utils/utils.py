import logging


logger = logging.getLogger(__name__)


def format_text(string):
    trans_dict = dict.fromkeys(' -/', '_')
    trans_dict.update({'ø': 'oe', 'å': 'aa', 'æ': 'ae'})
    return string.lower().translate(str.maketrans(trans_dict))


def flatten_xml(element):
    result = {}
    for child in element.iter():
        if child.tag != element.tag:
            key = child.tag
            value = child.text.strip() if child.text else None
            result[key] = value
            result.update(flatten_xml(child))
    return result
