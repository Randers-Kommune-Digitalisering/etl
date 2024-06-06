import logging


logger = logging.getLogger(__name__)


def format_text(string):
    trans_dict = dict.fromkeys(' -/', '_')
    trans_dict.update({'ø': 'oe', 'å': 'aa', 'æ': 'ae'})
    return string.lower().translate(str.maketrans(trans_dict))
