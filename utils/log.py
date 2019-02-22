import logging

LOG_FORMAT = '%(asctime)s - %(processName)s->%(filename)s:%(lineno)d-' \
             ' %(levelname)s - %(message)s'


def init_logging():
    logging.basicConfig(format=LOG_FORMAT)
    logging.getLogger().setLevel(logging.DEBUG)
