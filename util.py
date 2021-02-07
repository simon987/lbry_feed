import logging
import sys
from logging import StreamHandler

logger = logging.getLogger("default")
logger.setLevel(logging.DEBUG)

for h in logger.handlers:
    logger.removeHandler(h)

handler = StreamHandler(sys.stdout)
handler.formatter = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s")
logger.addHandler(handler)
