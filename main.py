import logging
from src.dispacher import Dispacher
from src.util.params import urls, format, datefmt

if __name__ == "__main__":
    logging.basicConfig(format=format, datefmt=datefmt)
    log = logging.getLogger("main")
    log.setLevel(logging.DEBUG)

    log.info("Creating a dispacher")
    d1 = Dispacher(urls, 1)
    d1.dispach()