import socket, logging, hashlib, random, sys, zmq
from util.colors import REDB, BLUEB, YELLOWB
from util.params import format, datefmt


def getIp():
    """
    Return local ip address(it must be connected to internet).
    """
    return [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

def getIpOffline():
    """
    Return local ip address(works on LAN without internet).
    """
    return (([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")] or [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) + ["no IP found"])[0]

def makeUuid(n, urls):
    name = ""
    random.shuffle(urls)
    for url in urls:
        name += url
        
    nounce = random.randint(1, n)
    h = hashlib.sha256(name.encode() + str(nounce).encode())
    return int.from_bytes(h.digest(), byteorder=sys.byteorder)

parseLevel = lambda x: getattr(logging, x)

def LoggerFactory(name="root"):
    logging.setLoggerClass(Logger)
    logging.basicConfig(format=format, datefmt=datefmt)
    return logging.getLogger(name=name)
    
class Logger(logging.getLoggerClass()):
    
    def __init__(self, name = "root", level = logging.NOTSET):
        self.debug_color =  BLUEB
        self.info_color = YELLOWB
        self.error_color = REDB
        return super().__init__(name, level)
        
    def debug(self, msg, mth=""):
        super().debug(msg, extra={"color": self.debug_color, "method": mth})
        
    def info(self, msg, mth=""):
        super().info(msg, extra={"color": self.info_color, "method": mth})
        
    def error(self, msg, mth=""):
        super().error(msg, extra={"color": self.error_color, "method": mth})
        
    def change_color(self, method, color):
        setattr(self, f"{method}_color", color)
        
def noBlockREQ(context, timeout=2000):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, timeout)
    return socket
        
def valid_tags(tag):
    return tag.has_attr('href') or tag.has_attr('src')


    