import socket, logging, hashlib, random, sys

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

parseLevel = action=lambda x: eval(f"logging.{x}")