import socket, logging, hashlib, random, sys, zmq, time, pickle, queue
from util.colors import REDB, BLUEB, YELLOWB
from util.params import format, datefmt, BROADCAST_PORT, login, localhost
from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, SO_BROADCAST, timeout
from multiprocessing import Process, Queue


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
    """
    Returns a number that can be used as an unique universal identifier.
    """
    name = ""
    random.shuffle(urls)
    for url in urls:
        name += url
        
    nounce = random.randint(1, n)
    h = hashlib.sha256(name.encode() + str(nounce).encode())
    return int.from_bytes(h.digest(), byteorder=sys.byteorder)


parseLevel = lambda x: getattr(logging, x)


def LoggerFactory(name="root"):
    '''
    Create a custom logger to use colors in the logs
    '''
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
    '''
    Create a custom zmq.REQ socket by modifying the values of:
    - zmq.REQ_RELAXED   to 1
    - zmq.REQ_CORRELATE to 1
    - zmq.RCVTIMEO      to <timeout>
    '''
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, timeout)
    return socket
      
        
def valid_tags(tag):
    '''
    Html tags filter
    '''
    return tag.has_attr('href') or tag.has_attr('src')


def discoverPeer(times, log):
        """
        Discover a seed in the subnet by broadcast.
        It not works offline.
        """
        sock = socket(AF_INET, SOCK_DGRAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        sock.settimeout(2)

        broadcastAddress = ('255.255.255.255', BROADCAST_PORT)
        message = login
        seed = ""
        network = True

        for i in range(times):
            try:
                log.info("Discovering peers", "discoverPeer")
                sock.sendto(message.encode(), broadcastAddress)

                log.debug("Waiting to receive", "discoverPeer")
                data, server = sock.recvfrom(4096)
                header, address = pickle.loads(data)
                if header == 'WELCOME':
                    log.info(f"Received confirmation: {address}", "discoverPeer")
                    log.info(f"Server: {str(server)}", "discoverPeer")
                    seed = f"{server[0]}:{address[1]}"
                    sock.close()
                    return seed, network
                else:
                    log.info("Login failed, retrying...", "discoverPeer")
            except timeout as e:
                log.error("Socket " + str(e), "discoverPeer")
            except Exception as e:
                log.error(e, "discoverPeer")
                log.error(f"Connect to a network please, retrying connection in {(i + 1) * 2} seconds...", "discoverPeer")
                network = False
                #Factor can be changed
                time.sleep((i + 1) * 2)

        sock.close()
        
        return seed, network


def change_html(html, changes):
    '''
    Function to replace a group of changes in a Html.
    Changes have the form (old, new)
    '''
    changes.sort(key=lambda x: len(x[0]), reverse=True)
    for url, name in changes:
        html = html.replace(f'"{url}"', f'"{name}"')
        html = html.replace(f"'{url}'", f"'{name}'")
    return html     


def getSeeds(seed, discoverPeer, address, login, q, log):
    """
    Request the list of seed nodes to a active seed, if <seed> is not active, then try to discover a seed active in the network. 
    """
    context = zmq.Context()
    sock = noBlockREQ(context, timeout=1200)
    sock.connect(f"tcp://{seed}")

    for i in range(4, 0, -1):
        try:
            sock.send_json(("GET_SEEDS",))
            seeds = sock.recv_pyobj()
            log.info(f"Received seeds: {seeds}", "Get Seeds")
            if login:
                sock.send_json(("NEW_SEED", address))
                sock.recv_json()
            sock.close()
            q.put(seeds)
            break
        except zmq.error.Again as e:
            log.debug(e, "Get Seeds")
            seed, _ = discoverPeer(i, log)
            if seed != "":
                sock.connect(f"tcp://{seed}")
        except Exception as e:
            log.error(e, "Get Seeds")
        finally:
            if i == 1:
                q.put({})
                

def ping(seed, q, time, log):
    """
    Process that make ping to a seed.
    """
    context = zmq.Context()
    socket = noBlockREQ(context, timeout=time)
    socket.connect(f"tcp://{seed[0]}:{seed[1]}")
    status = True

    log.debug(f"PING to {seed[0]}:{seed[1]}", "Ping")
    try:
        socket.send_json(("PING",))
        msg = socket.recv_json()
        log.info(f"Received {msg} from {seed[0]}:{seed[1]} after ping", "Ping")
    except zmq.error.Again as e:
        log.debug(f"PING failed -- {e}", "Ping")
        status = False
    q.put(status)


def findSeeds(seeds, peerQs, deadQs, log, timeout=1000, sleepTime=15, seedFromInput=None):
    """
    Process that ask to a seed for his list of seeds.
    """
    time.sleep(sleepTime)
    while True:
        #random address
        seed = (localhost, 9999)
        data = list(seeds)
        for s in data:
            #This process is useful to know if a seed is dead too
            pingQ = Queue()
            pPing = Process(target=ping, name="Ping", args=(s, pingQ, timeout, log))
            pPing.start()
            status = pingQ.get()
            pPing.terminate()
            if not status:
                for q in deadQs:
                    q.put(s)               
                seeds.remove(s)
            else:
                seed = s
        seedsQ = Queue()
        pGetSeeds = Process(target=getSeeds, name="Get Seeds", args=(f"{seed[0]}:{seed[1]}", discoverPeer, None, False, seedsQ, log))
        log.debug("Finding new seeds to pull from...", "Find Seeds")
        pGetSeeds.start()
        tmp = set(seedsQ.get())
        pGetSeeds.terminate()
        #If Get Seeds succeds to connect to a seed
        if len(tmp) != 0:
            dif = tmp - seeds
            if not len(dif):
                log.debug("No new seed nodes where finded", "Find Seeds")
            else:
                log.debug("New seed nodes where finded", "Find Seeds")

            for s in dif:
                for q in peerQs:
                    q.put(s)
            seeds.update(tmp)

            if seedFromInput is not None:
                try:
                    seeds.add(seedFromInput.get(block=False))
                except queue.Empty:
                    pass

        #The amount of the sleep in production can be changed 
        time.sleep(sleepTime)
        
        
def run_process(target, args):
    """
    Run a process and return it's result.
    """
    ansQ = Queue()
    args = args + (ansQ,)
    process = Process(target=target, args=args)
    process.start()
    ans = ansQ.get()
    process.terminate()
    return ans


def clock(cycle, q):
    """
    Process that notice to the <q> Queue owner when a cycle is passed.
    """
    time.sleep(cycle)
    q.put(True)

