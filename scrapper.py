import zmq, logging, time, os, requests
from util.params import seeds, localhost
from multiprocessing import Process, Lock, Queue, Value
from ctypes import c_int
from threading import Thread, Lock as tLock
from util.utils import parseLevel, LoggerFactory as Logger, noBlockREQ, discoverPeer, getSeeds


log = Logger(name="Scrapper")

availableSlaves = Value(c_int)

lockClients = tLock()
lockSocketPull = tLock()


def slave(tasks, notifications, idx):
    """
    Child Process of Scrapper, responsable of downloading the urls.
    """
    while True:
        url = tasks.get() 
        with availableSlaves:
            availableSlaves.value -= 1
        log.info(f"Child:{os.getpid()} of Scrapper downloading {url}", f"slave {idx}")
        #//TODO: Handle better a request connection error, we retry it a few times?
        for i in range(5):
            try:
                response = requests.get(url)
            except Exception as e:
                log.error(e, f"slave {idx}")
                if i == 4:
                    notifications.put(("FAILED", url, i))
                continue
            notifications.put(("DONE", url, response.text))
            break
        with availableSlaves:
            availableSlaves.value += 1
    

def listener(addr, port):
    socket = zmq.Context().socket(zmq.REP)
    socket.bind(f"tcp://{addr}:{port}")
    
    while True:
        res = socket.recv()
        socket.send_json(True)
    
    
def notifier(notifications):
    context = zmq.Context()
    socket = noBlockREQ(context)

    #//TODO: Connect to seeds in a way that a new seed can be added
    for addr, port in seeds:
        socket.connect(f"tcp://{addr}:{port + 2}")

    for msg in iter(notifications.get, "STOP"):
        try:
            assert len(msg) == 3, "wrong notification"
        except AssertionError as e:
            log.error(e)
            continue
        while True:
            try:
                log.debug(f"Sending msg: ({msg[0]}, {msg[1]}, data) to a seed", "Worker Notifier")
                socket.send_json(msg)
                # nothing important receive
                socket.recv()
                break
            except zmq.error.Again as e:
                log.debug(e, "Worker Notifier")
            except Exception as e:
                log.error(e, "Worker Notifier")
        
        
def connectToSeeds(sock, peerQ):
    """
    Thread that connect pull socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSocketPull:
            log.debug(f"Connecting to seed {addr}:{port + 1}","Connect to Seeds")
            sock.connect(f"tcp://{addr}:{port + 1}")
            log.info(f"Scrapper connected to seed with address:{addr}:{port + 1})", "Connect to Seeds")


def ping(seed, q):
    """
    Process that make ping to a seed.
    """
    context = zmq.Context()
    socket = noBlockREQ(context, timeout=1000)
    socket.connect(f"tcp://{seed[0]}:{seed[1]}")
    status = True

    log.debug(f"PING to {seed[0]}:{seed[1]}", "Ping")
    try:
        socket.send_json(("PING",))
        msg = socket.recv_json()
        log.debug(f"Received {msg} from {seed[0]}:{seed[1]} after ping", "Ping")
    except zmq.error.Again as e:
        log.debug(e, "Ping")
        status = False
    q.put(status)


def findSeeds(seeds, peerQ):
    """
    Process that ask to a seed for his list of seeds.
    """
    time.sleep(15)
    while True:
        #random address
        seed = (localhost, 9999)
        for s in seeds:
            #This process is useful to know if a seed is dead too
            seed = s
            pingQ = Queue()
            pPing = Process(target=ping, name="Ping", args=(s, pingQ))
            pPing.start()
            status = pingQ.get()
            pPing.terminate()
            if status:
                break
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

            for s in tmp - seeds:
                peerQ.put(s)
            seeds.update(tmp)

        #//TODO: Change the amount of the sleep in production
        time.sleep(15)


class Scrapper:
    """
    Represents a scrapper, the worker node in the Scrapper network.
    """
    def __init__(self, address, port):
        self.addr = address
        self.port = port
        
        log.debug(f"Scrapper created", "init")


    def login(self, seed):
        """
        Login the node in the system.
        """
        network = True
        if seed is not None:
            #ip_address:port_number
            regex = re.compile("\d{,3}\.\d{,3}\.\d{,3}\.\d{,3}:\d+")
            try:
                assert regex.match(seed).end() == len(seed)
            except (AssertionError, AttributeError):
                log.error(f"Parameter seed inserted is not a valid ip_address:port_number")
                seed = None

        if seed is None:
            #//TODO: Change times param in production
            log.debug("Discovering seed nodes", "login")
            seed, network = discoverPeer(3, log)
            if seed == "":
                self.seeds = list()
                log.info("Login finished", "login")
                return network

        seedsQ = Queue()
        pGetSeeds = Process(target=getSeeds, name="Get Seeds", args=(seed, discoverPeer, (self.addr, self.port), False, seedsQ, log))
        pGetSeeds.start()
        self.seeds = seedsQ.get()
        pGetSeeds.terminate()

        log.info("Login finished", "login")
        return network


    def manage(self, slaves):
        """
        Start to manage childs-slaves.
        """
        context = zmq.Context()
        socketPull = context.socket(zmq.PULL)
        
        seedsQ = Queue()
        for address in self.seeds:
            seedsQ.put(address)

        connectT = Thread(target=connectToSeeds, name="Connect to Seeds", args=(socketPull, seedsQ))
        connectT.start()

        pFindSeeds = Process(target=findSeeds, name="Find Seeds", args=(set(self.seeds), seedsQ))
        pFindSeeds.start()
        
        notificationsQ = Queue()
        pNotifier = Process(target=notifier, name="pNotifier", args=(notificationsQ,))
        pNotifier.start()
        
        pListen = Process(target=listener, name="pListen", args=(self.addr, self.port))
        pListen.start()
        
        taskQ = Queue()
        log.info(f"Scrapper starting child process", "manage")
        availableSlaves.value = slaves
        for i in range(slaves):
            p = Process(target=slave, args=(taskQ, notificationsQ, i))
            p.start()
            log.debug(f"Scrapper has started a child process with pid:{p.pid}", "manage")

        addr = (self.addr, self.port)
        while True:
            #task: (client_addr, url)
            with availableSlaves:
                if availableSlaves.value > 0:
                    log.debug(f"Available Slaves: {availableSlaves.value}", "manage")
                    url = socketPull.recv().decode()
                    taskQ.put(url)
                    notificationsQ.put(("PULLED", url, addr))
                    log.debug(f"Pulled {url} in scrapper", "manage")
            time.sleep(1)
            
        pListen.terminate()
        pNotifier.terminate()               


def main(args):
    log.setLevel(parseLevel(args.level))
    s = Scrapper(port=args.port, address=args.address)
    network = s.login(args.seed)
    if not network:
        log.info("You are not connected to a network", "main") 
    s.manage(2)

            
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Worker of a distibuted scrapper')
    parser.add_argument('-p', '--port', type=int, default=5050, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')
    parser.add_argument('-s', '--seed', type=str, default=None, help='address of a existing seed node. Insert as ip_address:port_number')

    args = parser.parse_args()

    main(args)