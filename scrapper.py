import zmq, logging, time, os, requests, re
from multiprocessing import Process, Lock, Queue, Value
from ctypes import c_int
from threading import Thread, Lock as tLock,Semaphore
from util.utils import parseLevel, LoggerFactory as Logger, noBlockREQ, discoverPeer, getSeeds, findSeeds


log = Logger(name="Scrapper")

availableSlaves = Value(c_int)

lockClients = tLock()
lockSocketPull = tLock()
lockSocketNotifier = tLock()
counterSocketPull = Semaphore(value=0)
counterSocketNotifier = Semaphore(value=0)

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
    #//TODO: Describe this function
    socket = zmq.Context().socket(zmq.REP)
    socket.bind(f"tcp://{addr}:{port}")
    
    while True:
        res = socket.recv()
        socket.send_json(True)
    

def connectToSeeds1(sock, peerQ):
    """
    Thread that connect pull socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSocketPull:
            log.debug(f"Connecting to seed {addr}:{port + 1}","Connect to Seeds1")
            sock.connect(f"tcp://{addr}:{port + 1}")
            counterSocketPull.release()
            log.info(f"Scrapper connected to seed with address:{addr}:{port + 1})", "Connect to Seeds1")


def disconnectFromSeeds1(sock, peerQ):
    """
    Thread that disconnect pull socket from seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSocketPull:
            log.debug(f"Disconnecting from seed {addr}:{port + 1}","Disconnect from Seeds1")
            sock.disconnect(f"tcp://{addr}:{port + 1}")
            counterSocketPull.acquire()
            log.info(f"Scrapper disconnected from seed with address:{addr}:{port + 1})", "Disconnect from Seeds1")


def connectToSeeds2(sock, peerQ):
    """
    Thread that connect REQ socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSocketNotifier:
            log.debug(f"Connecting to seed {addr}:{port + 2}","Connect to Seeds2")
            sock.connect(f"tcp://{addr}:{port + 2}")
            counterSocketNotifier.release()
            log.info(f"Scrapper connected to seed with address:{addr}:{port + 2})", "Connect to Seeds2")
    

def disconnectFromSeeds2(sock, peerQ):
    """
    Thread that disconnect REQ socket from seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSocketNotifier:
            log.debug(f"Disconnecting from seed {addr}:{port + 2}","Disconnect from Seeds2")
            sock.disconnect(f"tcp://{addr}:{port + 2}")
            counterSocketNotifier.acquire()
            log.info(f"Scrapper disconnected from seed with address:{addr}:{port + 2})", "Disconnect from Seeds2")


def notifier(notifications, peerQ, deadQ):
    #//TODO: Describe this function
    context = zmq.Context()
    socket = noBlockREQ(context)

    connectT = Thread(target=connectToSeeds2, name="Connect to Seeds", args=(socket, peerQ))
    connectT.start()

    disconnectT = Thread(target=disconnectFromSeeds2, name="Disconnect from Seeds", args=(socket, deadQ))
    disconnectT.start()

    for msg in iter(notifications.get, "STOP"):
        try:
            assert len(msg) == 3, "wrong notification"
        except AssertionError as e:
            log.error(e)
            continue
        while True:
            try:
                with lockSocketNotifier:
                    if counterSocketNotifier.acquire(timeout=1):
                        log.debug(f"Sending msg: ({msg[0]}, {msg[1]}, data) to a seed", "Worker Notifier")
                        socket.send_json(msg)
                        # nothing important receive
                        socket.recv()
                        counterSocketNotifier.release()
                        break
            except zmq.error.Again as e:
                log.debug(e, "Worker Notifier")
                counterSocketNotifier.release()
            except Exception as e:
                log.error(e, "Worker Notifier")
                counterSocketNotifier.release()
            finally:
                time.sleep(0.5)

        

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

        seedsQ1 = Queue()
        seedsQ2 = Queue()
        for address in self.seeds:
            seedsQ1.put(address)
            seedsQ2.put(address)

        connectT = Thread(target=connectToSeeds1, name="Connect to Seeds", args=(socketPull, seedsQ1))
        connectT.start()

        toDisconnectQ1 = Queue()
        toDisconnectQ2 = Queue()

        disconnectT = Thread(target=disconnectFromSeeds1, name="Disconnect from Seeds", args=(socketPull, toDisconnectQ1))
        disconnectT.start()

        pFindSeeds = Process(target=findSeeds, name="Find Seeds", args=(set(self.seeds), [seedsQ1, seedsQ2], [toDisconnectQ1, toDisconnectQ2], log))
        pFindSeeds.start()
        
        notificationsQ = Queue()
        pNotifier = Process(target=notifier, name="pNotifier", args=(notificationsQ, seedsQ2, toDisconnectQ2))
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
        time.sleep(1)
        while True:
            #task: (client_addr, url)
            try:
                with availableSlaves:
                    if availableSlaves.value > 0:
                        log.debug(f"Available Slaves: {availableSlaves.value}", "manage")
                        with counterSocketPull:
                            with lockSocketPull:
                                url = socketPull.recv(flags=zmq.NOBLOCK).decode()
                        taskQ.put(url)
                        notificationsQ.put(("PULLED", url, addr))
                        log.debug(f"Pulled {url} in scrapper", "manage")
            except zmq.error.ZMQError as e:
                log.debug(f"No new messages to pull: {e}", "manage")
            time.sleep(1)
            
        pListen.terminate()
        pNotifier.terminate()               


def main(args):
    log.setLevel(parseLevel(args.level))
    s = Scrapper(port=args.port, address=args.address)
    if not s.login(args.seed):
        log.info("You are not connected to a network", "main") 
    s.manage(2)

            
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Worker of a distibuted scrapper')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-p', '--port', type=int, default=5050, help='connection port')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')
    parser.add_argument('-s', '--seed', type=str, default=None, help='address of a existing seed node. Insert as ip_address:port_number')

    args = parser.parse_args()

    main(args)