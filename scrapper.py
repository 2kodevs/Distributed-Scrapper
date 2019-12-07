import zmq, logging, time, os, requests, pickle, re
from util.params import seeds, localhost
from multiprocessing import Process, Lock, Queue, Value
from ctypes import c_int
from threading import Thread, Lock as tLock,Semaphore
from util.utils import parseLevel, LoggerFactory as Logger, noBlockREQ, discoverPeer, getSeeds, findSeeds


log = Logger(name="Scrapper")

availableSlaves = Value(c_int)

lockWork = tLock()
lockClients = tLock()
lockSocketPull = tLock()
lockSocketNotifier = tLock()
counterSocketPull = Semaphore(value=0)
counterSocketNotifier = Semaphore(value=0)


def slave(tasks, notifications, idx, verifyQ):
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
            notifications.put(("DONE", url, response.content))
            verifyQ.put((False, url))
            break
        with availableSlaves:
            availableSlaves.value += 1
    

def listener(addr, port, queue, data):
    """
    Process to attend the verification messages sent by the seed.
    """
    
    def puller():
        for flag, url in iter(queue.get, "STOP"):
            with lockWork:
                try:
                    if flag:
                        data.append(url)
                    else:
                        data.remove(url)
                except Exception as e:
                    log.error(e, "puller")
                    
    thread = Thread(target=puller, args=(queue, data))
    thread.start()
    socket = zmq.Context().socket(zmq.REP)
    socket.bind(f"tcp://{addr}:{port}")
    
    while True:
        res = socket.recv().decode()
        with lockWork:
            socket.send_json(res in data)
    

def connectToSeeds(sock, inc, lock, counter, peerQ, user):
    """
    Thread that connect <sock> socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lock:
            log.debug(f"Connecting to seed {addr}:{port + inc}", f"Connect to Seeds -- {user} socket")
            sock.connect(f"tcp://{addr}:{port + inc}")
            counter.release()
            log.info(f"Scrapper connected to seed with address:{addr}:{port + inc})", f"Connect to Seeds -- {user} socket")


def disconnectFromSeeds(sock, inc, lock, counter, peerQ, user):
    """
    Thread that disconnect <sock> socket from seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lock:
            log.debug(f"Disconnecting from seed {addr}:{port + inc}", f"Disconnect from Seeds -- {user} socket")
            sock.disconnect(f"tcp://{addr}:{port + inc}")
            counter.acquire()
            log.info(f"Scrapper disconnected from seed with address:{addr}:{port + inc})", f"Disconnect from Seeds -- {user} socket")


def notifier(notifications, peerQ, deadQ):
    """
    Process to send notifications of task's status to seeds.
    """
    context = zmq.Context()
    socket = noBlockREQ(context)

    #Thread that connect REQ socket to seeds
    connectT = Thread(target=connectToSeeds, name="Connect to Seeds - Notifier", args=(socket, 2, lockSocketNotifier, counterSocketNotifier, peerQ, "Notifier"))
    connectT.start()

    #Thread that disconnect REQ socket from seeds
    disconnectT = Thread(target=disconnectFromSeeds, name="Disconnect from Seeds - Notifier", args=(socket, 2, lockSocketNotifier, counterSocketNotifier, deadQ, "Notifier"))
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
                        #msg: (flag, url, data)
                        socket.send_pyobj(msg)
                        # nothing important to receive
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
        sel.curTask = []
        
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

        #Thread that connect pull socket to seeds
        connectT = Thread(target=connectToSeeds, name="Connect to Seeds - Pull", args=(socketPull, 1, lockSocketPull, counterSocketPull, seedsQ1, "Pull"))
        connectT.start()

        pendingQ = Queue()
        toDisconnectQ1 = Queue()
        toDisconnectQ2 = Queue()

        #Thread that disconnect pull socket from seeds
        disconnectT = Thread(target=disconnectFromSeeds, name="Disconnect from Seeds - Pull", args=(socketPull, 1, lockSocketPull, counterSocketPull, toDisconnectQ1, "Notifier"))
        disconnectT.start()

        pFindSeeds = Process(target=findSeeds, name="Find Seeds", args=(set(self.seeds), [seedsQ1, seedsQ2], [toDisconnectQ1, toDisconnectQ2], log))
        pFindSeeds.start()
        
        notificationsQ = Queue()
        pNotifier = Process(target=notifier, name="pNotifier", args=(notificationsQ, seedsQ2, toDisconnectQ2))
        pNotifier.start()
        
        listenT = Process(target=listener, name="pListen", args=(self.addr, self.port, pendingQ, self.curTask))
        listenT.start()
        
        taskQ = Queue()
        log.info(f"Scrapper starting child process", "manage")
        availableSlaves.value = slaves
        for i in range(slaves):
            p = Process(target=slave, args=(taskQ, notificationsQ, i, pendingQ))
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
                        with lockWork:
                            if url not in self.curTask:
                                taskQ.put(url)
                                notificationsQ.put(("PULLED", url, addr))
                                pendingQ.put((True, url))
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
    s.manage(args.workers)

            
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Worker of a distibuted scrapper')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-p', '--port', type=int, default=5050, help='connection port')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')
    parser.add_argument('-s', '--seed', type=str, default=None, help='address of a existing seed node. Insert as ip_address:port_number')
    parser.add_argument('-w', '--workers', type=int, default=2, help='number of slaves')


    args = parser.parse_args()

    main(args)