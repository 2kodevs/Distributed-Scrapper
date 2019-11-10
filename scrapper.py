import zmq, logging, time, os, requests, pickle
from util.params import client_addr, seeds, localhost
from multiprocessing import Process, Lock, Queue
from threading import Thread, Lock as tLock
from util.params import format, datefmt

logging.basicConfig(format=format, datefmt=datefmt)
log = logging.getLogger(name="Scrapper")
log.setLevel(logging.DEBUG)

lockQueue = Lock()
lockClients = tLock()
lockSocketPull = tLock()

def slave(tasks, uuid):
    """
    Represents a child process of Scrapper, responsable of downloading the urls.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    while True:
        clientAddr, url = tasks.get() 
        log.info(f"Child:{os.getpid()} of Scrapper:{self.uuid} downloading {url}")
        response = requests.get(url)
        log.debug(f"Child:{os.getpid()} of Scrapper:{self.uuid} connecting to {clientAddr}")
        socket.connect(f"tcp://{clientAddr}")
        log.info(f"Child:{os.getpid()} of Scrapper:{self.uuid} sending downloaded content of {url} to {clientAddr}")
        socket.send_json(("RESULT", response))
        
def discoverClients(clients:dict, clientQueue, uuid):
    """
    Thread responsable for receive a new client to connect to.
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    #//TODO: Connect efficiently to the proper seed
    socket.connect(f"tcp://{seeds[0]}")
    socket.setsockopt(zmq.SUBSCRIBE, b"NEW_CLIENT")
    
    while True:
        [sub, contents] = socket.recv_multipart()
        id, addr = pickle.loads(contents)
        log.debug(f"Scrapper:{uuid} has received a new client(id:{id}, address:{addr})")
        with lockClients:
            if id in clients.keys():
                clientQueue.put((id, addr))
            clients[id] = addr

def connectToClients(socket, clientQueue, uuid):
    for id, addr in iter(clientQueue.get, "STOP"):
        lockSocketPull.acquire()
        socket.connect(f"tcp://{c}")
        log.debug(f"Scrapper:{uuid} connected to client(id:{id}, address:{addr})")
        lockSocketPull.release()


class Scrapper:
    """
    Represents a scrapper, the worker node in the Scrapper network.
    """
    def __init__(self, uuid, address=localhost, port=8301):
        self.uuid = uuid
        self.clients = dict()
        self.addr = address
        self.port = port
        log.debug(f"Scrapper created with uuid {uuid}")

    def manage(self, slaves):
        """
        Start to manage childs-slaves.
        """
        context = zmq.Context()
        socketPull = context.socket(zmq.PULL)
         
        clientQueue = Queue()
        discoverT = Thread(target=discoverClients, args=(self.clients, clientQueue, self.uuid))
        connectT = Thread(target=connectToClients, args=(socketPull, clientQueue, self.uuid))
        discoverT.start()
        connectT.start()
        
        while len(self.clients) == 0:
            log.debug(f"Scrapper:{self.uuid} waiting for clients")
            time.sleep(5)

        taskQueue = Queue()
        log.debug(f"Scrapper:{self.uuid} starting child process")
        for s in range(slaves):
            p = Process(target=slave, args=(taskQueue, self.uuid))
            p.start()

        while True:
            #task: (client_addr, url)
            task = socketPull.recv_json()
            log.debug(f"Pulled {task[1]} in worker:{self.uuid}")
            taskQueue.put(task)

    #//TODO: Create seed methods.
            
if __name__ == "__main__":
    s = Scrapper(2)
    s.manage(2)
