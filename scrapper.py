import zmq, logging, time, os, requests, pickle
from util.params import seeds, localhost
from multiprocessing import Process, Lock, Queue
from threading import Thread, Lock as tLock
from util.params import format, datefmt, login

logging.basicConfig(format=format, datefmt=datefmt)
log = logging.getLogger(name="Scrapper")
log.setLevel(logging.DEBUG)

lockQueue = Lock()
lockClients = tLock()
lockSocketPull = tLock()

def slave(tasks, uuid):
    """
    Child Process of Scrapper, responsable of downloading the urls.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    while True:
        clientAddr, url = tasks.get() 
        log.info(f"Child:{os.getpid()} of Scrapper:{uuid} downloading {url}")
        response = requests.get(url)
        log.debug(f"Child:{os.getpid()} of Scrapper:{uuid} connecting to {clientAddr}")
        socket.connect(f"tcp://{clientAddr}")
        log.info(f"Child:{os.getpid()} of Scrapper:{uuid} sending downloaded content of {url} to {clientAddr}")
        socket.send_json(("RESULT", url, response.text))
        
def addClient(clientId, addr, port, clients:dict, clientQueue, uuid):
    lockClients.acquire()
    try:
        if clients[clientId] != (addr, port):
            clients[clientId] = (addr, port)
            clientQueue.put((clientId, (addr, port))) 
            log.debug(f"Client(id:{clientId}, address:{addr}) added to the queue of Scrapper:{uuid}")
    except KeyError:
        clients[clientId] = (addr, port)
        clientQueue.put((clientId, (addr, port)))  
        log.debug(f"Client(id:{clientId}, address:{addr}) added to the queue of Scrapper:{uuid}")
    finally:
        lockClients.release()


def discoverClients(clients:dict, clientQueue, uuid):
    """
    Thread responsable for receive a new client to connect to.
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    #//HACK: Connect efficiently to the proper seed
    socket.connect(f"tcp://{seeds[0]}")
    socket.setsockopt(zmq.SUBSCRIBE, b"NEW_CLIENT")
    
    while True:
        [sub, contents] = socket.recv_multipart()
        #addr: (address, port)
        clientId, addr = pickle.loads(contents)
        log.debug(f"Scrapper:{uuid} has received a new client(id:{clientId}, address:{addr[0]:addr[1]})")
        addClient(clientId, addr[0], addr[1], clients, clientQueue, uuid)

def connectToClients(socket, clientQueue, uuid):
    for clientId, addr in iter(clientQueue.get, "STOP"):
        lockSocketPull.acquire()
        #//TODO: Check if this is the proper port for connect socketPull
        socket.connect(f"tcp://{addr[0]}:{addr[1]}")
        log.debug(f"Scrapper:{uuid} connected to client(id:{clientId}, address:{addr[0]:addr[1]})")
        lockSocketPull.release()


class Scrapper:
    """
    Represents a scrapper, the worker node in the Scrapper network.
    """
    def __init__(self, uuid, address=localhost, port=8301, seed=False):
        self.uuid = uuid
        self.clients = dict()
        self.addr = address
        self.port = port
        self.seed = seed
        log.debug(f"Scrapper created with uuid {uuid} --- Is Seed:{seed}")

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
        for _ in range(slaves):
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
