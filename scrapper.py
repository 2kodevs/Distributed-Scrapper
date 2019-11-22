import zmq, logging, time, os, requests
from util.params import seeds, localhost
from multiprocessing import Process, Lock, Queue, Value
from ctypes import c_int
from threading import Thread, Lock as tLock
from util.utils import parseLevel, LoggerFactory as Logger


log = Logger(name="Scrapper")

availableSlaves = Value(c_int)

lockClients = tLock()
lockSocketPull = tLock()

def slave(tasks, notifications, uuid, idx):
    """
    Child Process of Scrapper, responsable of downloading the urls.
    """
    while True:
        url = tasks.get() 
        with availableSlaves:
            availableSlaves.value -= 1
        log.info(f"Child:{os.getpid()} of Scrapper:{uuid} downloading {url}", f"slave{idx}")
        #//TODO: Handle request connection error
        response = requests.get(url)
        notifications.put(("DONE", url, response.text))
        with availableSlaves:
            availableSlaves.value += 1
  
        
def connectToSeeds(socket, clientQueue, uuid):
    for s in seeds:
        clientQueue.put(s)
    for addr, port in iter(clientQueue.get, "STOP"):
        try:
            socket.connect(f"tcp://{addr}:{port + 1}")
            log.info(f"Scrapper:{uuid} connected to seed with address:{addr}:{port + 1})", "connectToSeeds")
        except:
            clientQueue.put((addr, port))
    

def listener(addr, port):
    socket = zmq.Context().socket(zmq.REP)
    socket.bind(f"tcp://{addr}:{port}")
    
    while True:
        res = socket.recv()
        socket.send_json(True)
    
    
def notifier(notifications):
    context = zmq.Context()
    for msg in iter(notifications.get, "STOP"):
        try:
            assert len(msg) == 3, "wrong notification"
        except AssertionError as e:
            log.error(e)
            continue
        while True:
            addr, port = seeds[0]
            try:
                socket = context.socket(zmq.REQ)
                socket.connect(f"tcp://{addr}:{port + 2}")
                socket.send_json(msg)
                # nothing important receive
                socket.recv()
                socket.close()
                break
            except:
                seeds.append(seeds.pop(0)) 
        
        
class Scrapper:
    """
    Represents a scrapper, the worker node in the Scrapper network.
    """
    def __init__(self, uuid, address, port):
        self.uuid = uuid
        self.addr = address
        self.port = port
        
        log.debug(f"Scrapper created with uuid {uuid}", "init")

    def manage(self, slaves):
        """
        Start to manage childs-slaves.
        """
        context = zmq.Context()
        socketPull = context.socket(zmq.PULL)
        
        clientQueue = Queue()
        connectT = Thread(target=connectToSeeds, name="connectT", args=(socketPull, clientQueue, self.uuid))
        connectT.start()
        
        notificationsQueue = Queue()
        pNotifier = Process(target=notifier, name="pNotifier", args=(notificationsQueue,))
        pNotifier.start()
        
        pListen = Process(target=listener, name="pListen", args=(self.addr, self.port))
        pListen.start()
        
        taskQueue = Queue()
        log.info(f"Scrapper:{self.uuid} starting child process", "manage")
        availableSlaves.value = slaves
        for i in range(slaves):
            p = Process(target=slave, args=(taskQueue, notificationsQueue, self.uuid, i))
            p.start()
            log.debug(f"Scrapper:{self.uuid} has started a child process with pid:{p.pid}", "manage")

        addr = (self.addr, self.port)
        while True:
            #task: (client_addr, url)
            with availableSlaves:
                if availableSlaves.value > 0:
                    log.debug(f"Available Slaves: {availableSlaves.value}", "manage")
                    url = socketPull.recv().decode()
                    taskQueue.put(url)
                    notificationsQueue.put(("PULLED", url, addr))
                    log.debug(f"Pulled {url} in worker:{self.uuid}", "manage")
            
        pListen.terminate()
        pNotifier.terminate()               
        clientQueue.put("STOP")   
        connectT.join()


def main(args):
    log.setLevel(parseLevel(args.level))
    s = Scrapper(2, port=args.port, address=args.address)
    s.manage(2)

            
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Worker of a distibuted scrapper')
    parser.add_argument('-p', '--port', type=int, default=5050, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')

    args = parser.parse_args()

    main(args)