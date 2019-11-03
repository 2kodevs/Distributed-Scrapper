import zmq, logging, json
from src.util.params import format, datefmt
from multiprocessing import Process, Lock

#TODO: Find a way and a place to initialize more properly the logger for this module.
logging.basicConfig(format=format, datefmt=datefmt)
log = logging.getLogger(name="Dispacher")
log.setLevel(logging.DEBUG)

lockResults = Lock()
lockPeers = Lock()

def resultSubcriber(uuid, sizeUrls, peers, htmls):
    """
    Get downloaded html from workers.
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    log.debug(f"Subscriber to results of Dispacher:{uuid} created")

    connectSocketToPeers(socket, uuid, peers)
    # socket.setsockopt(zmq.SUB, "RESULT")
    socket.setsockopt_string(zmq.SUBSCRIBE, "RESULT")
    i = 0
    #TODO: Check if 'i' is enough for this condition to be fulfilled
    while i < sizeUrls:
        res = socket.recv_json()
        url, html = res[0], res[1]
        log.debug(f"GET {url} OK")
        i += 1
        lockResults.acquire()
        htmls.append((url, html))
        lockResults.release()
        #TODO: Check if are new peers in the network to be subscribed to. Call connectSocketToPeers with a Thread?
        

def connectSocketToPeers(socket, uuid, peers):
    lockPeers.acquire()
    #peer = (address, port)
    log.debug(f"Subscriber of Dispacher:{uuid} connecting to available workers")
    for p in peers:
        socket.connect(f"tcp://{p[0]}:{p[1]}")
    lockPeers.release()

class Dispacher:
    """
    Represents a client to the services of the Scrapper.
    """
    def __init__(self, urls, uuid, address="127.0.0.1", port="4142"):
        self.urls = set(urls)
        self.uuid = uuid
        self.address = address
        self.port = port

        self.pool = self.urls
        self.htmls = []
        self.peers = []
        log.debug(f"Dispacher created with uuid {uuid}")
        
    def dispach(self):
        """
        Start to serve the dispacher.
        """
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind(f"tcp://{self.address}:{self.port}")
        #params here are thread-safe???
        rSubscriber = Process(target=resultSubcriber, args=(self.uuid, len(self.urls), self.peers, self.htmls))
        rSubscriber.start()

        while len(self.htmls) != len(self.urls):
            if len(self.pool) == 0:
                #Check htmls vs urls and update pool
                log.debug(f"Waiting for update pool of Dispacher:{self.uuid}")
                lockResults.acquire()
                log.debug(f"Updating pool of Dispacher:{self.uuid}")
                responsedURLs = {html for url, html in self.htmls}
                self.pool = self.urls - responsedURLs
                lockResults.release()

            url = self.pool.pop()
            log.debug(f"Pushing {url} from Dispacher:{self.uuid}")
            socket.send_json(url)

        log.info(f"Dispacher:{self.uuid} has completed his URLs succefully")
        log.debug(f"Dispacher:{self.uuid} disconnecting from system")
        #disconnect
        rSubscriber.join()