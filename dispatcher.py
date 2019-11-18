import zmq, logging, json, time
from util.params import format, datefmt, urls, seeds, login
from multiprocessing import Process, Lock, Queue
from threading import Thread, Lock as tLock
from util.utils import parseLevel
from util.colors import GREEN, RESET

#//TODO: Find a way and a place to initialize more properly the logger for this module.
#//TODO: It would be useful that the logger log the thread of process name to.
logging.basicConfig(format=format, datefmt=datefmt)
log = logging.getLogger(name="Dispatcher")

lockResults = Lock()
lockPeers = Lock()
lockLogin = tLock()

def resultSubscriber(uuid, sizeUrls, peers, addr, msg_queue):
    """
    Child Process that get downloaded html from workers.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://{addr}")
    log.debug(f"Subscriber to results of Dispatcher:{uuid} created")

    #connectSocketToPeers(socket, uuid, peers)
    #socket.setsockopt_string(zmq.SUBSCRIBE, "RESULT")
    i = 0
    #//HACK: Check if 'i' is enough for this condition to be fulfilled
    while i < sizeUrls:
        res = socket.recv_json()
        #nothing important to send
        socket.send(b"Done")
        if res[0] == "RESULT":
            log.info(f"GET {res[1]} {GREEN}OK{RESET}")
            i += 1
        msg_queue.put(res)
        #//TODO: Check if are new peers in the network to be subscribed to. Call connectSocketToPeers with a Thread?
        
        
def connectSocketToPeers(socket, uuid, peers):
    with lockPeers:
        #peer = (address, port)
        log.debug(f"Subscriber of Dispatcher:{uuid} connecting to available workers")
        for p in peers:
            socket.connect(f"tcp://{p[0]}:{p[1]}")


def loginToNetwork(addr, port, uuid):
    """
    Thread that enter the network when is requested.
    """
    while True:
        with lockLogin:
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            log.debug(f"Dispatcher:{uuid} connecting to a seed")
            #//HACK: Connect only to one seed per subnet. Or not?
            for sa, sp in seeds:
                socket.connect(f"tcp://{sa}:{sp + 1}")
                log.debug(f"Dispatcher:{uuid} connected to seed --- {sa}:{sp + 1}")

            #message: (login, client_id , client_address, client_port)
            #//HACK: This send message to all conections of the socket or to only one?
            log.debug(f"Dispatcher:{uuid} sending message of login to seeds")
            socket.send_json((login, uuid, addr, port))
            #nothing important to receive
            socket.recv()
        time.sleep(1)


class Dispatcher:
    """
    Represents a client to the services of the Scrapper.
    """
    def __init__(self, urls, uuid, address="127.0.0.1", port=4142):
        self.urls = list(set(urls))
        self.size = len(self.urls)
        self.status = [0] * self.size
        self.uuid = uuid
        self.address = address
        self.port = port

        self.pending = self.size
        self.pool = urls
        self.htmls = [None] * self.size
        self.peers = []
        log.debug(f"Dispatcher created with uuid {uuid}")
        
    def dispatch(self):
        """
        Start to serve the Dispatcher.
        """
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind(f"tcp://{self.address}:{self.port}")
        global change
        #params here are thread-safe???
        msg_queue = Queue()
        args = (self.uuid, self.size, self.peers, f"{self.address}:{self.port + 1}", msg_queue)
        pRSubscriber = Process(target=resultSubscriber, args=args)
        pRSubscriber.start()

        loginT = Thread(target=loginToNetwork, name="loginT", args=(self.address, self.port, self.uuid))
        loginT.start()
        #Release again when node disconnect from the network unwittingly for some reason
        lockLogin.acquire()

        idx = {url:i for i, url in enumerate(self.urls)}
        while True:
            if len(self.pool) == 0:
                #Check htmls vs urls and update pool
                log.debug(f"Waiting for update pool of Dispatcher:{self.uuid}")
                while not msg_queue.empty():
                    msg, url, data = msg_queue.get()
                    #//HACK: Maybe you don't have that urls
                    index = idx[url]
                    if msg == "RESULT" and self.status[index] != 2:
                        with open(f"results/html{index}", "w") as fd:
                            log.info(f"{url} saved")
                            fd.write(data)
                        self.status[index] = 2
                        self.pending -= 1
                    elif msg == "PULLED":
                        self.status[index] = data
                for i, status in enumerate(self.status):
                    if status == 0:
                        self.pool.append(self.urls[i])
                        self.status[i] = 1
                    elif isinstance(status, str):
                        try:
                            conn_sock = context.socket(zmq.REQ)
                            conn_sock.connect(status)
                            conn_sock.send_json(self.urls[i])
                            assert conn_sock.recv_json()
                        except:
                            self.status[i] = 0
            if len(self.pool):
                url = self.pool.pop()
                log.debug(f"Pushing {url} from Dispatcher:{self.uuid}")
                socket.send_json((f"{self.address}:{self.port + 1}",url))
            else:
                if self.pending == 0:
                        break
            time.sleep(5)

        log.info(f"Dispatcher:{self.uuid} has completed his URLs succefully")
        log.debug(f"Dispatcher:{self.uuid} disconnecting from system")
        #disconnect
        pRSubscriber.join()


def main(args):
    log.setLevel(parseLevel(args.level))
    d = Dispatcher(urls, 1, args.address, args.port)
    d.dispatch()
    log.info("Dispatcher:1 finish!!!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    parser.add_argument('-p', '--port', type=int, default=4142, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')

    #//TODO: use another arg to set the path to a file that contains the set of urls

    args = parser.parse_args()

    main(args)
