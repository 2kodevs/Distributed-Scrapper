import zmq, logging, json, time
from util.params import format, datefmt, urls, seeds, login
from multiprocessing import Process, Lock
from threading import Thread, Lock as tLock
from util.utils import parseLevel, makeUuid

#//TODO: Find a way and a place to initialize more properly the logger for this module.
#//TODO: It would be useful that the logger log the thread of process name to.
logging.basicConfig(format=format, datefmt=datefmt)
log = logging.getLogger(name="Dispatcher")

lockResults = Lock()
lockPeers = Lock()
lockLogin = tLock()

change = False

def resultSubscriber(uuid, sizeUrls, peers, htmls, addr):
    """
    Child Process that get downloaded html from workers.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://{addr}")
    log.debug(f"Subscriber to results of Dispatcher:{uuid} created")
    global change

    i = 0
    #//HACK: Check if 'i' is enough for this condition to be fulfilled
    while i < sizeUrls:
        res = socket.recv_json()
        #nothing important to send
        socket.send(b"Done")
        if res[0] != "RESULT":
            continue
        url, html = res[1:]
        with open(f"results/html{i}", "w") as fd:
            fd.write(html)
        log.info(f"GET {url} OK")
        i += 1
        with lockResults:
            htmls.append((url, html))
            change = True

        
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
        self.urls = set(urls)
        self.uuid = uuid
        self.idToLog = str(uuid)[:10]
        self.address = address
        self.port = port

        self.pool = urls
        self.htmls = []
        self.peers = []
        log.debug(f"Dispatcher created with uuid {uuid}")
        
    def dispach(self):
        """
        Start to serve the Dispatcher.
        """
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind(f"tcp://{self.address}:{self.port}")
        global change
        #params here are thread-safe???
        pRSubscriber = Process(target=resultSubscriber, args=(self.idToLog, len(self.urls), self.peers, self.htmls, f"{self.address}:{self.port + 1}"))
        pRSubscriber.start()

        loginT = Thread(target=loginToNetwork, name="loginT", args=(self.address, self.port, self.idToLog))
        loginT.start()
        #Release again when node disconnect from the network unwittingly for some reason
        lockLogin.acquire()

        while True:
            if len(self.pool) == 0:
                #Check htmls vs urls and update pool
                log.debug(f"Waiting for update pool of Dispatcher:{self.idToLog}")
                with lockResults:
                #//HACK: For now the condition for the pool to be updated is that we get a result, but this is no correct because a worker can die without finish his task.
                    #//FIXME: change is no shared between pRSubscriber process
                    if change:
                        log.debug(f"Updating pool of Dispatcher:{self.idToLog}")
                        #//FIXME: self.htmls is no shared between pRSubscriber process
                        responsedURLs = {url for url, _ in self.htmls}
                        self.pool = self.urls - responsedURLs
                        change = False
            try:
                url = self.pool.pop()
                log.debug(f"Pushing {url} from Dispatcher:{self.idToLog}")
                socket.send_json((f"{self.address}:{self.port + 1}",url))
            except IndexError:
                with lockResults:
                    #//FIXME: self.htmls is no shared between pRSubscriber process
                    if len(self.urls) == len(self.htmls):
                        break
                time.sleep(5)

        log.info(f"Dispatcher:{self.uuid} has completed his URLs succefully")
        log.debug(f"Dispatcher:{self.uuid} disconnecting from system")
        #disconnect
        pRSubscriber.join()


def main(args):
    log.setLevel(parseLevel(args.level))
    uuid = makeUuid(2**55, urls)
    d = Dispatcher(urls, uuid, args.address, args.port)
    d.dispach()
    log.info(f"Dispatcher:{uuid} finish!!!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    parser.add_argument('-p', '--port', type=int, default=4142, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')

    #//TODO: use another arg to set the path to a file that contains the set of urls

    args = parser.parse_args()

    main(args)