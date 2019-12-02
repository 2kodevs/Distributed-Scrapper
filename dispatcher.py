import zmq, time, re
from util.params import urls
from util.colors import GREEN, RESET
from multiprocessing import Process, Queue
from util.utils import parseLevel, makeUuid, LoggerFactory as Logger, noBlockREQ, discoverPeer, getSeeds, findSeeds
from threading import Thread, Lock as tLock, Semaphore


log = Logger(name="Dispatcher")

lockSocketReq = tLock()
counterSocketReq = Semaphore(value=0)


def downloadsWriter(queue):
    for index, url, data in iter(queue.get, "STOP"):
        with open(f"downloads/html{index}", "w") as fd:
            log.info(f"{url} saved")
            fd.write(data)
    log.debug("All data saved")
  

def connectToSeeds(sock, peerQ):
    """
    Thread that connect REQ socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSocketReq:
            log.debug(f"Connecting to seed {addr}:{port}","Connect to Seeds")
            sock.connect(f"tcp://{addr}:{port}")
            counterSocketReq.release()
            log.info(f"Dispatcher connected to seed with address:{addr}:{port})", "Connect to Seeds")


def disconnectToSeeds(sock, peerQ):
    """
    Thread that disconnect REQ socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSocketReq:
            log.debug(f"Disconnecting to seed {addr}:{port}","Disconnect to Seeds")
            sock.disconnect(f"tcp://{addr}:{port}")
            counterSocketReq.acquire()
            log.info(f"Dispatcher disconnected to seed with address:{addr}:{port})", "Disconnect to Seeds")


class Dispatcher:
    """
    Represents a client to the services of the Scrapper.
    """
    def __init__(self, urls, uuid, address="127.0.0.1", port=4142):
        self.urls = list(set(urls))
        self.uuid = uuid
        self.idToLog = str(uuid)[:10]
        self.address = address
        self.port = port

        log.debug(f"Dispatcher created with uuid {uuid}", "Init")
        

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
                log.error("Login failed, get the address of a active master node or connect to the same network that the service", "login")
                return False

        seedsQ = Queue()
        pGetSeeds = Process(target=getSeeds, name="Get Seeds", args=(seed, discoverPeer, (self.address, self.port), False, seedsQ, log))
        pGetSeeds.start()
        self.seeds = seedsQ.get()
        pGetSeeds.terminate()

        if not len(self.seeds):
            log.error("Login failed, get the address of a active master node or connect to the same network that the service", "login")
            return False

        log.info("Login finished", "login")
        return network


    def dispatch(self, queue):
        """
        Start to serve the Dispatcher.
        """
        context = zmq.Context()
        socket = noBlockREQ(context)
        
        seedsQ = Queue()
        for address in self.seeds:
            seedsQ.put(address)

        connectT = Thread(target=connectToSeeds, name="Connect to Seeds", args=(socket, seedsQ))
        connectT.start()

        toDisconnectQ = Queue()
        disconnectT = Thread(target=disconnectToSeeds, name="Disconnect to Seeds", args=(socket, toDisconnectQ))
        disconnectT.start()

        pFindSeeds = Process(target=findSeeds, name="Find Seeds", args=(set(self.seeds), [seedsQ], [toDisconnectQ], log, 2000, 10))
        pFindSeeds.start()

        downloadsQ = Queue()
        pWriter = Process(target=downloadsWriter, args=(downloadsQ,))
        pWriter.start()

        idx = {url: i for i, url in enumerate(self.urls)}
        while len(self.urls):
            try:
                url = self.urls[0]
                with counterSocketReq:
                    socket.send_json(("URL", url))
                    log.debug(f"send {url}", "dispatch")
                    response = socket.recv_json()
                assert len(response) == 2, "bad response size"
                download, html = response
                self.urls.pop(0)
                if download:
                    log.debug(f"Received {download}", "dispatch")
                    log.info(f"{url} {GREEN}OK{RESET}", "dispatch")
                    downloadsQ.put((idx[url], url, html))
                else:
                    log.debug(f"Received {download, html}", "dispatch")
                    self.urls.append(url)
            except AssertionError as e:
                log.error(e, "dispatch")
            except zmq.error.Again as e:
                log.debug(e, "dispatch")
            except Exception as e:
                log.error(e, "dispatch")
                seeds.append(seeds.pop(0))  
            time.sleep(1)

        log.info(f"Dispatcher:{self.uuid} has completed his URLs succefully", "dispatch")
        log.debug(f"Dispatcher:{self.uuid} disconnecting from system", "dispatch")
        #disconnect

        downloadsQ.put("STOP")
        pWriter.join()
        queue.put(True)
        pFindSeeds.terminate()
        
        
def main(args):
    log.setLevel(parseLevel(args.level))
    
    uuid = makeUuid(2**55, urls)
    d = Dispatcher(urls, uuid, args.address, args.port)
    if not d.login(args.seed):
        return
    terminateQ = Queue()
    pDispatch = Process(target=d.dispatch, args=(terminateQ,))
    pDispatch.start()
    terminateQ.get()
    log.info(f"Dispatcher:{uuid} finish!!!", "main")
    pDispatch.terminate()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-p', '--port', type=int, default=4142, help='connection port')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')
    parser.add_argument('-s', '--seed', type=str, default=None, help='address of a existing seed node. Insert as ip_address:port_number')


    #//TODO: use another arg to set the path to a file that contains the set of urls

    args = parser.parse_args()

    main(args)
