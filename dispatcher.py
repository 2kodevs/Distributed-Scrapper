import zmq, time
from util.params import urls, seeds
from util.colors import GREEN, RESET
from multiprocessing import Process, Queue
from util.utils import parseLevel, makeUuid, LoggerFactory as Logger


log = Logger(name="Dispatcher")


def downloadsWriter(queue):
    for index, url, data in iter(queue.get, "STOP"):
        with open(f"downloads/html{index}", "w") as fd:
            log.info(f"{url} saved")
            fd.write(data)
    log.debug("All data saved")
  
            
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
        
    def dispatch(self):
        """
        Start to serve the Dispatcher.
        """
        context = zmq.Context()

        downloadsQueue = Queue()
        pWriter = Process(target=downloadsWriter, args=(downloadsQueue,))
        pWriter.start()

        idx = {url: i for i, url in enumerate(self.urls)}
        while len(self.urls):
            try:
                url = self.urls[0]
                addr, port = seeds[0]
                #//HACK: We need to create a socket every time?
                socket = context.socket(zmq.REQ)
                socket.connect(f"tcp://{addr}:{port}")
                log.debug(f"connected to {seeds[0]}", "dispatch")
                socket.send_json(("URL", url))
                log.debug(f"send {url}", "dispatch")
                response = socket.recv_json()
                assert len(response) == 2, "bad response size"
                download, html = response
                self.urls.pop(0)
                if download:
                    log.info(f"{url} {GREEN}OK{RESET}", "dispatch")
                    downloadsQueue.put((idx[url], url, html))
                else:
                    self.urls.append(url)
                socket.close()
            except AssertionError as e:
                log.error(e, "dispatch")
            except Exception as e:
                log.error(e, "dispatch")
                seeds.append(seeds.pop(0))  
            time.sleep(1)


        log.info(f"Dispatcher:{self.uuid} has completed his URLs succefully", "dispatch")
        log.debug(f"Dispatcher:{self.uuid} disconnecting from system", "dispatch")
        #disconnect
        downloadsQueue.put("STOP")
        

def main(args):
    log.setLevel(parseLevel(args.level))
    
    uuid = makeUuid(2**55, urls)
    d = Dispatcher(urls, uuid, args.address, args.port)
    d.dispatch()
    log.info(f"Dispatcher:{uuid} finish!!!", "main")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    parser.add_argument('-p', '--port', type=int, default=4142, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')

    #//TODO: use another arg to set the path to a file that contains the set of urls

    args = parser.parse_args()

    main(args)
