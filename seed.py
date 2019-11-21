import zmq
from multiprocessing import Process, Queue
from util.utils import parseLevel, LoggerFactory as Logger

log = Logger(name="Seed")
pMainLog = "main"

pushQueue = Queue()
pulledQueue = Queue()

def pushTask(toPushQ, addr):
    """
    Process that push tasks to workers and notify pulled tasks.
    """
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind(f"tcp://{addr}")
    
    for url in iter(toPushQ.get, "STOP"):
        log.debug(f"Pushing {url}", "PushTask")
        socket.send(url.encode())

def listenPulled(pulledQ, addr):
    """
    Process that listen notifications of PULLED from workers.
    """
    context = zmq.Context()

class Seed:
    """
    Represents a seed node, the node that receive and attend all client request.
    """
    def __init__(self, address, port):
        self.addr = address
        self.port = port
        self.tasks = dict()
        log.debug(f"Seed node created with address:{address}:{port}", pMainLog)

    def serve(self):
        """
        Start to attend clients.
        """
        context = zmq.Context()
        socket = context.socket.(zmq.REP)
        socket.bind(f"tcp://{self.addr}:{self.port}")

        while True:
            try:
                msg = socket.recv_json()
                if msg[0] != "URL":
                    msg.send(b"UNKNOWN")
                url = msg[1]
                #check res
                res = self.tasks[url]
                socket.send_json(res)
            except KeyError:
                

            


def main(args):
    log.setLevel(parseLevel(args.level))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    # parser.add_argument('-p', '--port', type=int, default=4142, help='connection port')
    # parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')

    args = parser.parse_args()

    main(args)
