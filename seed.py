import zmq, time, queue
from multiprocessing import Process, Queue
from threading import Thread, Lock as tLock
from util.utils import parseLevel, LoggerFactory as Logger

log = Logger(name="Seed")
pMainLog = "main"

lockTasks = tLock()

def pushTask(toPushQ, addr):
    """
    Process that push tasks to workers and notify pulled tasks.
    """
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind(f"tcp://{addr}")
    
    for url in iter(toPushQ.get, "STOP"):
        log.debug(f"Pushing {url}", "Task Pusher")
        socket.send(url.encode())


def workerAttender(pulledQ, resultQ, addr):
    """
    Process that listen notifications from workers.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://{addr}")

    while True:
        try:
            msg = socket.recv_json()
            log.info(f"Message received: {msg}", "Worker Attender")
            if msg[0] == "PULLED":
                #msg = PULLED, url, workerAddr
                pulledQ.put(msg[1], msg[2])
            elif msg[0] == "DONE":
                #msg = DONE, url, html
                resultQ.put(msg[1], msg[2])

            #nothing important to send
            msg.send(f"OK")  
        except Exception as e:
            #Handle connection error
            log.error(e, "Worker Attender")
            continue

            
def taskManager(tasks, q):
    """
    Thread that helps the seed main process to update the tasks map.
    """
    while True:
        try:
            url, val = q.get(block=False)
            with lockTasks:
                tasks[url] = val
                #publish to other seeds
        except queue.Empty:
            time.sleep(1)       


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
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://{self.addr}:{self.port}")

        pushQ = Queue()
        pulledQ = Queue()
        resultQ = Queue()

        pPush = Process(target=pushTask, name="Task Pusher", args=(pushQ, f"{self.addr}:{self.port + 1}"))
        pWorkerAttender = Process(target=workerAttender, name="Worker Attender", args=(pulledQ, resultQ, f"{self.addr}:{self.port + 2}"))
        taskManager1T = Thread(target=taskManager, name="Task Manager", args=(self.tasks, pulledQ))
        taskManager2T = Thread(target=taskManager, name="Task Manager", args=(self.tasks, resultQ))

        pPush.start()
        pWorkerAttender.start()
        taskManager1T.start()
        taskManager2T.start()

        while True:
            try:
                msg = socket.recv_json()
                log.info(f"Message received: {msg}", "serve")
                if msg[0] != "URL":
                    msg.send(b"UNKNOWN")
                    continue
                url = msg[1]
                with lockTasks:
                    try:
                        res = self.tasks[url]
                    except KeyError:
                        res = self.tasks[url] = [False, "Pushed"]
                        pushQ.put(url)
                socket.send_json(res)
            except Exception as e:
                 #Handle connection error
                log.error(e, "serve")
            

def main(args):
    log.setLevel(parseLevel(args.level))
    s = Seed(args.address, args.port)
    s.serve()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    parser.add_argument('-p', '--port', type=int, default=4142, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')

    args = parser.parse_args()

    main(args)
