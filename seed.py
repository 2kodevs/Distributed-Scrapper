import zmq, time, queue, pickle
from multiprocessing import Process, Queue
from threading import Thread, Lock as tLock
from util.params import seeds
from util.utils import parseLevel, LoggerFactory as Logger, noBlockREQ

log = Logger(name="Seed")
pMainLog = "main"

lockTasks = tLock()
lockSubscriber = tLock()

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
            if msg[0] == "PULLED":
                #msg = PULLED, url, workerAddr
                log.info(f"Message received: {msg}", "Worker Attender")
                pulledQ.put((False, msg[1], msg[2]))
            elif msg[0] == "DONE":
                #msg = DONE, url, html
                resultQ.put((True, msg[1], msg[2]))

            #nothing important to send
            socket.send(b"OK")  
        except Exception as e:
            #Handle connection error
            log.error(e, "Worker Attender")
            continue

            
def taskManager(tasks, q, toPubQ):
    """
    Thread that helps the seed main process to update the tasks map.
    """
    while True:
        try:
            flag, url, data = q.get(block=False)
            with lockTasks:
                tasks[url] = (flag, data)
                #publish to other seeds
                toPubQ.put((flag, url, data))
        except queue.Empty:
            time.sleep(1)  


def taskPublisher(addr, taskQ):
    """
    Process that publish tasks changes to others seed nodes.
    """
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://{addr}")

    while True:
        try:
            #task: (flag, url, data)
            task = taskQ.get()
            log.debug(f"Publish task: ({task[0]}, {task[1]})", "Task Publisher")
            socket.send_multipart([b"TASK", pickle.dumps(task)])
        except Exception as e:
            log.error(e, "Task Publisher")


def connectToPublishers(socket, addr, port, peerQ):
    """
    Thread that connect subscriber socket to seeds.
    """
    while True:
        pAddr, pPort = peerQ.get()
        #//TODO: Ask for address to, now we are testing with localhost for everybody
        if pPort != port:
            with lockSubscriber:
                log.debug(f"Connecting to seed {pAddr}:{pPort + 3}","Connect to Publishers")
                socket.connect(f"tcp://{pAddr}:{pPort + 3}")


def taskSubscriber(tasks, addr, port, peerQ):
    """
    Process that subscribe to published tasks
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.SUBSCRIBE, b"TASK")

    connectT = Thread(target=connectToPublishers, name="Connect to Publishers", args=(socket, addr, port, peerQ))
    connectT.start()
    time.sleep(1)

    while True:
        try:
            #task: (flag, url, data)
            with lockSubscriber:
                header, task = socket.recv_multipart()
                flag, url, data = pickle.loads(task)
                log.debug(f"Received Subscribed message: ({header.decode()}, {flag}, {url})", "Task Subscriber")
                with lockTasks:
                    tasks[url] = (flag, data)
        except Exception as e:
            log.error(e, "Task Subscriber")


def purger(tasks, cycle):
    """
    Thread that purge the downloaded htmls from tasks map when a time cycle occurs.
    """
    while True:
        with lockTasks:
            log.debug("Starting purge", "Purger")
            for url in tasks:
                if tasks[url][0]:
                    tasks.pop(url)
        log.debug("Purge finished", "Purger")
        time.sleep(cycle)


def getRemoteTasks(seedList, tasksQ):
    """
    Process that ask to other seed for his tasks.
    """
    context = zmq.Context()
    socket = noBlockREQ(context, timeout=1000)

    for s in seedList:
        socket.connect(f"tcp://{s}")

    #//HACK: Increase this number in a factor of two of the number of seeds or more
    for _ in range(4):
        try:
            socket.send_json("GET_TASKS")
            response = socket.recv_pyobj()
            if isinstance(response, dict):
                tasksQ.put(response)
                break
        except zmq.error.Again as e:
            log.debug(e, "Get Remote Tasks")
        except Exception as e:
            log.error(e, "Get Remote Tasks")
    socket.close()
    tasksQ.put(None)


class Seed:
    """
    Represents a seed node, the node that receive and attend all client request.
    """
    def __init__(self, address, port):
        self.addr = address
        self.port = port

        sList = list()
        #//TODO: Connect to seeds in a way that a new seed can be added
        for addr, p in seeds:
            if p != port:
                #//TODO: Ask for address to, now we are testing with localhost for everybody
                sList.append(f"{addr}:{p}")
        tasksQ = Queue()
        pGetRemoteTasks = Process(target=getRemoteTasks, name="Get Remote Tasks", args=(sList, tasksQ))
        pGetRemoteTasks.start()
        tasks = tasksQ.get()
        pGetRemoteTasks.terminate()
        if tasks is None:
            tasks = dict()

        self.tasks = tasks
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
        taskToPubQ = Queue()
        seedsQ = Queue()

        #//HACK: When a new seed enter the system, his address and port must be inserted in seedsQ
        #//TODO: Use seeds in a way that a new seed can be added
        for s in seeds:
            seedsQ.put(s)

        pPush = Process(target=pushTask, name="Task Pusher", args=(pushQ, f"{self.addr}:{self.port + 1}"))
        pWorkerAttender = Process(target=workerAttender, name="Worker Attender", args=(pulledQ, resultQ, f"{self.addr}:{self.port + 2}"))
        pTaskPublisher = Process(target=taskPublisher, name="Task Publisher", args=(f"{self.addr}:{self.port + 3}", taskToPubQ))
        pTaskSubscriber = Process(target=taskSubscriber, name="Task Subscriber", args=(self.tasks, self.addr, self.port, seedsQ))

        taskManager1T = Thread(target=taskManager, name="Task Manager", args=(self.tasks, pulledQ, taskToPubQ))
        taskManager2T = Thread(target=taskManager, name="Task Manager", args=(self.tasks, resultQ, taskToPubQ))
        purgerT = Thread(target=purger, name="Purger", args=(self.tasks, 10))

        pPush.start()
        pWorkerAttender.start()
        pTaskPublisher.start()
        pTaskSubscriber.start()

        taskManager1T.start()
        taskManager2T.start()
        purgerT.start()

        time.sleep(0.5)

        while True:
            try:
                msg = socket.recv_json()
                if msg[0] == "URL":
                    url = msg[1]
                    with lockTasks:
                        try:
                            res = self.tasks[url]
                        except KeyError:
                            res = self.tasks[url] = [False, "Pushed"]
                            pushQ.put(url)
                    socket.send_json(res)
                elif msg[0] == "GET_TASKS":
                    with lockTasks:
                        socket.send_pyobj(self.tasks)
                else:
                    socket.send(b"UNKNOWN")
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
    parser.add_argument('-p', '--port', type=int, default=8101, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')

    args = parser.parse_args()

    main(args)
