import zmq, time, queue, pickle
from multiprocessing import Process, Queue
from threading import Thread, Lock as tLock
from util.params import seeds
from util.utils import parseLevel, LoggerFactory as Logger, noBlockREQ

log = Logger(name="Seed")
pMainLog = "main"

lockTasks = tLock()
lockSubscriber = tLock()

def verificator(queue, t, pushQ):
    ansQ = Queue()
    for address, url in iter(queue.get, "STOP"):
        pQuick = Process(target=quickVerification, args=(address, url, t, ansQ))
        pQuick.start()
        ans = ansQ.get()
        pQuick.terminate()
        if not ans:
            pushQ.put(url)
            

def quickVerification(address, url, t, queue):
    context = zmq.Context()
    socket = noBlockREQ(context, timeout=t)
    ans = False
    try:
        addr, port = address
        socket.connect(f"tcp://{addr}:{port}")
        socket.send(url.encode())
        ans = socket.recv_json()
        log.debug(f"Worker at {address} is alive", "Quick Verification")
    except zmq.error.Again:
        log.debug(f"Worker at {address} unavailable", "Quick Verification")
    except Exception as e:
        log.error(e, "Quick Verification")
    finally:
        queue.put(ans)
        

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


def workerAttender(pulledQ, resultQ, failedQ, addr):
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
            elif msg[0] == "FAILED":
                #msg = FAILED, url, timesAttempted
                failedQ.put((False, msg[1], msg[1]))

            #nothing important to send
            socket.send(b"OK")  
        except Exception as e:
            #Handle connection error
            log.error(e, "Worker Attender")
            continue

            
def taskManager(tasks, q, toPubQ, pub):
    """
    Thread that helps the seed main process to update the tasks map.
    """
    while True:
        flag, url, data = q.get()
        with lockTasks:
            tasks[url] = (flag, data)
            #publish to other seeds
            if pub:
                toPubQ.put((flag, url, data))


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


def connectToPublishers(socket, peerQ):
    """
    Thread that connect subscriber socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSubscriber:
            log.debug(f"Connecting to seed {addr}:{port + 3}","Connect to Publishers")
            socket.connect(f"tcp://{addr}:{port + 3}")


def taskSubscriber(tasks, addr, port, peerQ):
    """
    Process that subscribe to published tasks
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.SUBSCRIBE, b"TASK")

    #why you dont connect manually
    connectT = Thread(target=connectToPublishers, name="Connect to Publishers", args=(socket, peerQ))
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
    #To not purge posible remote tasks received
    time.sleep(5)
    while True:
        with lockTasks:
            tmpTask = dict()
            tmpTask.update(tasks)
            log.debug("Starting purge", "Purger")
            for url in tmpTask:
                if tmpTask[url][0]:
                    tasks.pop(url)
        log.debug(f"Tasks after purge: {tasks}", "Purger")
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
            socket.send_json(("GET_TASKS",))
            response = socket.recv_json()
            log.debug(f"Tasks received", "Get Remote Tasks")
            assert isinstance(response, dict), f"Bad response, expected dict received {type(response)}"
            tasksQ.put(response)
            break
        except zmq.error.Again as e:
            log.debug(e, "Get Remote Tasks")
        except AssertionError as e:
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

        log.debug(f"Seed node created with address:{address}:{port}", pMainLog)


    def serve(self):
        """
        Start to attend clients.
        """
        seedsToConnect = [s for s in seeds]
        try:
            seedsToConnect.remove((self.addr, self.port))
        except ValueError:
            log.error(f"Unknown seed at {(self.addr, self.port)}", "Serve")
        sList = map(lambda x: f"{x[0]}:{x[1]}", seedsToConnect)
       
        tasksQ = Queue()
        pGetRemoteTasks = Process(target=getRemoteTasks, name="Get Remote Tasks", args=(sList, tasksQ))
        pGetRemoteTasks.start()
        tasks = tasksQ.get()
        pGetRemoteTasks.terminate()
        self.tasks = {} if tasks is None else tasks
        
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://{self.addr}:{self.port}")

        pushQ = Queue()
        pulledQ = Queue()
        resultQ = Queue()
        taskToPubQ = Queue()
        seedsQ = Queue()
        verificationQ = Queue()
        failedQ = Queue()

        #//HACK: When a new seed enter the system, his address and port must be inserted in seedsQ
        #//TODO: Use seeds in a way that a new seed can be added
        for s in seedsToConnect:
            seedsQ.put(s)

        pPush = Process(target=pushTask, name="Task Pusher", args=(pushQ, f"{self.addr}:{self.port + 1}"))
        pWorkerAttender = Process(target=workerAttender, name="Worker Attender", args=(pulledQ, resultQ, failedQ, f"{self.addr}:{self.port + 2}"))
        pTaskPublisher = Process(target=taskPublisher, name="Task Publisher", args=(f"{self.addr}:{self.port + 3}", taskToPubQ))
        pTaskSubscriber = Process(target=taskSubscriber, name="Task Subscriber", args=(self.tasks, self.addr, self.port, seedsQ))
        pVerifier = Process(target=verificator, name="Verificator", args=(verificationQ, 800, pushQ))

        taskManager1T = Thread(target=taskManager, name="Task Manager - PULLED", args=(self.tasks, pulledQ, taskToPubQ, True))
        taskManager2T = Thread(target=taskManager, name="Task Manager - DONE", args=(self.tasks, resultQ, taskToPubQ, True))
        taskManager3T = Thread(target=taskManager, name="Task Manager - FAILED", args=(self.tasks, failedQ, taskToPubQ, False))
        purgerT = Thread(target=purger, name="Purger", args=(self.tasks, 30))

        pPush.start()
        pWorkerAttender.start()
        pTaskPublisher.start()
        pTaskSubscriber.start()
        pVerifier.start()

        taskManager1T.start()
        taskManager2T.start()
        taskManager3T.start()
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
                            if not res[0]:
                                if isinstance(res[1], list):
                                    log.debug(f"Verificating {url} in the system...", "serve")
                                    verificationQ.put((res[1], url))
                                elif url == res[1]:
                                    raise KeyError
                        except KeyError:
                            res = self.tasks[url] = [False, "Pushed"]
                            pushQ.put(url)
                    socket.send_json(res)
                elif msg[0] == "GET_TASKS":
                    with lockTasks:
                        log.debug(f"GET_TASK received, sending tasks", "serve")
                        socket.send_json(self.tasks)
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
