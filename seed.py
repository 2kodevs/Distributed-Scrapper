import zmq, time, queue, pickle, re, random
from util.conit import Conit
from multiprocessing import Process, Queue
from threading import Thread, Lock as tLock
from util.params import login, BROADCAST_PORT
from util.utils import parseLevel, LoggerFactory as Logger, noBlockREQ, discoverPeer, getSeeds, clock
from socket import socket, AF_INET, SOCK_DGRAM


log = Logger(name="Seed")

lockTasks = tLock()
lockSubscriber = tLock()
lockSeeds = tLock()
lockOwners = tLock()
lockClients = tLock()


def verificator(queue, t, pushQ):
    """
    Process that manage the list of workers who should be verified. 
    """
    for address, url in iter(queue.get, "STOP"):
        ansQ = Queue()
        pQuick = Process(target=quickVerification, args=(address, url, t, ansQ))
        pQuick.start()
        ans = ansQ.get()
        pQuick.terminate()
        if not ans:
            pushQ.put(url)
            

def quickVerification(address, url, t, queue):
    """
    Process that send a verification message to a worker 
    to find out if he is working and report it.
    """
    context = zmq.Context()
    sock = noBlockREQ(context, timeout=t)
    ans = False
    try:
        addr, port = address
        sock.connect(f"tcp://{addr}:{port}")
        log.debug(f"Sending quick verification to {addr}:{port}", "Quick Verification")
        sock.send(url.encode())
        ans = sock.recv_json()
        log.debug(f"Worker at {address} is alive. Is working on {url}: {ans}", "Quick Verification")
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
    sock = context.socket(zmq.PUSH)
    sock.bind(f"tcp://{addr}")
    
    for url in iter(toPushQ.get, "STOP"):
        log.debug(f"Pushing {url}", "Task Pusher")
        sock.send(url.encode())


def workerAttender(pulledQ, resultQ, failedQ, addr):
    """
    Process that listen notifications from workers.
    """
    context = zmq.Context()
    sock = context.socket(zmq.REP)
    sock.bind(f"tcp://{addr}")

    while True:
        try:
            msg = sock.recv_pyobj()
            if msg[0] == "PULLED":
                log.info(f"Message received: {msg}", "Worker Attender")
                #msg = PULLED, url, workerAddr
                pulledQ.put((False, msg[1], msg[2]))
            elif msg[0] == "DONE":
                log.info(f"Message received: ({msg[0]}, {msg[1]})", "Worker Attender")
                #msg = DONE, url, html
                resultQ.put((True, msg[1], msg[2]))
            elif msg[0] == "FAILED":
                log.info(f"Message received: {msg}", "Worker Attender")
                #msg = FAILED, url, timesAttempted
                failedQ.put((False, msg[1], msg[1]))

            #nothing important to send
            sock.send(b"OK")  
        except Exception as e:
            #Handle connection error
            log.error(e, "Worker Attender")
            continue


def getData(url, address, owners, resultQ, removeQ):
    """
    Process that make a NOBLOCK request to know owners
    of url's data.
    """
    context = zmq.Context()
    sock = noBlockREQ(context, timeout=1000)

    random.shuffle(owners)
    for o in owners:
        sock.connect(f"tcp://{o[0]}:{o[1]}")
        try:
            log.info(f"Requesting data to seed: {o}", "Get Data")
            sock.send_json(("GET_DATA", url))
            ans = sock.recv_pyobj()
            if ans == False:
                #rare case that 'o' don't have the data
                removeQ.put((o, url))
                continue
            #ans: (data, lives)
            resultQ.put(ans)
            break
        except zmq.error.Again as e:
            log.debug(e, "Get Data")
            removeQ.put((o, url))
        except Exception as e:
            log.error(e, "Get Data")
        finally:
            sock.disconnect(f"tcp://{o[0]}:{o[1]}")
    resultQ.put(False)        
        

def conitCreator(tasks, address, resultQ, toPubQ, request, package):
    """
    Thread that manage conit creation.
    """
    while True:
        flag, url, data = resultQ.get()
        with lockTasks:
            if flag:
                #it comes from workerAttender
                if url in tasks and tasks[url][0]:
                    if tasks[url][1].data is not None:
                        #I have an old copy, update data
                        log.debug(f"Updating data with url: {url}", "Conit Creator")
                        cnit = tasks[url][1]
                        cnit.updateData(data)
                        #UPDATE: call your conit's updateData with data
                        toPubQ.put((flag, url, ("UPDATE", data)))
                    else:
                        #I have the list of owners, but force replication of data, this is a rare case
                        log.debug(f"Forcing to save data with url: {url}", "Conit Creator")
                        cnit = tasks[url][1]
                        cnit.updateData(data)
                        cnit.addOwner(address)
                        #FORCED: call your conit's updateData with data (in case of having a replica)
                        #and update owners
                        toPubQ.put((flag, url, ("FORCED", (data, address))))
                else:
                    #it seems that nobody have it. Save data
                    log.debug(f"Saving data with url: {url}", "Conit Creator")
                    #//TODO: Parameterize param limit of Conit constructor
                    cnit = Conit(data, owners=[address])
                    tasks[url] = (True, cnit)
                    #NEW_DATA: update owners of url's data with address
                    toPubQ.put((flag, url, ("NEW_DATA", address)))
            else:
                #It comes from dispatch, Replicate data
                log.debug(f"Replicating data of {url}...", "Conit Creator")
                tasks[url][1].updateData(data[0], data[1])
                data = data[0]
                tasks[url][1].addOwner(address)
                toPubQ.put((flag, url, ("NEW_DATA", address)))
            with lockClients:
                if url in request:
                    for id in request[url]:
                        log.debug(f"Adding {url} to {id}", 'Conit Creator')
                        package[id][url] = data
                    

def removeOwner(tasks, removeQ, toPubQ):
    """
    Thread that remove owner from all conits that have it.
    """
    while True:
        o, url = removeQ.get()
        with lockTasks:
            if url in tasks:
                tasks[url][1].removeOwner(o)
                log.debug(f"Owner {o} removed from conits", "Remove Owner")
            toPubQ.put(("REMOVE", o))


def updateOwners(owners, conit, owner):
    """
    Helper function that update owners dict.
    """
    try:
        owners[owner].add(conit)
    except KeyError:
        owners[owner] = {conit}


def resourceManager(owners, tasks, dataQ):
    """
    Thread that manage publications of seed nodes
    related to downloaded data.
    """
    while True:
        header, task = dataQ.get()
        url, data = task
        with lockTasks:
            try:
                if not tasks[url][0]:
                    raise KeyError 
                if header == "FORCED":
                    if tasks[url][1].data is not None:
                        tasks[url][1].updateData(data[0])
                    tasks[url][1].addOwner(data[1])
                    with lockOwners:
                        updateOwners(owners, tasks[url][1], data[1])
                elif header == "UPDATE":
                    log.debug(f"Updating data of {url}...", "Resource Manager")
                    tasks[url][1].updateData(data)
                elif header == "NEW_DATA":
                    log.debug(f"Updating owners of {url}...", "Resource Manager")
                    tasks[url][1].addOwner(data)
                    with lockOwners:
                        updateOwners(owners, tasks[url][1], data)
            except KeyError:
                if header == "FORCED":
                    tasks[url] = (True, Conit(None, [data[1]]))
                else:
                    tasks[url] = (True, Conit(None, [data]))


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


def seedManager(seeds, q):
    """
    Thread that helps the seed main process to update the seeds list.
    """
    while True:
        cmd, address = q.get()
        with lockSeeds:
            if cmd == "APPEND":
                seeds.append(address)
            elif cmd == "REMOVE":
                with lockSeeds:
                    seeds.remove(address)


def taskPublisher(addr, taskQ):
    """
    Process that publish tasks changes to others seed nodes.
    """
    context = zmq.Context()
    sock = context.socket(zmq.PUB)
    sock.bind(f"tcp://{addr}")

    while True:
        try:
            #task: (flag, url, data)
            task = taskQ.get()
            if isinstance(task[0], bool):
                if isinstance(task[2][1], int):
                    log.debug(f"Publish pulled task: ({task[0]}, {task[1]})", "Task Publisher")
                    sock.send_multipart([b"PULLED_TASK", pickle.dumps(task)])
                    continue

                header = task[2][0]
                log.debug(f"Publish {header} of {task[1]}", "Task Publisher")
                sock.send_multipart([header.encode(), pickle.dumps((task[1], task[2][1]))])
            elif task[0] == "PURGE":
                log.debug(f"Publish PURGE", "Task Publisher")
                sock.send_multipart([b"PURGE", b"JUNK"])
            elif task[0] == "REMOVE":
                log.debug(f"Publish REMOVE", "Task Publisher")
                sock.send_multipart([b"REMOVE", pickle.dumps(task[1])])
            else:
                log.debug(f"Publish seed: ({task[0]}:{task[1]})", "Task Publisher")
                sock.send_multipart([b"NEW_SEED", pickle.dumps(task)])
        except Exception as e:
            log.error(e, "Task Publisher")


def connectToPublishers(sock, peerQ):
    """
    Thread that connect subscriber socket to seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSubscriber:
            log.debug(f"Connecting to seed {addr}:{port + 3}","Connect to Publishers")
            sock.connect(f"tcp://{addr}:{port + 3}")


def disconnectFromPublishers(sock, peerQ):
    """
    Thread that disconnect subscriber socket from seeds.
    """
    for addr, port in iter(peerQ.get, "STOP"):
        with lockSubscriber:
            log.debug(f"Disconnecting from seed {addr}:{port + 3}","Disconnect from Publishers")
            sock.disconnect(f"tcp://{addr}:{port + 3}")


def taskSubscriber(peerQ, disconnectQ, taskQ, seedQ, dataQ, purgeQ):
    """
    Process that subscribe to published tasks
    """
    context = zmq.Context()
    sock = context.socket(zmq.SUB)
    sock.setsockopt(zmq.SUBSCRIBE, b"PULLED_TASK")
    sock.setsockopt(zmq.SUBSCRIBE, b"NEW_SEED")
    sock.setsockopt(zmq.SUBSCRIBE, b"UPDATE")
    sock.setsockopt(zmq.SUBSCRIBE, b"NEW_DATA")
    sock.setsockopt(zmq.SUBSCRIBE, b"FORCED")
    sock.setsockopt(zmq.SUBSCRIBE, b"REMOVE")
    sock.setsockopt(zmq.SUBSCRIBE, b"PURGE")

    connectT = Thread(target=connectToPublishers, name="Connect to Publishers", args=(sock, peerQ))
    connectT.start()

    disconnectT = Thread(target=disconnectFromPublishers, name="Disconnect from Publishers", args=(sock, disconnectQ))
    disconnectT.start()

    time.sleep(1)

    while True:
        try:
            with lockSubscriber:
                header, task = sock.recv_multipart()
                header = header.decode()
                log.debug(f"Received Subscribed message: {header}", "Task Subscriber")
                if header == "PULLED_TASK":
                    #task: (flag, url, data)
                    flag, url, data = pickle.loads(task)
                    taskQ.put((flag, url, data))
                elif header == "APPEND":
                    #task: (address, port)
                    addr, port = pickle.loads(task)
                    seedQ.put(("APPEND", (addr, port)))
                    peerQ.put((addr, port))
                elif header == "REMOVE":
                    #task: (address, port)
                    addr, port = pickle.loads(task)
                    seedQ.put(("REMOVE", (addr, port)))
                    disconnectQ.put((addr, port))
                elif header == "PURGE":
                    purgeQ.put(False)
                else:
                    #header: UPDATE, NEW_DATA, FORCED
                    #task: (url, data)
                    task = pickle.loads(task)
                    dataQ.put((header, task))
        except Exception as e:
            log.error(e, "Task Subscriber")


def purger(tasks, address, cycle, toPubQ, purgeQ):
    """
    Thread that purge the downloaded data from tasks map when a time cycle occurs.
    """
    while True:
        pClock = Process(target=clock, args=(cycle, purgeQ))
        pClock.start()
        pub = purgeQ.get()
        pClock.terminate()
        with lockTasks:
            log.debug("Starting purge", "Purger")
            for url, value in tasks.items():
                if value[0]:
                    if value[1].data is not None and value[1].isRemovable():
                        value[1].data = None
                        value[1].removeOwner(address)
                    value[1].addLive()
        log.debug(f"Tasks after purge: {tasks}", "Purger")
        log.debug("Purge finished", "Purger")
        if pub:
            toPubQ.put(("PURGE",))


def getRemoteTasks(seed, tasksQ):
    """
    Process that ask to other seed for his tasks.
    """
    context = zmq.Context()
    sock = noBlockREQ(context, timeout=1000)

    sock.connect(f"tcp://{seed}")

    for _ in range(2):
        try:
            sock.send_json(("GET_TASKS",))
            response = sock.recv_pyobj()
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
    sock.close()
    tasksQ.put(None)


def broadcastListener(addr, port):
    """
    Process that reply broadcast messages from other peers.
    It not works offline.
    """
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.bind(('', port))

    while True:
        #address = (ip, port)
        data, address = sock.recvfrom(4096)
        data = str(data.decode('UTF-8'))
        log.debug(f"Received {str(len(data))} bytes from {str(address)}", "Broadcast Listener")
        log.debug(f"Data: {data}", "Broadcast Listener")
        
        if data == login:
            #addr = (addr, port)
            sock.sendto(pickle.dumps(("WELCOME", addr)), address)


def cloneTasks(tasks:dict):
    """
    Helper function that makes a lite copy of tasks, without heavy conits.
    """
    liteTasks = dict()
    for key, value in tasks.items():
        if value[0]:
            liteTasks[key] = (value[0], value[1].copy())
        else:
            liteTasks[key] = value
    log.debug("Lite copy of tasks created", "cloneTasks")
    return liteTasks


class Seed:
    """
    Represents a seed node, the node that receive and attend all client request.
    """
    def __init__(self, address, port, repLimit):
        self.addr = address
        self.port = port
        self.repLimit = repLimit
        self.seeds = [(address, port)]
        self.owners = dict()
        self.package = dict()
        self.request = dict()

        log.debug(f"Seed node created with address:{address}:{port}", "main")


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
            seed, network = discoverPeer(3, log)
            log.debug(f"Seed founded: {seed}", "login")
            if seed == "":
                self.tasks = {}
                log.info("Login finished", "login")
                return network

        seedsQ = Queue()
        pGetSeeds = Process(target=getSeeds, name="Get Seeds", args=(seed, discoverPeer, (self.addr, self.port), True, seedsQ, log))
        pGetSeeds.start()
        tmp = seedsQ.get()
        #If Get Seeds fails to connect to a seed for some reason
        if tmp is not None:
            self.seeds.extend(tmp)
        pGetSeeds.terminate()

        tasksQ = Queue()
        pGetRemoteTasks = Process(target=getRemoteTasks, name="Get Remote Tasks", args=(seed, tasksQ))
        pGetRemoteTasks.start()
        tasks = tasksQ.get()
        pGetRemoteTasks.terminate()
        self.tasks = {} if tasks is None else tasks
        log.info("Login finished", "login")
        return network


    def serve(self, broadcastPort):
        """
        Start to attend clients.
        """
        
        context = zmq.Context()
        sock = context.socket(zmq.REP)
        sock.bind(f"tcp://{self.addr}:{self.port}")

        pushQ = Queue()
        pulledQ = Queue()
        resultQ = Queue()
        taskToPubQ = Queue()
        seedsQ = Queue()
        disconnectQ = Queue()
        verificationQ = Queue()
        failedQ = Queue()
        newSeedsQ = Queue()
        removeQ = Queue()
        dataQ = Queue()
        purgeQ = Queue()

        tmp = self.seeds.copy()
        tmp.remove((self.addr, self.port))
        for s in tmp:
            seedsQ.put(s)

        pPush = Process(target=pushTask, name="Task Pusher", args=(pushQ, f"{self.addr}:{self.port + 1}"))
        pWorkerAttender = Process(target=workerAttender, name="Worker Attender", args=(pulledQ, resultQ, failedQ, f"{self.addr}:{self.port + 2}"))
        pTaskPublisher = Process(target=taskPublisher, name="Task Publisher", args=(f"{self.addr}:{self.port + 3}", taskToPubQ))
        pTaskSubscriber = Process(target=taskSubscriber, name="Task Subscriber", args=(seedsQ, disconnectQ, failedQ, newSeedsQ, dataQ, purgeQ))
        pVerifier = Process(target=verificator, name="Verificator", args=(verificationQ, 500, pushQ))
        pListener = Process(target=broadcastListener, name="Broadcast Listener", args=((self.addr, self.port), broadcastPort))

        taskManager1T = Thread(target=taskManager, name="Task Manager - PULLED", args=(self.tasks, pulledQ, taskToPubQ, True))
        taskManager2T = Thread(target=taskManager, name="Task Manager - FAILED", args=(self.tasks, failedQ, taskToPubQ, False))
        seedManagerT = Thread(target=seedManager, name="Seed Manager", args=(self.seeds, newSeedsQ))
        resourceManagerT = Thread(target=resourceManager, name="Resource Manager", args=(self.owners, self.tasks, dataQ))
        conitCreatorT = Thread(target=conitCreator, name="Conit Creator", args=(self.tasks, (self.addr, self.port), resultQ, taskToPubQ, self.request, self.package))
        removeOwnerT = Thread(target=removeOwner, name="Remove Owner", args=(self.owners, removeQ, taskToPubQ))
        purgerT = Thread(target=purger, name="Purger", args=(self.tasks, (self.addr, self.port), 1200, taskToPubQ, purgeQ)) #20 minutes

        pPush.start()
        pWorkerAttender.start()
        pTaskPublisher.start()
        pTaskSubscriber.start()
        pVerifier.start()
        pListener.start()

        taskManager1T.start()
        taskManager2T.start()
        seedManagerT.start()
        resourceManagerT.start()
        conitCreatorT.start()
        removeOwnerT.start()
        purgerT.start()

        time.sleep(0.5)

        log.info("Starting to serve...", "serve")
        while True:
            try:
                msg = sock.recv_json()
                if msg[0] == "URL":
                    _, id, url = msg
                    with lockClients:
                        if url not in self.request:
                            self.request[url] = set()
                        self.request[url].add(id)
                        if id not in self.package:
                            self.package[id] = dict()
                    with lockTasks:
                        try:
                            res = self.tasks[url]
                            if not res[0]:
                                if isinstance(res[1], tuple):
                                    #log.debug(f"Verificating {url} in the system...", "serve")
                                    verificationQ.put((res[1], url))
                                elif url == res[1]:
                                    raise KeyError
                                else:
                                    self.tasks[url][1] += 1
                                    if self.tasks[url][1] == 10:
                                        raise KeyError
                            else:
                                if res[1].data == None:
                                    #i don't have a local replica, ask owners
                                    getDataQ = Queue()                      
                                    pGetData = Process(target=getData, name="Get Data", args=(url, (self.addr, self.port), res[1].owners, getDataQ, removeQ))
                                    pGetData.start()
                                    data = getDataQ.get()
                                    pGetData.terminate()
                                    if data:
                                        #data: (data, lives)
                                        log.debug(f"Hit on {url}. Total hits: {res[1].hits + 1}", "serve")
                                        res = (True, data[0])
                                        with lockClients:
                                            self.package[id][url] = data[0]
                                        if res[1].hit() and res[1].tryOwn(self.repLimit):
                                            #replicate
                                            log.debug(f"Replicating data of {url}", "serve")
                                            resultQ.put((False, url, data))
                                    else:
                                        #nobody seems to have the data
                                        raise KeyError        
                                else:
                                    #I have a local replica
                                    with lockClients:
                                        self.package[id][url] = res[1].data
                                    res = (True, res[1].data)
                        except KeyError:
                            res = self.tasks[url] = [False, 0]
                            pushQ.put(url)
                    with lockClients:
                        res = ("RESPONSE", self.package[id])
                        log.debug(f"Sending package of size {len(res[1])}", "serve")
                        sock.send_pyobj(res)
                        self.package[id].clear()
                elif msg[0] == "GET_TASKS":
                    with lockTasks:
                        log.debug("GET_TASK received, sending tasks", "serve")
                        sock.send_pyobj(cloneTasks(self.tasks))
                elif msg[0] == "NEW_SEED":
                    log.debug("NEW_SEED received, saving new seed...")
                    #addr = (address, port)
                    addr = tuple(msg[1])
                    with lockSeeds:
                        if addr not in self.seeds:
                            self.seeds.append(addr)
                            seedsQ.put(addr)
                            taskToPubQ.put(addr)
                    sock.send_json("OK")
                elif msg[0] == "GET_SEEDS":
                    log.debug("GET_SEEDS received, sending seeds", "serve")
                    with lockSeeds:
                        log.debug(f"Seeds: {self.seeds}", "serve")
                        sock.send_pyobj(self.seeds)
                elif msg[0] == "PING":
                    log.debug("PING received", "serve")
                    sock.send_json("OK")
                elif msg[0] == "GET_DATA":
                    log.debug("GET_DATA received", "serve")
                    try:
                        rep = False
                        if self.tasks[msg[1]][0] and self.tasks[msg[1]][1].data is not None:
                            rep = (self.tasks[msg[1]][1].data, self.tasks[msg[1]][1].lives) 
                    except KeyError:
                        pass
                    sock.send_pyobj(rep)
                else:
                    sock.send_pyobj("UNKNOWN")      
            except Exception as e:
                #Handle connection error
                log.error(e, "serve")
                time.sleep(5)
            

def main(args):
    log.setLevel(parseLevel(args.level))
    s = Seed(args.address, args.port, args.replication_limit)
    if not s.login(args.seed):
        log.info("You are not connected to a network", "main") 
    s.serve(args.broadcast_port)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-p', '--port', type=int, default=8101, help='connection port')
    parser.add_argument('-b', '--broadcast_port', type=int, default=BROADCAST_PORT, help='broadcast listener port (Default: 4142)')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')
    parser.add_argument('-s', '--seed', type=str, default=None, help='address of a existing seed node. Insert as ip_address:port_number')
    parser.add_argument('-r', '--replication_limit', type=int, default=2, help='maximum number of times that you want data to be replicated')

    args = parser.parse_args()

    main(args)
