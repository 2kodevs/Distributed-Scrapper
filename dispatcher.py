import zmq, time, os, json, re
from bs4 import BeautifulSoup
from util.colors import GREEN, RESET
from scrapy.http import HtmlResponse
from urllib.parse import urljoin, urlparse
from multiprocessing import Process, Queue
from util.utils import parseLevel, makeUuid, LoggerFactory as Logger, noBlockREQ, discoverPeer, getSeeds, findSeeds, valid_tags, change_html
from threading import Thread, Lock as tLock, Semaphore


log = Logger(name="Dispatcher")

lockSocketReq = tLock()
counterSocketReq = Semaphore(value=0)
    
    
def writer(root, url, old, data, name, graph):
    """
    Write all the files on which <url> depends
    on the <root> folder, taking their contents
    from <data> and its name from <name>.
    The <graph> dependency tree is traversed while 
    there are dependencies that are not found in <old>
    """
    if url in old or url not in data:
        return
    old.add(url)
    
    url_name = name[url]
    with open(f'{root}/{url_name}', 'wb') as fd:
        fd.write(data[url])
        
    for next_url in graph[url]:
        writer(root, next_url, old, data, name, graph)


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


def getSeedFromFile(peerQ, extraQ):
    """
    Process that gets an address of a seed node by standart input.
    """
    #Creating seed.txt
    log.debug("Creating network.txt...", "Get seed from file")
    newFile = open("network.txt", "w")
    newFile.close()

    while True:
        #get input
        with open("network.txt", "r") as f:
            s = f.read()
            if s == "":
                time.sleep(1)
                continue
            log.info(f"Get \"{s}\" from network.txt", "Get seed from file")
        with open("network.txt", "w") as f:
            f.write("")

        #ip_address:port_number
        regex = re.compile("\d{,3}\.\d{,3}\.\d{,3}\.\d{,3}:\d+")
        try:
            assert regex.match(s).end() == len(s)
            addr, port = s.split(":")
            peerQ.put((addr, int(port)))
            extraQ.put((addr, int(port)))
        except (AssertionError, AttributeError):
            log.error(f"Parameter seed inserted is not a valid ip_address:port_number", "Get seed from file")
            seed = None


class Dispatcher:
    """
    Represents a client to the services of the Scrapper.
    """
    def __init__(self, urls, uuid, address="127.0.0.1", port=4142, depth=1):
        self.depth = depth
        self.originals = set(urls)
        self.urls = list(self.originals)
        self.old = {url for url in self.originals}
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
        
        seedsQ1 = Queue()
        seedsQ2 = Queue()
        for address in self.seeds:
            seedsQ1.put(address)

        connectT = Thread(target=connectToSeeds, name="Connect to Seeds", args=(socket, seedsQ1))
        connectT.start()

        toDisconnectQ = Queue()
        disconnectT = Thread(target=disconnectToSeeds, name="Disconnect to Seeds", args=(socket, toDisconnectQ))
        disconnectT.start()

        pFindSeeds = Process(target=findSeeds, name="Find Seeds", args=(set(self.seeds), [seedsQ1], [toDisconnectQ], log, 2000, 10, seedsQ2))
        pFindSeeds.start()

        pInput = Process(target=getSeedFromFile, name="Get seed from file", args=(seedsQ1, seedsQ2))
        pInput.start()

        graph = {}
        depth = 1
        data = {}
        url_mapper = {url:f"url_{i}" for i, url in enumerate(self.urls)}
        
        src = set()
        while True:   
            new_data = {}
            while len(self.urls):
                try:
                    url = self.urls[0]
                    with counterSocketReq:
                        socket.send_json(("URL", url))
                        log.debug(f"send {url}", "dispatch")
                        response = socket.recv_pyobj()
                    assert len(response) == 2, "bad response size"
                    download, html = response
                    log.debug(f"Received {download}", "dispatch")
                    self.urls.pop(0)
                    if download:
                        log.info(f"{url} {GREEN}OK{RESET}", "dispatch")
                        new_data[url] = html
                    else:
                        self.urls.append(url)
                except AssertionError as e:
                    log.error(e, "dispatch")
                except zmq.error.Again as e:
                    log.debug(e, "dispatch")
                except Exception as e:
                    log.error(e, "dispatch")
                    seeds.append(seeds.pop(0))  
                time.sleep(1)
                 
            log.info(f'Depth {depth} done', 'dispatch')
            for url, html in new_data.items():
                graph[url] = set()
                try:
                    text = html.decode()
                    soup = BeautifulSoup(html, 'html.parser')
                    tags = soup.find_all(valid_tags)
                    new_urls = [['src', 'href'][tag.has_attr('href')] for tag in tags]
                    changes = []
                    for i, attr in enumerate(new_urls):
                        url_dir = urljoin(url, tags[i][attr])
                        graph[url].add(url_dir)
                        if url_dir not in url_mapper:
                            url_mapper[url_dir] = f'url_{len(url_mapper)}'
                        changes.append((tags[i][attr], url_mapper[url_dir]))
                        if attr == 'src' or tags[i].name == 'link':
                            src.add(url_dir)
                            continue
                        self.urls.append(url_dir)
                    html = change_html(text, changes).encode()
                except UnicodeDecodeError:
                    log.debug(f'{url} is not decodeable', 'dispatch')
                except: # BeautifulSoup strange exceptions related with it's logger
                    pass
                new_data[url] = html
            data.update(new_data)
            self.urls = set(self.urls)
            self.urls.difference_update(self.old)
            self.old.update(self.urls)
            self.urls = list(self.urls)
            
            if depth > self.depth:
                break
            if depth == self.depth:
                src.difference_update(self.old)
                self.old.update(src)
                self.urls = list(src)
            depth += 1
            log.info(f"Number of URLs to be requested for download: {len(self.urls)}", "dispatch")
            
        log.info(f"Starting to write data", "dispatch")
        for i, url in enumerate(self.originals):
            try:
                res = HtmlResponse(url=url, body=data[url], encoding='utf8')
                base = res.css('title::text')[0].get()
            except:
                base = f"web_page_{i}"
            try:
                os.makedirs(f'downloads/{base}-data')
            except:
                pass
            writer(f'downloads/{base}-data', url, set(), data, url_mapper, graph)   
            
            html = data[url]
            if len(graph[url]) > 0:
                text = data[url].decode()
                changes = []
                for dep in graph[url]:
                    name = url_mapper[dep]
                    changes.append((name, f'{base}-data/{name}'))
                html = change_html(text, changes).encode()
            with open(f'downloads/{base}', 'wb') as fd:
                fd.write(html)
            
        log.info(f"Dispatcher:{self.uuid} has completed his URLs succefully", "dispatch")
        log.debug(f"Dispatcher:{self.uuid} disconnecting from system", "dispatch")
        #disconnect

        pWriter.join()
        queue.put(True)
        pFindSeeds.terminate()
        
        
def main(args):
    log.setLevel(parseLevel(args.level))
    
    urls = []
    try:
        assert os.path.exists(args.urls), "No URLs to request"
        with open(args.urls, 'r') as fd:
            urls = json.load(fd)
    except Exception as e:
        log.error(e, 'main')
        
    log.info(urls, "main")
    uuid = makeUuid(2**55, urls)
    d = Dispatcher(urls, uuid, args.address, args.port, args.depth)
    seed = args.seed
    
    while not d.login(seed):
        log.info("Enter an address of an existing seed node. Insert as ip_address:port_number. Press ENTER if you want to omit this address. Press q if you want to exit the program")
        seed = input("-->")
        if seed == '':
            continue
        seed = seed.split()[0]
        if seed == 'q':
            break
        
    if seed != 'q':
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
    parser.add_argument('-d', '--depth', type=int, default=1, help='depth of recursive downloads')
    parser.add_argument('-u', '--urls', type=str, default='urls', help='path of file that contains the urls set')
    parser.add_argument('-s', '--seed', type=str, default=None, help='address of an existing seed node. Insert as ip_address:port_number')


    #//TODO: use another arg to set the path to a file that contains the set of urls

    args = parser.parse_args()

    main(args)
