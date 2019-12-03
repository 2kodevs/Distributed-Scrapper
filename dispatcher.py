from bs4 import BeautifulSoup
from util.params import seeds
from util.colors import GREEN, RESET
from scrapy.http import HtmlResponse
from urllib.parse import urljoin, urlparse
from multiprocessing import Process, Queue
import zmq, time, os, mimetypes, pickle, json
from util.utils import parseLevel, makeUuid, LoggerFactory as Logger, noBlockREQ, valid_tags, change_html


log = Logger(name="Dispatcher")
    
    
def writer(root, url, old, data, name, graph):
    if url in old or url not in data:
        return
    old.add(url)
    
    url_name = name[url]
    with open(f'{root}/{url_name}', 'wb') as fd:
        fd.write(data[url])
        
    for next_url in graph[url]:
        writer(root, next_url, old, data, name, graph)

            
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
        
    
    def dispatch(self, queue):
        """
        Start to serve the Dispatcher.
        """
        context = zmq.Context()
        socket = noBlockREQ(context)

        #//TODO: Connect to seeds in a way that a new seed can be added
        for addr, port in seeds:
            socket.connect(f"tcp://{addr}:{port}")
            log.info(f"connected to {addr}:{port}", "dispatch")

        downloadsQ = Queue()
        pWriter = Process(target=downloadsWriter, args=(downloadsQ,))
        pWriter.start()

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
                    socket.send_json(("URL", url))
                    log.debug(f"send {url}", "dispatch")
                    response = pickle.loads(socket.recv())
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
            log.error(len(self.urls))
            
        
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

        downloadsQ.put("STOP")
        pWriter.join()
        queue.put(True)
        
        

def main(args):
    log.setLevel(parseLevel(args.level))
    
    urls = []
    try:
        assert os.path.exists(args.urls), "No URLs to request"
        with open(args.urls, 'r') as fd:
            urls = json.load(fd)
    except Exception as e:
        log.error(e, 'main')
    log.error(urls, args.urls)
    uuid = makeUuid(2**55, urls)
    d = Dispatcher(urls, uuid, args.address, args.port, args.depth)
    terminateQ = Queue()
    pDispatch = Process(target=d.dispatch, args=(terminateQ,))
    pDispatch.start()
    terminateQ.get()
    log.info(f"Dispatcher:{uuid} finish!!!", "main")
    pDispatch.terminate()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Client of a distibuted scrapper')
    parser.add_argument('-p', '--port', type=int, default=4142, help='connection port')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='node address')
    parser.add_argument('-l', '--level', type=str, default='DEBUG', help='log level')
    parser.add_argument('-d', '--depth', type=int, default=1, help='log level')
    parser.add_argument('-u', '--urls', type=str, default='urls', help='log level')

    #//TODO: use another arg to set the path to a file that contains the set of urls

    args = parser.parse_args()

    main(args)
