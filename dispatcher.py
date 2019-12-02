from bs4 import BeautifulSoup
from util.params import seeds
from util.colors import GREEN, RESET
from scrapy.http import HtmlResponse
from urllib.parse import urljoin, urlparse
from multiprocessing import Process, Queue
import zmq, time, os, mimetypes, pickle, json
from util.utils import parseLevel, makeUuid, LoggerFactory as Logger, noBlockREQ, valid_tags


log = Logger(name="Dispatcher")


def downloadsWriter(queue):
    for index, url, data in iter(queue.get, "STOP"):
        with open(f"downloads/html{index}", "w") as fd:
            log.info(f"{url} saved")
            fd.write(data)
    log.debug("All data saved")
    
    
def writer(root, url, depth, old, data, name, graph):
    if url in old:
        return
    old.add(url)
    
    url_name = name[url]
    with open(f'{root}/{url_name}', 'wb') as fd:
        fd.write(data[url])
        
    if depth > 1:
        for next_url in graph[url]:
            writer(root, next_url, depth - 1, old, data, name, graph)

            
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
        while True:
            # idx = {url: i for i, url in enumerate(self.urls)}
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
                        #downloadsQ.put((idx[url], url, html))
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
            
            for url, (html, content) in new_data.items():
                graph[url] = set()
                try:
                    if ';' in content:
                        content = content[:content.index(';')]
                    if '.html' in mimetypes.guess_all_extensions(content):
                        soup = BeautifulSoup(html, 'html.parser')
                        tags = soup.find_all(valid_tags)
                        new_urls = [(tag['href'] if tag.has_attr('href') else tag['src']) for tag in tags]
                        full_urls = [urljoin(url, other) for other in new_urls]
                        for i, url_dir in enumerate(full_urls):
                            if url_dir not in url_mapper:
                                url_mapper[url_dir] = f'url_{len(url_mapper)}'
                            if tags[i].has_attr('href'):
                                tags[i]['href'] = url_mapper[url_dir]
                            else:
                                tags[i]['src'] = url_mapper[url_dir]
                        graph[url].update(full_urls)
                        self.urls.extend(full_urls)
                        html = soup.html.encode()
                except:
                    pass
                new_data[url] = html
            data.update(new_data)
            self.urls = set(self.urls)
            self.urls.difference_update(self.old)
            self.old.update(self.urls)
            self.urls = list(self.urls)
            log.error(len(self.urls))
            
            if depth == self.depth:
                break
            depth += 1
            
        
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
            writer(f'downloads/{base}-data', url, self.depth, set(), data, url_mapper, graph)   
            
            html = data[url]
            if len(graph[url]) > 0:
                soup = BeautifulSoup(html, 'html.parser')
                tags = soup.find_all(valid_tags)
                for tag in tags:
                    if tag.has_attr('href'):
                        tag['href'] = f'{base}-data/{tag["href"]}'
                    else:
                        tag['src'] = f'{base}-data/{tag["src"]}'
                html = soup.html.encode()
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
    if os.path.exists(args.urls):
        with open(args.urls, 'r') as fd:
            urls = json.load(fd)
    else:
        log.info("No URLs to request", 'main')
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
