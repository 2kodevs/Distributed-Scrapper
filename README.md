# Distributed-Scrapper
A simple distributed scrapper service that, given an initial set of URLs and a value referring to the depth of the download, returns the html of each URL and its group of dependencies necessary to be able to navigate without connection as much as the specified depth value.



## Starting

To use the project, clone it or download it to your local computer.



### Requirements 📋

It is necessary to have `python v-3.7.2` 

Dependencies:

- `zmq`
- `scrapy`
- `requests`
- `BeautifulSoup`

### Configure and run 🔧

To run the project, simply open the console from the root location of the project and run each of the nodes types, one or more of each.

Every node have a `make` rule to excecute them with the defaults values, rules are:

- `seed` for master node
- `worker` for scrapper node
- `dispatcher` for client node

If you want to use other parameters, run each node without using `make` directly executing its corresponding python file:
```
python <node>.py <params>
```
To know how to pass the parameters in each type of node use:
```
python <node>.py --help
```

## Authors ✒️

- **Miguel Tenorio Potrony** ------> [stdevAntiD2ta](https://github.com/stdevAntiD2ta)
- **Lázaro Raúl Iglesias Vera** ----> [stdevRulo](https://github.com/stdevRulo)

## License 📄

This project is under the License (MIT License) - see the file [LICENSE.md](LICENSE.md) for details.
