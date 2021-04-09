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

Every node have a `make` rule to execute them with the defaults values, rules are:

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

### Run with Docker 🐳

Inside python-image folder there is a dockerfile for dependencies image. This image must be build from the `python: 3.7-slim` image. Run from the CLI `make python-docker-image` to build this image, then run `make project-image` to build the image for the project. This image is build with **scrapper** tag by default, if it is wanted to build the image with other tag, run `make project-image image_tag=<tag_name>`.

To run the image, execute from the CLI:

```
docker run -it --rm --network host \
-v <path>:/home/Distributed-Scrapper/downloads \
-v <urls>:/home/Distributed-Scrapper/urls \
2kodevs:<image_tag>
```

Where

- `<path>` it is the folder path where downloads are desired
- `<urls>` the path to the file where the set of URLs to be downloaded is located
- `<image_tag>` is the tag used when building the image

Another way is to run from the project's root:

`make run target=docker node="<node_name.py> <args>"`

`target=<target>` is for specify if a node it is wanted to be run using docker, skip it to not use docker.

### Note

The set of URLs to be downloaded must be of the form (it does not need a special extension):

```
[
    "<url_1>",
    "<url_2>",
       ...
    "<url_n>"
]
```

## Authors ✒️

- **Miguel Tenorio Potrony** -------> [AntiD2ta](https://github.com/AntiD2ta)
- **Lázaro Raúl Iglesias Vera** ----> [e1Ru1o](https://github.com/e1Ru1o)

## License 📄

This project is under the License (MIT License) - see the file [LICENSE.md](LICENSE.md) for details.
