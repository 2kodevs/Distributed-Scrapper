urls = [
    "http://www.cubadebate.cu/",
    "https://rpyc.readthedocs.io/en/latest/tutorial/tut3.html",
    "https://gobyexample.com/",
    "https://www.wikipedia.org/"
]

format  = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
datefmt ='%Y-%m-%d %H:%M:%S'

localhost = "127.0.0.1"
seeds = [(localhost, 8101)]

login = "DISTRIBUTED-SCRAPPER"

#//TODO: Add this description to README.md
"""

List of port usage:

Scrapper-Seed:
main_port(mp): to publish NEW_CLIENT
mp + 1:        to receive new clients

Dispacher:
mp:            to push tasks
mp + 1:        to receive results

"""