from util.colors import * 

urls = [
    "http://www.cubadebate.cu",
    "http://www.bohemia.cu",
    "http://www.juventudrebelde.cu"
]

format  = f'{BLUEB}%(asctime)s{RESET} - {RED}%(levelname)s{RESET} - {GREEN}%(name)s{RESET} - %(message)s'
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