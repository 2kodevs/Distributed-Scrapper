from util.colors import * 

urls = [
    "http://www.cubadebate.cu",
    "http://www.bohemia.cu",
    "http://www.juventudrebelde.cu"
]

format  = f'{BLUE}%(asctime)s{RESET} - %(color)s%(levelname)s{RESET} - {BLACKB}%(name)s{RESET} - {GREEN}%(method)s{RESET} - %(message)s'

datefmt ='%Y-%m-%d %H:%M:%S'

localhost = "127.0.0.1"
seeds = [(localhost, 8101), (localhost, 9000)]

login = "DISTRIBUTED-SCRAPPER"

BROADCAST_PORT = 4142

#//TODO: Add this description to README.md
"""

List of port usage:

Seed:
mp:            to attend clients
mp + 1:        to push tasks
mp + 2:        to attend scrappers
mp + 3:        to publish new tasks to seeds
"""