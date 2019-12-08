from util.colors import * 

format  = f'{BLUE}%(asctime)s{RESET} - %(color)s%(levelname)s{RESET} - {BLACKB}%(name)s{RESET} - {GREEN}%(method)s{RESET} - %(message)s'

datefmt ='%Y-%m-%d %H:%M:%S'

localhost = "127.0.0.1"

login = "DISTRIBUTED-SCRAPPER"

BROADCAST_PORT = 4142

"""
List of port usage:

Seed:
mp:            to attend clients
mp + 1:        to push tasks
mp + 2:        to attend scrappers
mp + 3:        to publish new tasks to seeds
"""