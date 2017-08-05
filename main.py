#!/usr/bin/python3

import time
import random
from multiprocessing import Pool
import logging
import asyncio
import settings
# import sqlite3

# import Robot from Robot
# import sitemap
# from crawlers import Crawler
import crawlers

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawlers.scan(loop, 10))
    # c.scan()
    # c.fresh()
