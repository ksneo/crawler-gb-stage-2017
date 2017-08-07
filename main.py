#!/usr/bin/python3

import time
import random
from multiprocessing import Pool
import logging
import settings
# import sqlite3

# import Robot from Robot
# import sitemap
# from crawlers import Crawler
import crawlers

if __name__ == '__main__':
    crawlers.scan(max_limit=10)
    # c.scan()
    # c.fresh()
