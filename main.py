#!/usr/bin/python3

import time
import logging
import settings
import crawler

if __name__ == '__main__':
    crawler.scan(max_limit=10)
