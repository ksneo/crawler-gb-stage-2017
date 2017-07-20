#!/usr/bin/python3

import time
import random
import multiprocessing as mp
import logging
import settings
import sqlite

import Robot from Robot
import Sitemap from sitemap
import Crawler from crawlers

def main():
    