#!/usr/bin/python3
# import sqlite3 as sq
import MySQLdb
"""db_connector
    Пути"""
# DB = sq.connect('crawler.db')
# DB=MySQLdb.connect(host='127.0.0.1', user='andrewisakov', password='thua8Ohj', db='dbwpmod')
DB=MySQLdb.connect(
    host='127.0.0.1', user='andrewisakov', password='thua8Ohj',
    db='dbwpmod', use_unicode=True, charset='utf8')

POOL_SIZE = 4 # Количество потоков multiprocessing.pool
CHUNK_SIZE = 64 # Запись в БД страницами чере multiprocessing.pool
