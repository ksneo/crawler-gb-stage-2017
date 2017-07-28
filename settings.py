#!/usr/bin/python3
# import sqlite3 as sq
import MySQLdb
"""db_connector
    Пути"""
# DB = sq.connect('crawler.db')
DB=MySQLdb.connect(
    host='127.0.0.1',user='andrewisakov',password='thua8Ohj',
    db='dbwpmod',use_unicode=True, charset='utf8')
