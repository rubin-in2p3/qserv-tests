#!/usr/bin/env python
"""
Utility script to benchmark the cosmoDC2 database in qserv 
"""

import mysql
from mysql.connector import Error
from optparse import OptionParser
import pandas as pd

def qservInit(host, user, port):
	# Inititialize qserv connection
	conn = mysql.connector.connect(host=host, user=user, port=port)
	cursor = conn.cursor(dictionary=True, buffered=True)
	return conn, cursor

def listDB(conn, cursor):
	query = "SHOW DATABASES;"
	cursor.execute(query)
	res = cursor.fetchall()

	print(f'Found{len(res)} databases')
	print(item['Database'] for item in res)


def main():
    parser = OptionParser(usage="usage: %prog [options] input",
                          version="%prog 1.0")
    parser.add_option("-H", "--host",
                      type="string",
                      default="ccqserv201",
                      help="qserv host [%default]")
    parser.add_option("-u", "--user",
                      type="string",
                      default="qsmaster",
                      help="qserv user [%default]")
    parser.add_option("-p", "--port",
                      type="int",
                      default=30040,
                      help="qserv server port [%default]")
    parser.add_option("-D", "--database",
                      type="string",
                      default="cosmoDC2_v1_1_4_image",
                      help="qserv user [%default]")

    (opts, args) = parser.parse_args()
    if len(args) != 0:
        parse.error("Wrong number of arguments")

    conn, cursor = qservInit(opts.host, opts.user, opts.port)



if __name__ == '__main__':
    main()