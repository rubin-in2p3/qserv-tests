#!/usr/bin/env python
"""
Utility script to benchmark the cosmoDC2 database in qserv 
"""

import os
import time
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

	print(f'found {len(res)} databases on the qserv cluster')
	print([item['Database'] for item in res])

def countObjects(conn, cursor, db):
	query = f"SELECT COUNT(*) FROM {db}.position;"
	cursor.execute(query)
	res = cursor.fetchall()

	print(f"{res[0]['COUNT(*)']} entries found - Should be 2256249331" )

def fullScan_1(conn):
	# Simple query to select galaxy clusters

	mmin = 5.e14 #Msun
	zmax = 2

	query = "SELECT data.coord_ra, data.coord_dec, data.halo_mass, data.redshift, data.halo_id FROM cosmoDC2_v1_1_4_image.data as data "
	query += f"WHERE data.halo_mass>{mmin} AND data.redshift<{zmax} "
	query += "AND is_central = 1 " 
	query += "AND Mag_true_z_lsst_z0 < 1.5 "
	query += ";"

	print(query)
	print("This query should run in ~4 minutes if the cache is empty and in ~1.5 minutes if the table is loaded in the cache")
	startTime = time.time()
	tab = pd.read_sql_query(query,conn)
	endTime = time.time()
	print(f"{len(tab)} galaxy clusters found (should be 147)")
	print("query ran in {:.1f} seconds".format(endTime - startTime))

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

    database = opts.database

    cls = lambda: os.system('cls' if os.name=='nt' else 'clear')

    conn, cursor = qservInit(opts.host, opts.user, opts.port)

    print("\n \n \n")
    print(f"Checking database {database}")
    listDB(conn, cursor)
    countObjects(conn, cursor, database)
    fullScan_1(conn)

if __name__ == '__main__':
    main()