#!/usr/bin/env python
"""
Utility script to benchmark the cosmoDC2 database in qserv
Authors: Dominique Boutigny - LAPP
         Fabrice Jammes - LPC+
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
from optparse import OptionParser
import os
import sys
import time

# ----------------------------
# Imports for other modules --
# ----------------------------
import mysql.connector
from mysql.connector import Error
import sqlparse

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


def qservInit(host, user, port):
    # Inititialize qserv connection
    conn = mysql.connector.connect(host=host, user=user, port=port, tls_versions=['TLSv1.1', 'TLSv1.2'])
    cursor = conn.cursor(dictionary=True, buffered=True)
    return conn, cursor

def listDB(conn, cursor):
    query = "SHOW DATABASES;"
    cursor.execute(query)
    res = cursor.fetchall()

    _LOG.info(f'Found {len(res)} databases on the qserv cluster: {[item["Database"] for item in res]}')

def countObjects(conn, cursor, db):
    query = f"SELECT COUNT(*) FROM {db}.position;"
    cursor.execute(query)
    res = cursor.fetchall()
    nb_entries = res[0]['COUNT(*)']
    _LOG.info(f"{nb_entries} entries found - Should be 2 256 249 331" )

def fullScan_1(conn, lite=False):
    # Simple query to select galaxy clusters

    mmin = 5.e14 #Msun
    zmax = 2

    query = f"""
        SELECT 
            data.coord_ra, 
            data.coord_dec, 
            data.halo_mass, data.redshift, 
            data.halo_id 
        FROM cosmoDC2_v1_1_4_image.data as data
        WHERE data.halo_mass>{mmin} AND data.redshift<{zmax}
            AND is_central = 1
            AND Mag_true_z_lsst_z0 < 1.5
            ;
    """

    # Format query
    query = sqlparse.format(query, strip_comments=True, reindent=True).strip()

    _LOG.info(query)
    _LOG.info("This query should run in ~4 minutes if the cache is empty and in ~1.5 minutes if the table is loaded in the cache")
    startTime = time.time()
    if lite:
        cursor = conn.cursor(dictionary=True, buffered=True)
        cursor.execute(query)
        res = cursor.fetchall()
        nb_row = len(res)
        for row in res:
            print(row)
    else:
        import pandas as pd
        tab = pd.read_sql_query(query,conn)
        nb_row = len(tab)
    endTime = time.time()
    _LOG.info(f"{nb_row} galaxy clusters found (should be 147)")
    _LOG.info("query ran in {:.1f} seconds".format(endTime - startTime))

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
    parser.add_option("-l", action="store_true", dest="lite", help="only run sql queries, but no data analysis")
    parser.add_option("--verbose", "-v", action="store_true", help="Use debug logging")

    (opts, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("Wrong number of arguments")

    logger = logging.getLogger()

    if opts.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # create console handler
    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    database = opts.database

    logger.info("Checking database %s", database)

    cls = lambda: os.system('cls' if os.name=='nt' else 'clear')

    conn, cursor = qservInit(opts.host, opts.user, opts.port)

    listDB(conn, cursor)
    countObjects(conn, cursor, database)
    fullScan_1(conn, opts.lite)

if __name__ == '__main__':
    main()
