#!/usr/bin/env python
"""
Utility script to demonstrate and benchmark parallel qserv queries on the cosmoDC2 database
It is using a list of halos created by Constantin Payerne - LPSC
Author: Dominique Boutigny - LAPP
Modified to use system calls to mysql instead of python mysql connector by Fabrice Jammes - LPSC
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import multiprocessing
from optparse import OptionParser
import os
import time
from optparse import OptionParser
import pickle

# ----------------------------
# Imports for other modules --
# ----------------------------
import mysql.connector
from mysql.connector import Error
import pandas as pd
import sqlparse

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

def query(host, port, user, db, halo, mysqlclient):

  halo_id = halo['halo_id']
  z_halo = halo['z']
  ra_up = halo['ra'] + 0.3
  ra_down = halo['ra'] - 0.3
  dec_up = halo['dec'] + 0.3
  dec_down = halo['dec'] - 0.3

  query = f"""
    SELECT
      data.coord_ra,
      data.coord_dec,
      data.redshift,
      data.mag_i,
      data.mag_r,
      data.mag_y,
      data.galaxy_id,
      data.shear_1,
      data.shear_2,
      data.convergence,
      data.ellipticity_1_true,
      data.ellipticity_2_true

    FROM
      {db}.data as data

    WHERE
      scisql_s2PtInBox(data.coord_ra, data.coord_dec, {ra_down}, {dec_down}, {ra_up}, {dec_up}) = 1

      AND data.redshift >= {z_halo + 0.1}
      AND data.mag_i <= 23;
  """

  if mysqlclient:
    tab = None
    stream = os.popen(f'mysql -h {host} -P {port} -u qsmaster -e "{query}"')
    output = stream.read()
    _LOG.info(f"---\nQUERY: {query}\nOUTPUT {output}\n---")
  else:
    conn = mysql.connector.connect(host=host, user=user, port=port)

    tab = pd.read_sql_query(query,conn)
    tab['halo_id'] = halo_id

    conn.close()
  return tab

def parallelQuery(host, port, user, db, halos, threads, mysqlclient):
  frames = []

  with multiprocessing.Pool(threads) as pool:
    i=0
    for index in range(0, len(halos), threads):
      _LOG.info(f"Process halos batch number: {i}")
      halos_batch = halos[index:index + threads]
  
      params = [(host, port, user, db, halo, mysqlclient, ) for halo in halos_batch]
      results = [pool.apply_async(query, p) for p in params]

      for r in results:
          df = r.get()
          if not mysqlclient:
            frames.append(df)
      i=i+1

  gals = pd.concat(frames)
  return gals

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
    parser.add_option("-f", "--pickle",
                      type="string",
                      default="cosmodc2_DM_halos.pkl",
                      help="pickle file with halo coordinates [%default]")
    parser.add_option("-j", "--threads",
                      type="int",
                      default=50,
                      help="number of threads [%default]")
    parser.add_option("-m", "--mass",
                      type="float",
                      default=3.E14,
                      help="Halo mass cut [%default]")
    parser.add_option("-M", "--mysql-client",
                      action="store_true",
                      dest="mysqlclient",
                      help="Use mysql client instead of python mysql connector")
    parser.add_option("--verbose", "-v", action="store_true", help="Use debug logging")

    (opts, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("Wrong number of arguments")

    database = opts.database
    host = opts.host
    port = opts.port
    user = opts.user
    threads = opts.threads
    massCut = opts.mass
    mysqlclient = opts.mysqlclient

    cls = lambda: os.system('cls' if os.name=='nt' else 'clear')
    
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

    _LOG.info(f"Checking database {database} - running {threads} queries in parallel")

    halosFile = opts.pickle
    with open(halosFile, 'rb') as halosFile:
      halos = pickle.load(halosFile)

    cut = halos["z"] < 1
    cut &= halos["halo_mass"] > massCut
    halos = halos[cut]
    _LOG.info(f"Number of halos: {len(halos)}")
    _LOG.info("Halo mass cut {:.2e}".format(massCut))

    startTime = time.time()
    galaxies = parallelQuery(host, port, user, database, halos, threads, mysqlclient)
    endTime = time.time()

    _LOG.info("*** query ran in {:.1f} seconds".format(endTime - startTime))
    _LOG.info(galaxies)

if __name__ == '__main__':
    main()
