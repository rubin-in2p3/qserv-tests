#!/usr/bin/env python
"""
Utility script to demonstrate and benchmark parallel qserv queries on the cosmoDC2 database
It is using a list of halos created by Constantin Payerne - LPSC 
Author: Dominique Boutigny - LAPP
"""

import os
import time
import mysql
from mysql.connector import Error
from optparse import OptionParser
import pandas as pd
import pickle
import multiprocessing

def query(host, port, user, db, halo):
  conn = mysql.connector.connect(host=host, user=user, port=port)

  halo_id = halo['halo_id']
  z_halo = halo['z']
  ra_up = halo['ra'] + 0.3
  ra_down = halo['ra'] - 0.3
  dec_up = halo['dec'] + 0.3
  dec_down = halo['dec'] - 0.3
  query = "SELECT data.coord_ra, data.coord_dec, data.redshift, "
  query += "data.mag_i, data.mag_r, data.mag_y, data.galaxy_id, "
  query += "data.shear_1, data.shear_2, data.convergence, "
  query += "data.ellipticity_1_true, data.ellipticity_2_true " 
  query += f"FROM {db}.data as data "
  query += f"WHERE data.redshift >= {z_halo + 0.1} "
  query += f"AND scisql_s2PtInBox(data.coord_ra, data.coord_dec, {ra_down}, {dec_down}, {ra_up}, {dec_up}) = 1 "
  query += f"AND data.mag_i <= 23 "
  query += ";"
  tab = pd.read_sql_query(query,conn)
  tab['halo_id'] = halo_id
  return tab

def parallelQuery(host, port, user, db, halos, threads):
  frames = []
  n_halos = len(halos)
  num_paquets = n_halos//threads

  with multiprocessing.Pool(threads) as pool:
      for paquet in range(num_paquets):
          print(f"processing paquet number {paquet}")
      
          params = [(host, port, user, db, halos[i+paquet*threads], ) for i in range(threads)]
          results = [pool.apply_async(query, p) for p in params]

          for r in results:
              df = r.get()
              frames.append(df)
              
      last_batch = n_halos%threads
      params = [(host, port, user, db, halos[i+num_paquets*threads], ) for i in range(last_batch)]
      results = [pool.apply_async(query, p) for p in params]

      for r in results:
          df = r.get()
          frames.append(df)
        
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

    (opts, args) = parser.parse_args()
    if len(args) != 0:
        parse.error("Wrong number of arguments")

    database = opts.database
    host = opts.host
    port = opts.port
    user = opts.user
    threads = opts.threads
    massCut = opts.mass

    cls = lambda: os.system('cls' if os.name=='nt' else 'clear')

    print("\n \n \n")
    print(f"Checking database {database} - running {threads} queries in parallel")

    halosFile = opts.pickle
    with open(halosFile, 'rb') as halosFile:
      halos = pickle.load(halosFile)

    cut = halos["z"] < 1
    cut &= halos["halo_mass"] > massCut
    halos = halos[cut]
    print(f"Number of halos: {len(halos)}")
    print("Halo mass cut {:.2e}".format(massCut))

    startTime = time.time()
    galaxies = parallelQuery(host, port, user, database, halos, threads)
    endTime = time.time()

    print("*** query ran in {:.1f} seconds".format(endTime - startTime))
    print(galaxies)

if __name__ == '__main__':
    main()