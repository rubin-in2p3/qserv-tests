#!/usr/bin/env python
"""
Utility script to benchmark the dr6-wfd database in qserv
Author: Dominique Boutigny - LAPP
"""

import os
import time
import mysql
from mysql.connector import Error
import sqlparse
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

def listTables(conn, cursor, db):
  query = f"SHOW TABLES in {db};"
  cursor.execute(query)
  res = cursor.fetchall()
  print(f"found {len(res)} tables in {db}")
  print([list(item.values()) for item in res])

def countObjects(conn, cursor, db):
  query = f"SELECT COUNT(*) FROM {db}.position;"
  cursor.execute(query)
  res = cursor.fetchall()

  print(f"{res[0]['COUNT(*)']} entries found - should be 147088445" )

def fullScan_1(conn):
  # Simple query to trigger a full scan

  ra_min = 20
  ra_max = 95
  dec_min = -70
  dec_max = 0

  #build a query equivalent to the "good" and "clean" flags in GCRCatalogs
  query_good  = f"""
    AND dref.base_PixelFlags_flag_edge = 0
    AND dref.base_PixelFlags_flag_interpolatedCenter = 0
    AND dref.base_PixelFlags_flag_saturatedCenter = 0
    AND dref.base_PixelFlags_flag_crCenter = 0
    AND dref.base_PixelFlags_flag_bad = 0
    AND dref.base_PixelFlags_flag_suspectCenter = 0
    AND dref.base_PixelFlags_flag_clipped = 0 
  """

  query_clean = query_good + 'AND dref.deblend_skipped = 0 '

  # Main query
  query = f"""
    -- This is the part of the query where we specify which columns we want to extract from the tables
    SELECT 
      dref.coord_ra as ra, 
      dref.coord_dec as dec, 
      dref.ext_shapeHSM_HsmShapeRegauss_e1,
      dref.ext_shapeHSM_HsmShapeRegauss_e2, 
      dref.objectId as id, 
      dfrc.i_modelfit_CModel_instFlux

    -- Here we indicate which tables will be used and at the same time we define a short alias for each table
    FROM 
      dc2_object_run2_2i_dr6_wfd.dpdd_ref as dref,
      dc2_object_run2_2i_dr6_wfd.position as dpos,
      dc2_object_run2_2i_dr6_wfd.dpdd_forced as dfrc

    -- Here we specify the ra, dec bounding box and we use the special SQL function: scisql_s2PtInBox
    -- It is mandatory to put this constraint right after the WHERE statement
    -- Using the special function is mandatory to use the special qserv optimization mechanism
    WHERE 
      scisql_s2PtInBox(coord_ra, coord_dec, {ra_min}, {dec_min}, {ra_max}, {dec_max}) = 1

    -- The following is a join between the 3 tables on the objectId. It gurantees that each line extracted 
    -- from the 3 tables corresponds to the same object
    AND dref.objectId = dpos.objectId 
    AND dfrc.objectId = dpos.objectId

    -- We add all the other selection cuts
    AND dpos.detect_isPrimary = 1
    AND dfrc.i_modelfit_CModel_flag = 0
    AND dfrc.i_modelfit_CModel_instFlux > 0
    AND dref.base_SdssCentroid_flag = 0

    -- We can have SQL math funcions in the query
    AND dref.base_Blendedness_abs < POWER(10, -0.375)

    AND dref.base_Blendedness_abs_instFlux IS NULL 
    AND dref.base_ClassificationExtendedness_flag = 0 
    AND dref. base_ClassificationExtendedness_value > 0 
    AND ext_shapeHSM_HsmShapeRegauss_flag = 0 
    AND dfrc.i_modelfit_CModel_flag = 0 AND dfrc.i_modelfit_CModel_instFlux > 0 

    -- We put a crazy cut on the flux S/N for demonstration purpose to limit the number of returned lines 
    -- A more reasonable cut would be ~30 as in the commented line
    AND dfrc.i_modelfit_CModel_instFlux/dfrc.i_modelfit_CModel_instFluxErr > 1000

    AND dref.ext_shapeHSM_HsmShapeRegauss_resolution >= 0.3
    AND dref.ext_shapeHSM_HsmShapeRegauss_sigma <= 0.4

    -- Another example of SQL math
    AND SQRT(POWER(dref.ext_shapeHSM_HsmShapeRegauss_e1, 2)+POWER(dref.ext_shapeHSM_HsmShapeRegauss_e2, 2)) < 2
  """

  # Finally we add the query_clean that we have defined in the previous cell 
  query += query_clean
  # And the final semi-column
  query += ";"

  # As the SQL python API doesn't accept SQL comments we need to filter out our 
  # nicely formatted query

  query = sqlparse.format(query, strip_comments=True, reindent=True).strip()


  print(query)
  print("This query should run in ~2 minutes if the cache is empty and in ~35 seconds if the table is loaded in the cache")
  startTime = time.time()
  tab = pd.read_sql_query(query,conn)
  endTime = time.time()
  print(f"{len(tab)} galaxy clusters found (should be 260787)")
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
                      default="dc2_object_run2_2i_dr6_wfd",
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
    listTables(conn, cursor, database)
    countObjects(conn, cursor, database)
    fullScan_1(conn)

if __name__ == '__main__':
    main()