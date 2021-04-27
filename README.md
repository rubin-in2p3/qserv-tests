# qserv-tests
This repository contains a set of python scripts designed to check the various
databases deployed in the qserv cluster at IN2P3 and to control the performances 
on realistic queries. 

- test_cosmoDC2.py : Basic tests (number of objects and time for single query) on the DESC DC2 extragalactic catalog
- test_dr6-wfd.py : Basic tests (number of objects and time for single query) on the DC2 image processed catalogs (5 year depth)
- test_cosmoDC2_parallel.py : Test parallel queries on the cosmoDC2 extragalactic catalog. 

The list of halos used in test_cosmoDC2_parallel.py is from Constantin Payerne (LPSC)

## Dependencies
A standard python 3 environment is needed with the following extra packages:
- mysql-connector-python - SQL python API
- sqlparse - to parse SQL queries and strip out comments (not strictly necessary)

## How to run
There are no required arguments to run the scripts. Optional arguments can be displayed using "--help"

The number of paralle threads can be specified using "--threads" in test_cosmoDC2_parallel.py. One can also change
the halo mass cut (--mass) in order to change the number of halos analyzed. The default is 3.E14 which will select
548 halos. Setting this cut to 1.E14 will select 5953 halos.

## Warning
At the momment, test_cosmoDC2_parallel.py is failing systematically for ~100 threads and may also fail for a smaller
number of threads. This is under investigation. 