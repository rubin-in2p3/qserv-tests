# qserv-tests
This repository contains a set of python scripts designed to check the various
databases deployed in the qserv cluster at IN2P3 and to control the performances 
on realistic queries. 

- test_cosmoDC2.py : Basic tests (number of objects and time for single query) on the DESC DC2 extragalactic catalog
- test_dr6-wfd.py : Basic tests (number of objects and time for single query) on the DC2 image processed catalogs (5 year depth)
- test_cosmoDC2_parallel.py : Test parallel queries on the cosmoDC2 extragalactic catalog. 

The list of halos used in test_cosmoDC2_parallel.py is from Constantin Payerne (LPSC)