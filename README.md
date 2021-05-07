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

## Sample output
```
./test_dr6-wfd.py

 
 

Checking database dc2_object_run2_2i_dr6_wfd
found 8 databases on the qserv cluster
['cosmoDC2_v1_1_4_image', 'dc2_object_run22i_dr6_wfd_v1_08', 'dc2_object_run2_2i_dr6_wfd', 'information_schema', 'qservCssData', 'qservMeta', 'qservResult', 'test']
found 3 tables in dc2_object_run2_2i_dr6_wfd
[['dpdd_forced'], ['dpdd_ref'], ['position']]
147088445 entries found - should be 147088445
SELECT dref.coord_ra as ra,
       dref.coord_dec as dec,
       dref.ext_shapeHSM_HsmShapeRegauss_e1,
       dref.ext_shapeHSM_HsmShapeRegauss_e2,
       dref.objectId as id,
       dfrc.i_modelfit_CModel_instFlux
FROM dc2_object_run2_2i_dr6_wfd.dpdd_ref as dref,
     dc2_object_run2_2i_dr6_wfd.position as dpos,
     dc2_object_run2_2i_dr6_wfd.dpdd_forced as dfrc
WHERE scisql_s2PtInBox(coord_ra, coord_dec, 20, -70, 95, 0) = 1
  AND dref.objectId = dpos.objectId
  AND dfrc.objectId = dpos.objectId
  AND dpos.detect_isPrimary = 1
  AND dfrc.i_modelfit_CModel_flag = 0
  AND dfrc.i_modelfit_CModel_instFlux > 0
  AND dref.base_SdssCentroid_flag = 0
  AND dref.base_Blendedness_abs < POWER(10, -0.375)
  AND dref.base_Blendedness_abs_instFlux IS NULL
  AND dref.base_ClassificationExtendedness_flag = 0
  AND dref. base_ClassificationExtendedness_value > 0
  AND ext_shapeHSM_HsmShapeRegauss_flag = 0
  AND dfrc.i_modelfit_CModel_flag = 0
  AND dfrc.i_modelfit_CModel_instFlux > 0
  AND dfrc.i_modelfit_CModel_instFlux/dfrc.i_modelfit_CModel_instFluxErr > 1000
  AND dref.ext_shapeHSM_HsmShapeRegauss_resolution >= 0.3
  AND dref.ext_shapeHSM_HsmShapeRegauss_sigma <= 0.4
  AND SQRT(POWER(dref.ext_shapeHSM_HsmShapeRegauss_e1, 2)+POWER(dref.ext_shapeHSM_HsmShapeRegauss_e2, 2)) < 2
  AND dref.base_PixelFlags_flag_edge = 0
  AND dref.base_PixelFlags_flag_interpolatedCenter = 0
  AND dref.base_PixelFlags_flag_saturatedCenter = 0
  AND dref.base_PixelFlags_flag_crCenter = 0
  AND dref.base_PixelFlags_flag_bad = 0
  AND dref.base_PixelFlags_flag_suspectCenter = 0
  AND dref.base_PixelFlags_flag_clipped = 0
  AND dref.deblend_skipped = 0 ;
This query should run in ~2 minutes if the cache is empty and in ~35 seconds if the table is loaded in the cache
260787 galaxy clusters found (should be 260787)
query ran in 30.2 seconds
```


##  [Kubernetes HOW-TO](./doc/k8s.md)