import mysql.connector
cnx = mysql.connector.connect(user='qsmaster', password='',
                              host='qserv-czar',
                              port=4040,
                              database='cosmoDC2_v1_1_4_image')
cnx.close()
