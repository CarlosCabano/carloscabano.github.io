#CAMBIAR AQUI USUARIO Y CONTRASEÑA DE BBDD DE MYSQL
USUARIO_MYSQL="root"
CONTRASENIA_MYSQL="asd123asd"

#importamos librería
import mysql.connector as mysql
import pandas as pd 
import numpy as np

#ahora vamos a usar sqlalchemy, es un kit de herramientas que permiten trabajar con SQL
import sqlalchemy
cadena_conexion='mysql+mysqlconnector://'+USUARIO_MYSQL+':'+CONTRASENIA_MYSQL+'@localhost/renfe'
engine = sqlalchemy.create_engine(cadena_conexion) # connect to server

#mostramos si hay alguna tabla
tablas=engine.execute("SHOW TABLES")

print(tablas.fetchall())


'''
FUNCIONES MYSQL ÚTILES
MIN: Te da el valor mínimo de una lista de números
Ejemplo SELECT MIN(EDAD) FROM TABLA_PRUEBA

MAX: Te da el valor máximo de una lista de números
Ejemplo SELECT MAX(EDAD) FROM TABLA_PRUEBA

AVG: Te da el promedio de una lista de números
Ejemplo SELECT AVG(EDAD) FROM TABLA_PRUEBA

MONTH: Te devuelve el mes en número de una fecha
Ejemplo SELECT MONTH(FECHA_NACIMIENTO) FROM TABLA_PRUEBA

YEAR: Te devuelve el año de una fecha
Ejemplo SELECT YEAR(FECHA_NACIMIENTO) FROM TABLA_PRUEBA

TIMESTAMPDIFF: Te devuelve la diferencia de meses, dias, horas, minutos (Según le especifiques en la función)
Ejemplo SELECT NOMBRE_PERSONA FROM TABLA_PRUEBA WHERE TIMESTAMPDIFF(MONTH, FECHA_NACIMIENTO, FECHA_ACTUAL)>250

LIMIT X: Se pone al final del query y te limita los resultados del select
Ejemplo SELECT NOMBRE FROM TABLA_PRUEBA WHERE EDAD>30 LIMIT 5 #Te devolverá solo los 5 primeros según como lo ordenes

DISTINCT: Se pone luego de la sintaxis SELECT para que no devuelva duplicados
Ejemplo SELECT DISTINCT NOMBRE FROM TABLA_PRUEBA #Si hubieran 3 marías en la BBDD solo devolvería una

LIKE: Se pone en el where para que la condición busque en la columna que se especifica todas las que aparecen
Ejemplo SELECT NOMBRE FROM TABLA_PRUEBA WHERE NOMBRE '%maria%' #El resultado buscará en todos los nombres que aparezcan maria como maria jose, ana maria, maria, etc
'''

#ESTRUCTURA DE UN SELECT

#  SELECT CAMPOS  
#    FROM TABLA A
#    INNER/LEFT JOIN TABLA2 B ON A.ID=B.ID AND CONDICIONES
#   WHERE CONDICIONES 
#   GROUP BY CAMPOS
#   HAVING CONDICIONES 
#   ORDER BY CAMPO ASC/DESC

pd.read_sql("SELECT * FROM RENFE", con=engine)

pd.read_sql("SELECT * FROM TARIFAS", con=engine)


'''DESARROLLO DEL PROYECTO'''
'''FASE ANALISIS CON SQL'''


'''PREGUNTA 2'''
pd.read_sql("SELECT COUNT(*) FROM CIUDADES", con=engine)



'''PREGUNTA 3'''
#HINT: usar MIN y MAX
pd.read_sql("SELECT MIN(FECHA_CONSULTA), MAX(FECHA_CONSULTA) FROM RENFE", con=engine)


'''PREGUNTA 4'''
#HINT: usar MIN, MAX, AVG
#Considerar el precio del billete superior a 0
pd.read_sql("SELECT MIN(PRECIO), MAX(PRECIO), AVG(PRECIO) FROM RENFE WHERE PRECIO>0", con=engine)


'''PREGUNTA 5'''
pd.read_sql("SELECT COUNT(*) FROM RENFE WHERE MONTH(FECHA_CONSULTA)=8 AND YEAR(FECHA_CONSULTA)=2019", con=engine)


'''PREGUNTA 6'''
#HINT: Usar TIMESTAMPDIFF
pd.read_sql("SELECT COUNT(*) FROM RENFE WHERE TIMESTAMPDIFF(HOUR, FECHA_INICIO, FECHA_FIN) >4", con=engine)



'''PREGUNTA 7'''
#HINT 1: crear una consulta con la cantidad de ciudades y simulaciones 
#HINT 2: cruzar esta información con la tabla de ciudades
#HINT 3: quedarme con la primera (LIMIT 1)
pd.read_sql("SELECT B.DESCRIPCION, COUNT(A.ID_CIUDAD_ORIGEN) AS CANTIDAD FROM RENFE A \
            INNER JOIN CIUDADES B ON A.ID_CIUDAD_ORIGEN=B.ID_CIUDAD \
            GROUP BY B.DESCRIPCION ORDER BY CANTIDAD ASC LIMIT 1", con=engine)



'''PREGUNTA 8'''
#HINT 1: Usar distinct
#HINT 2: IS NULL
pd.read_sql("SELECT DISTINCT A.DESCRIPCION FROM CIUDADES A \
            LEFT JOIN RENFE B ON A.ID_CIUDAD=B.ID_CIUDAD_ORIGEN  \
            LEFT JOIN RENFE C ON A.ID_CIUDAD=C.ID_CIUDAD_DESTINO \
            WHERE B.ID_CIUDAD_ORIGEN IS NULL AND C.ID_CIUDAD_DESTINO IS NULL", con=engine)



'''PREGUNTA 9'''
#HINT 1: hacer un min y max de precio
#HINT 2: agregar el ID_CLASE
#HINT 3: hacer cruce con la tabla de clases
pd.read_sql("SELECT A.DESCRIPCION, MIN(B.PRECIO), MAX(B.PRECIO) \
            FROM RENFE B \
                INNER JOIN CLASES A ON A.ID_CLASE=B.ID_CLASE \
                    WHERE B.PRECIO>0 \
                        GROUP BY A.DESCRIPCION", con=engine)


'''PREGUNTA 10'''
#HINT 1: Mismos pasos que el anterior solo con la tabla de TARIFAS
#HINT 2: Agregar mes septiembre de 2019
pd.read_sql("SELECT A.DESCRIPCION,  MAX(B.PRECIO) \
            FROM RENFE B \
                INNER JOIN TARIFAS A ON A.ID_TARIFA=B.ID_TARIFA \
                    WHERE B.PRECIO>0 AND MONTH(B.FECHA_INICIO)=9 AND YEAR(B.FECHA_INICIO)=2019 \
                        AND MONTH(B.FECHA_FIN)=9 \
                        GROUP BY A.DESCRIPCION", con=engine)




'''PREGUNTA 11'''
#HINT 1: Usar el AVG para el promedio
#HINT 2: Usar un order ASCENDENTE

pd.read_sql("SELECT A.DESCRIPCION, AVG(B.PRECIO) FROM RENFE B \
                INNER JOIN TIPO_TRENES A ON A.ID_TIPO_TREN=B.ID_TIPO_TREN \
                    WHERE PRECIO>0 \
                        GROUP BY A.DESCRIPCION ", con=engine)


'''PREGUNTA 12'''
#HINT 1: Sacar un conteo de simulaciones
#HINT 2: Cruzar con la tabla CIUDADES dos veces por origen y destino

pd.read_sql("SELECT A.DESCRIPCION AS ORIGEN,B.DESCRIPCION AS DESTINO,COUNT(*) FROM \
            RENFE C \
            INNER JOIN CIUDADES A ON A.ID_CIUDAD=C.ID_CIUDAD_ORIGEN AND A.DESCRIPCION='MADRID' \
            INNER JOIN CIUDADES B ON B.ID_CIUDAD=C.ID_CIUDAD_DESTINO AND B.DESCRIPCION='BARCELONA' \
            GROUP BY ORIGEN, DESTINO", con=engine)



'''PREGUNTA 13'''
#HINT 1: Obtener el año, mes y el precio medio sin cruces
#HINT 2: Cruzar dos veces con
#ciudades origen Barcelona y destino Madrid
#HINT 3: Cruzar con tipo de trenes y filtrar AVE

pd.read_sql("SELECT MONTHNAME(A.FECHA_INICIO) AS MES, AVG(A.PRECIO) FROM RENFE A \
                INNER JOIN CIUDADES B ON A.ID_CIUDAD_ORIGEN=B.ID_CIUDAD AND B.DESCRIPCION='BARCELONA' \
                    INNER JOIN CIUDADES C ON A.ID_CIUDAD_DESTINO=C.ID_CIUDAD AND C.DESCRIPCION='MADRID' \
                        INNER JOIN TIPO_TRENES D ON A.ID_TIPO_TREN=D.ID_TIPO_TREN AND D.DESCRIPCION='AVE' \
                            WHERE A.PRECIO>0 GROUP BY MES", con=engine)


'''PREGUNTA 14'''
#HINT 1: Hacer cruce de Tipo_trenes y quedarme solo con la descripcion que contenga AVE
#HINT 2: Cruce con la tabla de ciudades


'''PREGUNTA 15'''
#HINT 1: Obtener el precio medio
#HINT 2: Cruzar 2 veces con la tabla CIUDADES
#HINT 3: Realizar el filtro de mes y año y filtro MADRID y VALENCIA



'''PREGUNTA 16'''
#HINT: Construirlo paso a paso y filtrar según se indique


'''FINALMENTE GENERAREMOS EL DATASET!!'''

