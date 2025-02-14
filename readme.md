# Censo 2017 Viviendas

Censo de Viviendas procesado con R para disponibilizarlo con códigos/nombres de comunas y regiones, y etiquetas de sus variables. Disponible en formato ancho (6,5 millones de filas x 19 columnas, en formato Parquet) o formato largo de conteos por comunas (17 mil filas con columna de cantidad de viviendas por comuna, por cada variable y categoría). El formato largo (`censo_vivienda_comunas.csv`) es considerablemente más liviano de usar, pero sólo sirve para obtener estadísticos a nivel de comunas, mientras que el formato ancho (`censo_vivienda.parquet`) contiene toda la resolución de la información, pero es más pesado y demandante de usar.

### Fuente:
- [Censo de Población y Vivienda 2017](https://www.ine.gob.cl/estadisticas/sociales/censos-de-poblacion-y-vivienda/censo-de-poblacion-y-vivienda)