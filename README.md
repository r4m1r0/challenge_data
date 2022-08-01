# Challenge

El equipo de Ciencia de Datos se ha acercado a ti ya que necesitan incorporar información sobre clima dentro de sus modelos predictivos. Para ello, han encontrado el siguiente servicio web: https://smn.conagua.gob.mx/es/web-service-api. Te piden que los apoyes en lo siguiente:

## Objetivo

1)	Cada hora debes consumir el último registro devuelto por el servicio de pronóstico por municipio y por hora.
2)	A partir de los datos extraídos en el punto 1, generar una tabla a nivel municipio en la que cada registro contenga el promedio de temperatura y precipitación de las últimas dos horas.
3)	Hay una carpeta “data_municipios” que contiene datos a nivel municipio organizados por fecha, cada vez que tu proceso se ejecute debe generar una tabla en la cual se crucen los datos más recientes de esta carpeta con los datos generados en el punto 2.
4)	Versiona todas tus tablas de acuerdo a la fecha y hora en la que se ejecutó el proceso, en el caso del entregable del punto 3, además, genera una versión “current” que siempre contenga una copia de los datos más recientes.


## Tabla de Contenido
* [Información General](#informacion-general)
* [Tecnologias](#tecnologias)
* [Setup](#setup)

## Informacion General


## Tecnologias

    airflow
    pandas

## Setup

```
python3 -m venv venv
```
Generar su ambiente virtual

```
python -m pip install -r requirements.txt
```
Descarga las dependencias necesarias para la correcta ejecución del proyecto
```
docker-compose up --build -d
```
