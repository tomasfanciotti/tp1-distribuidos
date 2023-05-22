# Bikes Rides Analyzer

### tp1-distribuidos 


Para ejecutar el trabajo práctico utilizar el script: ``tp1.sh``:

- ``tp1.sh build``      - Buildea las imagenes necesarias
- ``tp1.sh run``        - Ejecuta el script 'composer.py' con la configuración deseada, genera el yaml y levanta docker compose.
- ``tp1.sh run logs ``  - Revisar los logs
- ``tp1.sh stop``       - Detiene los containers (utilizar -k para eliminarlos)


Ubicar los datasets dentro de la siguiente ubicación: ```./.data/dataset/``` que se bindeará al container.