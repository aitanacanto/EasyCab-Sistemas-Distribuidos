# Ejecución local de EasyCab

Instrucciones para ejecutar los componentes en un único equipo con Ubuntu y entorno gráfico.

## Preparación

Necesitas Python 3 con Tkinter, un entorno gráfico para Tkinter y Pygame, Node.js/npm y una distribución de Kafka compatible con ZooKeeper. Utiliza versiones compatibles con las dependencias de `requirements.txt` y `front/package-lock.json`.

Desde la raíz del repositorio:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Tkinter es un componente del sistema/Python, no una dependencia de pip. Activa el entorno virtual en cada terminal que ejecute módulos Python.

## 1. Kafka

El ejemplo supone que todos los procesos se ejecutan en el mismo ordenador y Kafka escucha en `localhost:9092`. Configura los listeners y advertised listeners del broker de acuerdo con ese escenario. En una distribución que incluya ZooKeeper, ejecuta desde la carpeta de Kafka, en terminales separadas:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

```bash
bin/kafka-server-start.sh config/server.properties
```

Esos archivos pertenecen a Kafka, no a este repositorio. Estos comandos no sirven para una instalación exclusiva de KRaft. El broker debe permitir la creación automática de los topics o tener creados los utilizados por el código, entre ellos `solicitud_taxi`, `procesar_viaje`, `actualizaciones_taxi` y `actualizar_mapa`.

## 2. OpenWeather

Crea localmente un archivo `secrets/apikey.txt` con tu propia clave de OpenWeather, en una sola línea. No lo subas a GitHub. El módulo necesita conexión a Internet y una clave válida.

Consulta la ciudad seleccionada cada tres segundos: una temperatura mayor que cero produce `OK`; el resto de casos y los errores producen `KO`. Comprueba que la frecuencia encaja con tu cuenta de OpenWeather.

## 3. Servicios

Desde la raíz, con el entorno activado, usa una terminal por comando.

Control meteorológico:

```bash
python3 src/EC_CTC.py secrets/apikey.txt
```

Expone `GET http://localhost:5000/trafico` y abre una ventana para cambiar de ciudad.

Registro de taxis:

```bash
python3 src/EC_Registry.py
```

Expone la API en `5001`. Usa los mismos archivos de `runtime/` que la central; las rutas se resuelven desde el proyecto.

Central:

```bash
python3 src/EC_Central.py 9091 localhost 9092 localhost
```

Argumentos: puerto TCP de la central, dirección Kafka, puerto Kafka y dirección del control meteorológico. El `9091` es un puerto de ejemplo: usa uno libre y repítelo en el comando del taxi.

La central carga `data/maps/mapa.txt`, genera el estado de los taxis, abre Pygame e inicia la API `http://localhost:5003/mapa`. No hace falta ejecutar `EC_API.py` aparte. El arranque reinicia claves, tokens y estado; no reinicies la central durante una sesión que quieras conservar.

## 4. Taxi y sensor

El mapa incluido contiene IDs del `1` al `6`. Para el taxi `1`, inicia el motor digital:

```bash
python3 src/EC_DE.py localhost 9091 localhost 9092 localhost 9093 1
```

Argumentos: IP central, puerto central, IP Kafka, puerto Kafka, IP donde escucha al sensor, puerto sensor e ID taxi.

Después, en otra terminal:

```bash
python3 src/EC_S.py localhost 9093
```

Pulsa **Conectar Taxi** en la ventana del motor digital. En la ventana del sensor, `s` alterna el estado y `q` finaliza la conexión. Para más taxis, usa otro ID del mapa y un puerto de sensor distinto por motor digital.

## 5. Cliente

Formulario interactivo:

```bash
python3 src/EC_Customer.py localhost -f
```

O recorrido de ejemplo:

```bash
python3 src/EC_Customer.py localhost data/requests/EC_Requests_a.json
```

Ambos modos abren una interfaz; el modo JSON carga los destinos del archivo. En el formulario usa coordenadas del `1` al `20`.

## 6. Interfaz web

```bash
cd front
npm ci
npm run dev
```

Abre la dirección indicada por Astro. La interfaz consulta `http://localhost:5003/mapa` cada 500 ms. Por esa dirección fija, abre el navegador en el mismo equipo que la central.

## Limitaciones de configuración

- Varias conexiones Kafka de la central usan `localhost:9092` fijo, aunque existan argumentos de configuración.
- El motor digital usa `localhost:5001` para el registro.
- La central liga su socket a la dirección pasada como IP del gestor Kafka; esas dos direcciones no son independientes en la implementación actual.
- Central y registro comparten archivos locales dentro de `runtime/`. Las APIs usan el servidor de desarrollo de Flask.
- Los módulos HTTP y TCP no configuran TLS; utiliza el sistema en una red local de prácticas.

Un despliegue en varios equipos requiere revisar direcciones, archivos compartidos y red. Esta guía describe el prototipo académico, no un servicio preparado para producción.

## Carpetas de configuración y estado

Los JSON de peticiones están en `data/requests/`, los destinos en `data/locations/` y el mapa inicial en `data/maps/`. Los archivos generados de taxis, tokens, claves y auditoría se guardan en `runtime/`, creada automáticamente al cargar las rutas compartidas. Las credenciales locales se conservan en `secrets/`. Usa credenciales propias y conserva esta configuración local fuera del control de versiones.

Los PDF originales se conservan sin modificar en `docs/`; sus comandos pueden usar las rutas anteriores.
