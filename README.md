# EasyCab

### Simulación distribuida de una flota de taxis

EasyCab conecta clientes, taxis y una central de coordinación mediante **Apache Kafka, sockets TCP y APIs REST**. El sistema incorpora sensores simulados, control meteorológico y visualización del estado de la flota.

**Python · Apache Kafka · Flask · Tkinter · Pygame · Astro · React · Fernet**

Proyecto académico de **Sistemas Distribuidos**, Universidad de Alicante, curso **2024–2025**.

[Arquitectura](#arquitectura) · [Ejecución local](docs/EJECUCION.md) · [Memoria](docs/memorias/Documentaci%C3%B3n.pdf) · [Equipo](#equipo)

## El proyecto en imágenes

<table>
  <tr>
    <td align="center"><img src="docs/images/mapa.jpg" width="330" alt="Mapa de EasyCab con taxis, clientes y destinos en una cuadrícula de 20 por 20" /></td>
    <td align="center"><img src="docs/images/cliente.jpg" width="400" alt="Formulario de solicitud de taxi con nombre, origen y destino" /></td>
  </tr>
  <tr>
    <td align="center">Seguimiento de la flota</td>
    <td align="center">Solicitud de un viaje</td>
  </tr>
</table>

Capturas de la memoria del proyecto elaborada por el equipo.

## Funcionalidades

- Coordinación de solicitudes de clientes y viajes de taxis simulados.
- Intercambio de peticiones, órdenes y actualizaciones mediante Kafka.
- Conexión inicial de los taxis y comunicación de sensores mediante sockets TCP.
- Simulación de estados de funcionamiento y retorno a base ante condiciones meteorológicas adversas.
- Mapa de escritorio en Pygame y visualización web mediante una API Flask.
- Cifrado Fernet de órdenes y actualizaciones entre la central y los taxis.
- Registro de taxis y auditoría local de eventos.

## Arquitectura

```mermaid
flowchart LR
    Cliente["Cliente · Tkinter"] <-->|"Peticiones y respuestas"| Kafka["Apache Kafka"]
    Kafka <-->|"Órdenes, posiciones y mapa"| Central["Central · Python"]
    Taxi["Motor digital del taxi"] <--> Kafka
    Taxi <-->|"Conexión inicial · TCP"| Central
    Sensor["Sensor · Tkinter"] -->|"OK / KO / FIN · TCP"| Taxi
    Taxi <-->|"HTTP"| Registro["Registro · Flask :5001"]
    Meteo["Control meteorológico · Flask :5000"] -->|"HTTP"| OpenWeather["OpenWeather"]
    Central -->|"Consulta de estado"| Meteo
    Central --> Pantalla["Mapa · Pygame"]
    Central --> API["API del mapa · Flask :5003"]
    Web["Interfaz web · Astro"] -->|"GET /mapa"| API
```

La central combina mensajería asíncrona y conexiones directas. Los procesos utilizan hilos, eventos y bloqueos para coordinar comunicaciones y estado. La persistencia del prototipo se basa en archivos locales compartidos por la central y el registro.

## Estructura

```text
src/                  Módulos Python y rutas compartidas
front/                Interfaz web Astro/React
data/
  requests/           JSON de peticiones de ejemplo
  locations/          Destinos disponibles
  maps/               Posiciones iniciales del mapa
docs/
  images/             Capturas
  memorias/           Memoria del equipo
  guias/              Guía de despliegue original
  EJECUCION.md        Instrucciones de ejecución
secrets/              Configuración local y ejemplo sin credenciales
requirements.txt      Dependencias Python
```

Durante la ejecución se crea `runtime/` para guardar el estado de los taxis, las claves de sesión, los tokens y los registros. Las rutas de datos y estado se calculan desde la ubicación del proyecto.

## Componentes

| Módulo | Responsabilidad |
| --- | --- |
| `src/EC_Central.py` | Coordinación de solicitudes, taxis y estado del mapa. |
| `src/EC_DE.py` | Motor digital de cada taxi. |
| `src/EC_S.py` | Sensor simulado con estados de funcionamiento. |
| `src/EC_Customer.py` | Interfaz de solicitud de viajes. |
| `src/EC_Registry.py` | API de registro y baja de taxis. |
| `src/EC_CTC.py` | Consulta meteorológica y estado de circulación. |
| `src/EC_API.py` | API HTTP del mapa. |
| `src/mapaPygame.py` | Mapa de escritorio y tabla de estados. |
| `src/ec_paths.py` | Rutas compartidas de datos y archivos de ejecución. |

## Ejecución local

Consulta la [guía de ejecución](docs/EJECUCION.md) para preparar el entorno y arrancar los procesos. El escenario descrito utiliza un solo equipo, un broker Kafka con ZooKeeper y una clave propia de OpenWeather.

La [interfaz web](front/README.md) consulta el estado de la central en el puerto `5003` y requiere que los servicios Python estén en ejecución para mostrar datos de la simulación.

## Alcance

EasyCab es un prototipo académico para explorar comunicaciones, concurrencia y coordinación en sistemas distribuidos. Utiliza persistencia en archivos y configuración local. El control meteorológico convierte la temperatura de la ciudad seleccionada en un estado de circulación; no utiliza datos reales de tráfico. Las APIs y conexiones TCP requieren configuración adicional para un despliegue fuera de un entorno local de prácticas.

## Equipo

**Aitana Cantó Rico · Levan Kipiani**

Proyecto desarrollado conjuntamente en pareja.

## Documentación

- [Instrucciones de ejecución](docs/EJECUCION.md).
- [Memoria del equipo](docs/memorias/Documentaci%C3%B3n.pdf).
- [Guía original de despliegue](docs/guias/guia_despliegue.pdf). Los comandos de este PDF corresponden a la estructura original; para las rutas actuales, utiliza las instrucciones de ejecución.
