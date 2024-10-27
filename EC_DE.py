import socket
import sys
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata
import json
import traceback

if len(sys.argv) != 8:
    print("ERROR: Número de argumentos incorrecto")
    print("Formato correcto -> python3 EC_DE <IP central> <Puerto central> <IP gestor> <Puerto gestor> <IP Sensor> <Puerto Sensor> <ID Taxi>")
    sys.exit(1)

# Variables iniciales del programa
argumentos = sys.argv
FIN = "<FIN>"
START = "START"
OK = "OK"
KO = "KO"

ALTO = 20
ANCHO = 20

IP_CENTRAL = argumentos[1]
PUERTO_CENTRAL = int(argumentos[2])
IP_GESTOR = argumentos[3]
PUERTO_GESTOR = int(argumentos[4])
IP_SENSOR = argumentos[5]
PUERTO_SENSOR = int(argumentos[6])
ID_TAXI = argumentos[7]

# Variables compartidas
ubicacion = None
cliente = None
destino = None
clienteDentro = False
mapa = None
permisoMovimiento = False
mapa_lock = threading.Lock()

############ FUNCIONES #############

def mostrarMapa(mapa):
    linea_horizontal = '-' * (ANCHO * 2 + 1)

    print(linea_horizontal)
    for y in range(ALTO):
        fila = "|"
        for x in range(ANCHO):
            elemento = mapa[x][y] if mapa[x][y] != ' ' else ' '
            fila += f"{elemento}|"
        print(fila)
        print(linea_horizontal)

def realizarViaje():
    global permisoMovimiento
    global clienteDentro
    global ubicacion
    global destino
    global mapa

    while True:
        if permisoMovimiento:
            mostrarMapa(mapa)
            antiguaPosicion = ubicacion

            if not clienteDentro:
                ubicacion = actualizarPosicion(cliente, destino)
                enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, OK)

                if ubicacion[0] == cliente[0] and ubicacion[1] == cliente[1]:
                    clienteDentro = True
            else:
                ubicacion = actualizarPosicion(cliente, destino)

                if ubicacion[0] == destino[0] and ubicacion[1] == destino[1]:
                    enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, True, OK)
                    break
                else:
                    enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, OK)
                        
            while not recibirActualizacionMapa():
                time.sleep(0.1)
        else:
            enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, KO)

        time.sleep(2)

def cerebroDigitalEngine():
    global clienteDentro

    while True:
        if recibirActualizacionMapa() and recibirPosicionCliente():
            clienteDentro = False
            realizarViaje()

def recibirActualizacionMapa():
    global mapa

    consumer = KafkaConsumer(
        'actualizar_mapa',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
    )

    for message in consumer:
        with mapa_lock:
            mapa = message.value
        return True

    return False

def recibirPosicionCliente():
    global cliente
    global destino

    consumer = KafkaConsumer(
        'procesar_viaje',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='taxi_group'
    )

    try:
        messages = consumer.poll(timeout_ms=1000)
        if not messages:
            return False

        for topic_partition, msgs in messages.items():
            for message in msgs:
                info = message.value
                taxi_id = info['taxi_id']

                if taxi_id == ID_TAXI:
                    cliente = info['posCliente']
                    destino = info['posDestino']
                    consumer.commit({
                        topic_partition: OffsetAndMetadata(message.offset + 1, None)
                    })
                    print("RECIBIDO ORDEN DE VIAJE")
                    return True
    except Exception as e:
        print(f"Error al recibir la orden del servicio: {e}")
        traceback.print_exc()

    return False

def enviarPosicionTaxi(taxi_id, posicion, antiguaPosicion, finalizado, estadoSensor):
    producerTAXI = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'actualizaciones_taxi'
    message = {'taxi_id': taxi_id, 'taxi_posicion': posicion, 'viaje_finalizado': finalizado, 'antigua_posicion': antiguaPosicion, 'estado_sensor': estadoSensor}
    producerTAXI.send(topic, message)
    producerTAXI.flush()
    
def conexionSensor():
    global permisoMovimiento
    socketSensor = socket.socket()
    socketSensor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socketSensor.bind((IP_SENSOR, PUERTO_SENSOR))
    socketSensor.listen(60)

    print(f"SERVIDOR CREADO [{IP_SENSOR} : {PUERTO_SENSOR}]")
    conexion, addr = socketSensor.accept()
    print(f"Conexión hecha con Sensor en {addr}")

    while True:
        try:
            mensaje = conexion.recv(4096).decode()
            if mensaje == FIN:
                break
            elif mensaje == OK:
                permisoMovimiento = True
            elif mensaje == KO:
                permisoMovimiento = False

            print(f"Mensaje Sensor entrante: [{mensaje}]")

        except Exception as e:
            print(f"Error en conexión con el Sensor: {e}")
            break

    conexion.close()
    socketSensor.close()

def movimiento(posX, posY, objetivo):
    
    if posY < objetivo[1]:
        posY = posY + 1
    elif posY > objetivo[1]:
        posY = posY - 1
    
    if posX < objetivo[0]:
        posX = posX + 1
    elif posX > objetivo[0]:
        posX = posX - 1

    return (posX, posY)

def actualizarPosicion(cliente, destino):
    global ubicacion
    global clienteDentro

    posX = ubicacion[0]
    posY = ubicacion[1]

    if clienteDentro:
        objetivo = destino
    else:
        objetivo = cliente

    return movimiento(posX, posY, objetivo)

def validarID(socketCentral):
    global ubicacion
    socketCentral.send(ID_TAXI.encode())
    respuesta = socketCentral.recv(4096)

    datos = respuesta.decode().strip().replace('<', '').split('>')

    if respuesta.decode() == FIN:
        print("ERROR: ID del taxi se está utilizando")
        socketCentral.close()
        sys.exit(1)
    else:
        ubicacion = (int(datos[1]), int(datos[2]))
        print("ID asignado con éxito")

def conectarConServidor():
    global socketCentral
    socketCentral = socket.socket()
    print(f"CONECTADO AL SERVIDOR CENTRAL EN [{IP_CENTRAL} : {PUERTO_CENTRAL}]")
    socketCentral.connect((IP_CENTRAL, PUERTO_CENTRAL))
    validarID(socketCentral)

############## MAIN ################

conectarConServidor()

hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

hiloKafka = threading.Thread(target=cerebroDigitalEngine)
hiloKafka.start()

hiloSensor.join()
