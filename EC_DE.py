import socket
import sys
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata
import json

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

############ FUNCIONES #############

def mostrarMapa():
    global mapa
    for y in range(ALTO):
        fila = []
        for x in range(ANCHO):
            fila.append(mapa[x][y])
        print(' '.join(fila))

def realizarViaje():
    global permisoMovimiento
    global clienteDentro
    global ubicacion
    global destino

    while True:
        if permisoMovimiento:
            mostrarMapa()
            if not clienteDentro:
                ubicacion = actualizarPosicion(ubicacion, cliente, mapa)
                enviarPosicionTaxi(ID_TAXI, ubicacion, False)

                if ubicacion[0] == cliente[0] and ubicacion[1] == cliente[1]:
                    clienteDentro = True
            else:
                ubicacion = actualizarPosicion(ubicacion, destino, mapa)

                if ubicacion[0] == destino[0] and ubicacion[1] == destino[1]:
                    enviarPosicionTaxi(ID_TAXI, ubicacion, True)
                    break
                else:
                    enviarPosicionTaxi(ID_TAXI, ubicacion, False)

        time.sleep(2)

def cerebroDigitalEngine():
    global clienteDentro

    while True:
        if recibirPosicionCliente() and recibirActualizacionMapa():
            print("Orden de viaje recibido, recogiendo al cliente...")
            clienteDentro = False
            realizarViaje()

        time.sleep(1)

def recibirActualizacionMapa():
    global mapa

    consumer = KafkaConsumer(
        'actualizar_mapa',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
    )

    for message in consumer:
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
                    return True
    except Exception as e:
        print(f"Error al recibir la orden del servicio: {e}")

    return False

def enviarPosicionTaxi(taxi_id, posicion, finalizado):
    producerTAXI = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'actualizaciones_taxi'
    message = {'taxi_id': taxi_id, 'taxi_posicion': posicion, 'viaje_finalizado': finalizado}
    producerTAXI.send(topic, message)
    producerTAXI.flush()

def mostrarMapa(mapa):
    for y in range(ALTO):
        fila = []
        for x in range(ANCHO): 
            fila.append(mapa[x][y])
        print(' '.join(fila))

def conexionSensor():
    global permisoMovimiento
    socketSensor = socket.socket()
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

def movimiento(posX, posY, objetivo, movimiento):
    if movimiento == "Vertical":
        ubicacion = posY
        destino = objetivo[1]
    else:
        ubicacion = posX
        destino = objetivo[0]

    posNueva = 0
    distanciaDirecta = abs(ubicacion - destino)
    distanciaPorFuera = ALTO - distanciaDirecta

    if distanciaDirecta < distanciaPorFuera:
        if ubicacion < destino:
            posNueva = (ubicacion + 1) % ALTO
        else:
            posNueva = (ubicacion - 1) % ALTO
    else:
        if ubicacion < destino:
            posNueva = (ubicacion - 1) % ALTO
        else:
            posNueva = (ubicacion + 1) % ALTO

    if movimiento == "Vertical":
        return (posX, posNueva)
    else:
        return (posNueva, posY)

def actualizarPosicion(cliente, destino, mapa):
    global ubicacion
    global clienteDentro

    posX = ubicacion[0]
    posY = ubicacion[1]

    if clienteDentro:
        objetivo = destino
    else:
        objetivo = cliente

    if posX == objetivo[0]:
        return movimiento(posX, posY, objetivo, "Vertical")
    else:
        return movimiento(posX, posY, objetivo, "Horizontal")

def validarID(socketCentral):
    global ubicacion
    socketCentral.send(ID_TAXI.encode())            # Manda a central "START" para que este le mande si la ID solicitada está disponible
    respuesta = socketCentral.recv(4096)
    
    datos = respuesta.decode().strip().replace('<', '').split('>')

    if respuesta.decode() == FIN:                   # El taxi tiene asignado la ID y puede ejercer
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

conectarConServidor()  # Validar ID

# Hilo Sensor
hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

hiloKafka = threading.Thread(target=cerebroDigitalEngine)
hiloKafka.start()

hiloSensor.join()
hiloKafka.join()