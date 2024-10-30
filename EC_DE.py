import socket
import sys
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata
import json
import traceback

# Argumentos y configuraciones
if len(sys.argv) != 8:
    print("ERROR: Número de argumentos incorrecto")
    print("Formato correcto -> python3 EC_DE <IP central> <Puerto central> <IP gestor> <Puerto gestor> <IP Sensor> <Puerto Sensor> <ID Taxi>")
    sys.exit(1)

argumentos = sys.argv
FIN = "<FIN>"
OK = "OK"
KO = "KO"

ALTO = 20
ANCHO = 20

# Evento global de parada
stop_event = threading.Event()

# Variables de configuración y estado
IP_CENTRAL = argumentos[1]
PUERTO_CENTRAL = int(argumentos[2])
IP_GESTOR = argumentos[3]
PUERTO_GESTOR = int(argumentos[4])
IP_SENSOR = argumentos[5]
PUERTO_SENSOR = int(argumentos[6])
ID_TAXI = argumentos[7]

# Variables compartidas
letraUbicacion = None
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
    while not stop_event.is_set():  # Revisa stop_event en cada ciclo
        global permisoMovimiento, clienteDentro, ubicacion, destino, mapa

        if permisoMovimiento:
            mostrarMapa(mapa)
            antiguaPosicion = ubicacion

            if not clienteDentro:
                ubicacion = actualizarPosicion(cliente, destino, mapa)
                enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, OK)

                if ubicacion[0] == cliente[0] and ubicacion[1] == cliente[1]:
                    clienteDentro = True
            else:
                ubicacion = actualizarPosicion(cliente, destino, mapa)
                if ubicacion[0] == destino[0] and ubicacion[1] == destino[1] and not recibirPosicionCliente():
                    enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, True, OK)
                    break
                else:
                    enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, OK)

            while not recibirActualizacionMapa() and not stop_event.is_set():
                time.sleep(0.1)
        else:
            antiguaPosicion = ubicacion
            enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, KO)

        time.sleep(2)

def cerebroDigitalEngine():
    global clienteDentro
    while not stop_event.is_set():  # Revisa stop_event en cada ciclo
        if recibirActualizacionMapa() and recibirPosicionCliente():
            clienteDentro = False
            realizarViaje()

def recibirActualizacionMapa():
    global mapa

    consumer = KafkaConsumer(
        'actualizar_mapa',
        bootstrap_servers=[f"{IP_GESTOR}:{PUERTO_GESTOR}"],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=False  
    )

    for message in consumer:
        if stop_event.is_set():  # Revisa stop_event en cada ciclo de consumo
            return False
        with mapa_lock:
            mapa = message.value
        return True

    return False

def recibirPosicionCliente():
    global cliente, destino, letraUbicacion

    consumer = KafkaConsumer(
        'procesar_viaje',
        bootstrap_servers=[f"{IP_GESTOR}:{PUERTO_GESTOR}"],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='taxi_group'
    )

    try:
        messages = consumer.poll(timeout_ms=1000)
        
        if not messages or stop_event.is_set():
            return False

        for topic_partition, msgs in messages.items():
            for message in msgs:
                info = message.value
                print("Mensaje recibido con ID: " + info['taxi_id'])
                taxi_id = info['taxi_id']
                if taxi_id == ID_TAXI:
                    cliente = info['posCliente']
                    destino = info['posDestino']
                    letraUbicacion = info['letraDestino']
                    consumer.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})
                    print("RECIBIDO ORDEN DE VIAJE")
                    return True
    except Exception as e:
        print(f"Error al recibir la orden del servicio: {e}")
        traceback.print_exc()

    return False

def enviarPosicionTaxi(taxi_id, posicion, antiguaPosicion, finalizado, estadoSensor):
    producerTAXI = KafkaProducer(bootstrap_servers=[f"{IP_GESTOR}:{PUERTO_GESTOR}"],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'actualizaciones_taxi'
    message = {'taxi_id': taxi_id, 'taxi_posicion': posicion, 'viaje_finalizado': finalizado, 'antigua_posicion': antiguaPosicion, 'estado_sensor': estadoSensor}
    producerTAXI.send(topic, message)
    producerTAXI.flush()
    print("POSICION ENVIADA A CENTRAL")

def conexionSensor():
    global permisoMovimiento
    
    socketSensor = socket.socket()
    socketSensor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socketSensor.bind((IP_SENSOR, PUERTO_SENSOR))
    socketSensor.listen(60)

    print(f"SERVIDOR CREADO [{IP_SENSOR} : {PUERTO_SENSOR}]")
    conexion, addr = socketSensor.accept()
    print(f"Conexión hecha con Sensor en {addr}")

    timeout_seconds = 5
    last_message_time = time.time()
    conexion.settimeout(timeout_seconds)

    while not stop_event.is_set():  # Revisa stop_event
        try:
            mensaje = conexion.recv(4096).decode()
            if mensaje == OK:
                permisoMovimiento = True
                last_message_time = time.time()
            elif mensaje == KO:
                permisoMovimiento = False
                last_message_time = time.time()
            elif mensaje == FIN:
                conexion.sendall(FIN.encode())
                print("Recibido FIN, terminando conexión...")
                stop_event.set()  # Activa evento de parada
                break

            print(f"Mensaje Sensor entrante: [{mensaje}]")

            if time.time() - last_message_time > timeout_seconds:
                print("Tiempo de espera agotado. Enviando FIN...")
                enviarPosicionTaxi(ID_TAXI, ubicacion, ubicacion, False, FIN)
                stop_event.set()
                break

        except socket.timeout:
            if time.time() - last_message_time > timeout_seconds:
                print("Tiempo de espera agotado. Enviando FIN...")
                enviarPosicionTaxi(ID_TAXI, ubicacion, ubicacion, False, FIN)
                stop_event.set()
                break
        except Exception as e:
            print(f"Error en conexión con el Sensor: {e}")
            break

    conexion.close()
    socketSensor.close()

def movimiento(posX, posY, objetivo, mapa, letraUbicacion):

    if   posY < objetivo[1] and posX < objetivo[0] and (mapa[posX+1][posY+1] == ' ' or mapa[posX+1][posY+1] == letraUbicacion or (posX+1 == objetivo[0] and posY+1 == objetivo[1])):
        posX += 1
        posY += 1
    elif posY > objetivo[1] and posX > objetivo[0] and (mapa[posX-1][posY-1] == ' ' or mapa[posX-1][posY-1] == letraUbicacion or (posX-1 == objetivo[0] and posY-1 == objetivo[1])):
        posX -= 1
        posY -= 1
    elif posY > objetivo[1] and posX < objetivo[0] and (mapa[posX+1][posY-1] == ' ' or mapa[posX+1][posY-1] == letraUbicacion or (posX+1 == objetivo[0] and posY-1 == objetivo[1])):
        posX += 1
        posY -= 1
    elif posY < objetivo[1] and posX > objetivo[0] and (mapa[posX-1][posY+1] == ' ' or mapa[posX-1][posY+1] == letraUbicacion or (posX-1 == objetivo[0] and posY+1 == objetivo[1])):
        posX -= 1
        posY += 1

    elif posX < objetivo[0] and (mapa[posX+1][posY] == ' ' or mapa[posX+1][posY] == letraUbicacion or (posX+1 == objetivo[0] and posY == objetivo[1])):
        posX += 1
    elif posX > objetivo[0] and (mapa[posX-1][posY] == ' ' or mapa[posX-1][posY] == letraUbicacion or (posX-1 == objetivo[0] and posY == objetivo[1])):
        posX -= 1
    elif posY < objetivo[1] and (mapa[posX][posY+1] == ' ' or mapa[posX][posY+1] == letraUbicacion or (posX == objetivo[0] and posY+1 == objetivo[1])):
        posY += 1
    elif posY > objetivo[1] and (mapa[posX][posY-1] == ' ' or mapa[posX][posY-1] == letraUbicacion or (posX == objetivo[0] and posY-1 == objetivo[1])):
        posY -= 1

    else:
        if mapa[posX-1][posY+1] == ' ':
            posX -= 1
            posY += 1
        elif mapa[posX-1][posY-1] == ' ':
            posX -= 1
            posY -= 1
        elif mapa[posX+1][posY+1] == ' ':
            posX += 1
            posY += 1
        elif mapa[posX+1][posY-1] == ' ':
            posX += 1
            posY -= 1

    return (posX, posY)

def actualizarPosicion(cliente, destino, mapa):
    global ubicacion
    global clienteDentro
    global letraUbicacion

    posX = ubicacion[0]
    posY = ubicacion[1]

    if clienteDentro:
        objetivo = destino
        letraObjetivo = letraUbicacion.upper()
    else:
        objetivo = cliente
        letraObjetivo = letraUbicacion.lower()

    return movimiento(posX, posY, objetivo, mapa, letraObjetivo)

def validarID(socketCentral):
    global ubicacion
    socketCentral.send(ID_TAXI.encode())
    respuesta = socketCentral.recv(4096)

    if respuesta.decode() == FIN:
        print("ERROR: ID del taxi ya está en uso")
        socketCentral.close()
        sys.exit(1)
    else:
        datos = respuesta.decode().strip().replace('<', '').split('>')
        ubicacion = (int(datos[1]), int(datos[2]))
        print("ID asignado con éxito")

def conectarConServidor():
    global socketCentral
    socketCentral = socket.socket()
    socketCentral.connect((IP_CENTRAL, PUERTO_CENTRAL))
    validarID(socketCentral)

############## MAIN ################

conectarConServidor()

hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

hiloKafka = threading.Thread(target=cerebroDigitalEngine)
hiloKafka.start()

hiloSensor.join()
hiloKafka.join()

if stop_event.is_set():
    sys.exit("Programa terminado correctamente.")