import socket
import sys
import threading
from threading import Lock
import math
import time
from kafka import KafkaProducer, KafkaConsumer
import json

FORMAT = 'utf-8'
ANCHO = 20
ALTO = 20
MAX_CONEXIONES = 22
CUSTOMERS_ACTIVOS = list((letra, False) for letra in 'abcdefghijklmnopqrstuvwxyz')

if len(sys.argv) != 4:
    print("ERROR: Número de argumentos incorrecto")    
    print("Formato correcto -> python3 EC_Central.py <Puerto escucha> <IP gestor> <Puerto gestor>\n")
    exit()

argumentos = sys.argv
PUERTO_ESCUCHA = int(argumentos[1])
IP_GESTOR = argumentos[2]
PUERTO_GESTOR = int(argumentos[3])

ARCHIVO_MAPA = 'mapa.txt'
ARCHIVO_TAXI = 'taxi.txt'
SERVER = socket.gethostbyname(socket.gethostname())
DIRECCION = (SERVER, PUERTO_ESCUCHA)

lock = Lock()

global mapa
mapa = [[' ' for _ in range(ANCHO)] for _ in range(ALTO)]

FIN = "<FIN>"
OK = "OK"

#################### KAFKA #####################

def conexionKafka():
    while True:
        peticion = recibirPeticionesCustomers()
        if peticion is not None:
            taxi = hayTaxisDisponibles(peticion[1], peticion[2])
            if taxi is not None:
                enviarMensajeTaxiOrden(taxi[0], (peticion[1], peticion[2]), (peticion[3], peticion[4]))

        movimientoTaxi = recibirMensajeTaxi()
        if movimientoTaxi is not None:
            with lock:
                cambiarPosicionTaxiMapa(movimientoTaxi[0], movimientoTaxi[1])
                actualizarPosicionTaxiFichero(movimientoTaxi[0], movimientoTaxi[1], movimientoTaxi[2])
                enviarMensajeTaxiMapa(mapa)
def enviarMensajeTaxiMapa(mapa):
    producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producerTaxi.send('actualizar_mapa', value=mapa)
    producerTaxi.flush()
def enviarMensajeTaxiOrden(taxi_id, cliente, destino):
    producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'procesar_viaje'
    message = {'taxi_id': taxi_id, 'posCliente': cliente, 'posDestino': destino}
    producerTaxi.send(topic, value=message)
    producerTaxi.flush()
def recibirMensajeTaxi():
    consumer = KafkaConsumer(
        'actualizaciones_taxi',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='central_group'
    )

    try:
        message = consumer.poll(timeout_ms=1000)
        if not message:
            return None

        for topic_partition, records in message.items():
            for record in records:
                taxi_status = record.value
                taxi_id = taxi_status.get('taxi_id')
                taxi_posicion = taxi_status.get('taxi_posicion')
                viaje_finalizado = taxi_status.get('viaje_finalizado')
                return (taxi_id, taxi_posicion, viaje_finalizado)
    except Exception as e:
        print(f"Error al recibir el mensaje del taxi: {e}")
    finally:
        consumer.close()

    return None
def recibirPeticionesCustomers():
    consumer = KafkaConsumer(
        'solicitud_taxi',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
    )

    try:
        message = consumer.poll(timeout_ms=1000)
        if not message:
            return None

        for topic_partition, records in message.items():
            for record in records:
                info = record.value
                customer_nombre = info['cliente_nombre']
                customer_id = asignar_id_cliente()

                if not existeMismaID(customer_id):
                    return [customer_id, info['ubicacion_x'], info['ubicacion_y'], info['destino_x'], info['destino_y']]
                
                if customer_id is not None:
                    return [customer_id, info['ubicacion_x'], info['ubicacion_y'], info['destino_x'], info['destino_y']]
                else:
                    print("No hay IDs de clientes disponibles.")
                    return None
    finally:
        consumer.close()

    return None
def asignar_id_cliente():
    for i, (letra, estado) in enumerate(CUSTOMERS_ACTIVOS):
        if not estado:
            CUSTOMERS_ACTIVOS[i] = (letra, True) 
            return letra
    return None
def liberar_id_cliente(customer_id):
    for i, (letra, estado) in enumerate(CUSTOMERS_ACTIVOS):
        if letra == customer_id:
            CUSTOMERS_ACTIVOS[i] = (letra, False) 

################## SOCKET TAXI ##################

def validarUbicacion(id_taxi, conn):
    taxiEncontrado = False
    nuevasLineas = []

    try:
        with lock:
            with open(ARCHIVO_TAXI, 'r') as archivo:
                lineas = archivo.readlines()

            for linea in lineas:
                datos = linea.strip().replace('<', '').split('>')

                taxi_asignado = datos[0]
                taxi_operativo = datos[1]
                taxi_id = datos[2]

                if taxi_id == id_taxi:
                    if taxi_asignado == "NO" and taxi_operativo == "NO":
                        taxiEncontrado = True
                        nueva_linea = f"<SI><NO><{taxi_id}><{datos[3]}><{datos[4]}>\n"
                        nuevasLineas.append(nueva_linea)
                        mensaje = "<" + OK + "><"+ datos[3] +"><" + datos[4] + ">"
                        print("Confirmado el ID del taxi: " + mensaje)
                        conn.send(mensaje.encode(FORMAT))
                    else:
                        taxiEncontrado = False
                        nuevasLineas.append(linea)
                else:
                    nuevasLineas.append(linea)

            if not taxiEncontrado:
                conn.send(FIN.encode(FORMAT))
                return

            with open(ARCHIVO_TAXI, 'w') as archivo:
                archivo.writelines(nuevasLineas)

    except Exception as e:
        print(f"EXCEPCIÓN ---> No se ha podido comunicar con el taxi: {e}")
def verificacionTaxi(conn, addr):
    try:
        mensaje = conn.recv(4096)
        id_taxi = mensaje.decode(FORMAT)
        validarUbicacion(id_taxi, conn)
        conn.close()
    except Exception as e:
        print("EXCEPCION ---> NO SE HA PODIDO COMUNICAR CON EL TAXI")
    finally:
        conn.close()
def startTaxi(server):
    server.listen()
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count() - 1
        if CONEX_ACTIVAS <= MAX_CONEXIONES:
            thread = threading.Thread(target=verificacionTaxi, args=(conn, addr))
            thread.start()
        else:
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
def conexionTaxi():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(DIRECCION)
    print(DIRECCION)
    startTaxi(server)

###################### MAPA ######################

def mostrarMapa():
    global mapa
    for y in range(ALTO):
        fila = []
        for x in range(ANCHO):
            fila.append(mapa[x][y])
        print(' '.join(fila))
def inicMapa():
    global mapa
    mapa = [[' ' for _ in range(ANCHO)] for _ in range(ALTO)]
    for item in arrayPos:
        mapa[item['x']][item['y']] = str(item['id'])
def existeMismaID(customer_id):
    global mapa
    for y in range(ALTO):
        for x in range(ANCHO):
            if mapa[x][y] == customer_id:
                return True
    return False
def cambiarPosicionTaxiMapa(taxi_id, taxi_posicion):
    global mapa
    for y in range(ALTO):
        for x in range(ANCHO):
            if mapa[x][y] == str(taxi_id):
                mapa[x][y] = '.'
                break
    mapa[taxi_posicion[0]][taxi_posicion[1]] = str(taxi_id)

################## BASE DE DATOS ##################

def actualizarPosicionTaxiFichero(id_taxi, posicion, finalizado):
    nuevo_x, nuevo_y = posicion
    with lock:
        with open(ARCHIVO_TAXI, 'r') as archivo:
            lineas = archivo.readlines()

        taxiActualizado = False
        nuevasLineas = []

        for linea in lineas:
            datos = linea.strip().replace('<', '').split('>')

            if len(datos) >= 5:
                taxi_id = datos[2]
                if taxi_id == str(id_taxi):
                    taxi_x = str(nuevo_x)
                    taxi_y = str(nuevo_y)
                    taxiActualizado = True

                    if finalizado:
                        nueva_linea = f"<SI><NO><{taxi_id}><{taxi_x}><{taxi_y}>\n"
                    else:
                        nueva_linea = f"<SI><SI><{taxi_id}><{taxi_x}><{taxi_y}>\n"
                    nuevasLineas.append(nueva_linea)
                else:
                    nuevasLineas.append(linea)
            else:
                nuevasLineas.append(linea)

        if not taxiActualizado:
            print(f"No se encontró el taxi con ID {id_taxi} para actualizar.")

        with open(ARCHIVO_TAXI, 'w') as archivo:
            archivo.writelines(nuevasLineas)
def hayTaxisDisponibles(cliente_x, cliente_y):
    with open(ARCHIVO_TAXI, 'r') as archivo:
        lineas = archivo.readlines()

    if not lineas:
        return None

    taxiMasCercano = None
    minDistancia = float('inf')

    for linea in lineas:
        datos = linea.strip().replace('<', '').split('>')

        if len(datos) >= 5:
            taxi_asignado = datos[0]
            taxi_operativo = datos[1]
            taxi_id = datos[2]
            taxi_x = int(datos[3])
            taxi_y = int(datos[4])

            if taxi_asignado == "SI" and taxi_operativo == "SI":
                distancia = math.sqrt((taxi_x - cliente_x) ** 2 + (taxi_y - cliente_y) ** 2)

                if distancia < minDistancia:
                    minDistancia = distancia
                    taxiMasCercano = (taxi_id, taxi_x, taxi_y)

    if taxiMasCercano:
        return taxiMasCercano
    else:
        return None
def meterTaxisBaseDatos(arrayPos):
    arrayTaxis = []
    for item in arrayPos:
        if item['id'].isdigit():
            arrayTaxis.append(item)

    with open(ARCHIVO_TAXI, 'w') as archivo:
        for item in arrayTaxis:
            archivo.write(f"<NO><NO><{item['id']}><{item['x']}><{item['y']}>\n")
def leerMapaFichero():
    arrayPos = []

    with open(ARCHIVO_MAPA, 'r') as archivo:
        for linea in archivo:
            datos = linea.strip().replace('<', '').replace('>', '').split()

            if len(datos) == 3:
                id_localizacion = datos[0]
                posicion_x = int(datos[1]) - 1
                posicion_y = int(datos[2]) - 1

                arrayPos.append({
                    'id': id_localizacion,
                    'x': posicion_x,
                    'y': posicion_y
                })
            else:
                print(f"Línea malformada: {linea}")

    return arrayPos

arrayPos = leerMapaFichero()
meterTaxisBaseDatos(arrayPos)
inicMapa()

print("[EMPEZANDO] Servidor inicializando...")

hiloTaxi = threading.Thread(target=conexionTaxi)
hiloTaxi.start()

#hiloKafka = threading.Thread(target=conexionKafka)
#hiloKafka.start()

hiloTaxi.join()
#hiloKafka.join()
