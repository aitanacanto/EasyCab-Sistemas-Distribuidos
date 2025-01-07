# Levan Kipiani, Aitana Cantó
import socket
import sys
import threading
import requests
import time
import secrets
from kafka import KafkaProducer, KafkaConsumer, OffsetAndMetadata
import json
from flask import Flask, request, jsonify # type: ignore
import ast
import mapaPygame
import EC_API
from cryptography.fernet import Fernet
import logging
from datetime import datetime
import os

ARCHIVO_TOKENS = 'token.txt'
ARCHIVO_CLAVES = 'claves.txt'

try:
    if os.path.exists(ARCHIVO_CLAVES):
        os.remove(ARCHIVO_CLAVES)
        print(f"Archivo {ARCHIVO_CLAVES} eliminado.")
    if os.path.exists(ARCHIVO_TOKENS):
        os.remove(ARCHIVO_TOKENS)
        print(f"Archivo {ARCHIVO_TOKENS} eliminado.")
except Exception as e:
    print(f"Error al eliminar archivos: {e}")

lock = threading.Lock()

ANCHO = 20
ALTO = 20

FORMAT = 'utf-8'
MAX_CONEXIONES = 22
CUSTOMERS_ACTIVOS = list((letra, False) for letra in 'abcdefghijklmnopqrstuvwxyz')
TAMANO_CELDA = 30
ANCHO_BORDE = 1
REFRESCO_MAPA = 1

global viajeTerminado
viajeTerminado = False

global estadoEmergencia
estadoEmergencia = False

if len(sys.argv) != 5:
    print("ERROR: Número de argumentos incorrecto")    
    print("Formato correcto -> python3 EC_Central.py <Puerto escucha> <IP gestor> <Puerto gestor> <IP EC_CTC>\n")
    exit()

argumentos = sys.argv
PUERTO_ESCUCHA = int(argumentos[1])
IP_GESTOR = argumentos[2]
PUERTO_GESTOR = int(argumentos[3])
IP_CTC = argumentos[4]

estado_trafico = "OK"  # Variable global para el estado del tráfico

ARCHIVO_MAPA = 'mapa.txt'
ARCHIVO_TAXI = 'taxi.txt'
ARCHIVO_DESTINOS = 'EC_locations.json'
DIRECCION = (IP_GESTOR, PUERTO_ESCUCHA)

URL_TRAFICO = f"http://{IP_CTC}:5000/trafico"

global mapa
global listaEsperaClientes
listaEsperaClientes = []

FIN = "<FIN>"
OK = "OK"

################## AUDITORÍA ##################

logging.basicConfig(filename='audit_log.txt', level=logging.INFO, format='%(message)s')

def registrar_evento(origen_ip, accion, detalles):
    evento = {
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "source_ip": origen_ip,
        "event": accion,
        "details": detalles
    }
    logging.info(evento)

################### TOKENS ####################

def leer_tokens():
    tokens = {}
    try:
        with open(ARCHIVO_TOKENS, "r") as archivo:
            for linea in archivo:
                taxi_id, token = linea.strip().split(":")
                tokens[taxi_id] = token
    except FileNotFoundError:
        pass  # Si el archivo no existe, devuelve un diccionario vacío
    except Exception as e:
        print(f"Error al leer el archivo de tokens: {e}")
    return tokens

def generar_token():
    return secrets.token_hex(32)  

################# TEMPERATURA #################

def consultar_estado_trafico():
    try:
        respuesta = requests.get(URL_TRAFICO)
        respuesta.raise_for_status()
        datos = respuesta.json()
        return datos.get("estado_trafico", "KO")
    except requests.exceptions.RequestException as e:
        print(f"Error al consultar el estado del tráfico: {e}")
        return "KO"

def activarEmergencia():
    global estadoEmergencia

    estadoEmergencia = True
    print("Estado de emergencia activado. Todos los taxis deben ir a la base.")
    enviarTodosTaxisBase()

def verificarEstadoTrafico():
    global estadoEmergencia

    while True:
        estado_trafico = consultar_estado_trafico()
        print(f"Estado del tráfico: {estado_trafico}")

        if estado_trafico == "KO" and not estadoEmergencia:
            registrar_evento("127.0.0.1", "Cambio en estado del tráfico", {"estado": "KO"})
            activarEmergencia()
            
        if estado_trafico == "OK" and estadoEmergencia:
            estadoEmergencia = False

        time.sleep(10)  # Consulta periódica

################ VUELTA A BASE #################

def enviarTodosTaxisBase():
    try:
        with open(ARCHIVO_TAXI, 'r') as archivo:
            lineas = archivo.readlines()

        for linea in lineas:
            datos = linea.strip().replace('<', '').split('>')
            taxi_id = datos[2]  # ID del taxi
            taxi_asignado = datos[0]  # "SI" o "NO"

            if taxi_asignado == "SI":
                vueltaBase(taxi_id)

    except Exception as e:
        print(f"Error al procesar los taxis: {e}")

    return 

def vueltaBase(taxi_id):
    
    producerTaxi = KafkaProducer(
        bootstrap_servers='localhost:9092',
        # Ojo, ahora en vez de hacer value_serializer de JSON
        # lo vamos a mandar en bytes ya cifrados
        value_serializer=lambda v: v
    )
    topic = 'procesar_viaje'

    # Creamos el mensaje en claro
    clear_message = {
        'taxi_id': taxi_id,
        'posDestino': (1, 1),
        'letraDestino': "Base",
        'estadoEmergencia': True
    }

    # 1) Leemos la clave para este taxi
    key_str = leer_clave_de_fichero(taxi_id)
    # 2) Ciframos
    encrypted_message = encrypt_message(clear_message, key_str)

    # 3) Enviamos el mensaje cifrado
    producerTaxi.send(topic, key=taxi_id.encode(), value=encrypted_message)
    producerTaxi.flush()
    print(f"Enviado orden de ir a la BASE cifrado al taxi: {taxi_id}")

#################### KAFKA #####################

def meterEnMapa(pos_x, pos_y, elemento, estado_taxi):

    mapa[pos_x][pos_y] = elemento
    mapa[1][1] = 'X'
    mapaPygame.actualizar_mapa_pygame(mapa, estado_taxi)
    EC_API.actualizar_mapa(mapa)
    
def envioMuchasPeticiones(peticion, customerXtaxis, peticionesXtaxis, taxi):
    global viajeTerminado, estadoEmergencia

    print(f"Enviando ordenes al taxi {str(taxi[0])}: {str(peticion)}")
    cambiarEstadoTaxiFichero(taxi[0], "NO")
    
    for unDestino in peticion:
        if not estadoEmergencia:
            # Verificar si hay emergencia
            if estadoEmergencia:
                print(f"Emergencia activada. Taxi {taxi[0]} enviado directamente a la base.")
                vueltaBase(taxi[0])
                return

            enviarMensajeTaxiMapa()
            with lock:
                viajeTerminado = False
                customerXtaxis.append((peticion[0], taxi[0]))
                peticionesXtaxis.append((peticion, taxi[0]))
                estados_actualizados = leerEstadosDeTaxis(OK, unDestino[0])                     # Estado, destino
                meterEnMapa(unDestino[1], unDestino[2], unDestino[0], estados_actualizados)
                enviarMensajeTaxiOrden(taxi[0], (unDestino[1], unDestino[2]), unDestino[0])
            enviarMensajeTaxiMapa()
            
            while True:
                with lock:
                    if viajeTerminado or estadoEmergencia:
                        if estadoEmergencia:
                            print(f"Interrumpiendo viaje. Taxi {taxi[0]} debe ir a la base.")
                            vueltaBase(taxi[0])
                        break
                time.sleep(0.1)
        else:
            cambiarEstadoTaxiFichero(taxi[0], "SI")
            
    cambiarEstadoTaxiFichero(taxi[0], "SI")

def conexionKafka():
    global listaEsperaClientes, viajeTerminado

    peticionesXtaxis = []
    customerXtaxis = []
    
    destino = ""

    while True:

        if listaEsperaClientes:
            peticiones = listaEsperaClientes
            listaEsperaClientes = []
        else:
            peticiones = recibirPeticionesCustomers()       # (customer_id, array_destinos)

        arrayPeticiones = []

        if isinstance(peticiones, tuple):
            arrayPeticiones.append(peticiones)
        else:
            arrayPeticiones = peticiones

        if arrayPeticiones:

            print("-----------------------------------")
            print("Peticiones: " + str(arrayPeticiones))
            print("-----------------------------------")

            for peticion in arrayPeticiones:
                
                peticionf = list(peticion)
                peticionf.pop(0)  
                peticion = tuple(peticionf)

                peticion = peticion[0]

                if isinstance(peticion, tuple):
                    break

                taxi = hayTaxisDisponibles()

                if taxi is not None:
                    hiloSolicitudes = threading.Thread(target=envioMuchasPeticiones, args=(peticion, customerXtaxis, peticionesXtaxis, taxi))
                    hiloSolicitudes.start()
                else:
                    listaEsperaClientes.append(peticion)
                    time.sleep(4)                                   # Lo ponemos en la lista de espera y nos esperamos 4 segundos
                

        movimientosTaxi = recibirMensajeTaxi()
        
        if movimientosTaxi:
            for movimientoTaxi in movimientosTaxi:

                if movimientoTaxi[4] == FIN:
                    for tupla in peticionesXtaxis:
                        if tupla[1] == movimientoTaxi[0]:
                            listaEsperaClientes.append(tupla[0])

                if movimientoTaxi[2]:
                    for tupla in customerXtaxis:
                        if tupla[1] == movimientoTaxi[0]:
                            enviarMensajeCustomer('2', tupla[0], "FINALIZADO")
                            with lock:
                                viajeTerminado = True
                            customerXtaxis.remove(tupla)

                                        # taxi_id,          taxi_posicion,      antigua_posicion, estado_sensor     letra
                cambiarPosicionTaxiMapa(movimientoTaxi[0], movimientoTaxi[1], movimientoTaxi[3], movimientoTaxi[4], movimientoTaxi[5])
                actualizarPosicionTaxiFichero(movimientoTaxi[0], movimientoTaxi[1], movimientoTaxi[2], movimientoTaxi[4])
                enviarMensajeTaxiMapa()

def enviarMensajeTaxiMapa():
    global mapa
    with lock:
        producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producerTaxi.send('actualizar_mapa', value=mapa)
        producerTaxi.flush()

import ast
import json
from cryptography.fernet import Fernet

def leer_clave_de_fichero(taxi_id):
    try:
        with open("claves.txt", "r") as f:
            for linea in f:
                partes = linea.strip().split(":")  # ["1", "b'...'"]
                if partes[0] == taxi_id:
                    # Convertir clave en formato "b'...'" a bytes reales
                    key_bytes = ast.literal_eval(partes[1])
                    return key_bytes
        print(f"Clave no encontrada para el taxi {taxi_id}.")
    except Exception as e:
        print(f"Error al leer la clave del fichero: {e}")
    return None

def encrypt_message(message_dict, key_bytes):
    """
    Cifra un diccionario (message_dict) con la clave (key_bytes).
    Devuelve bytes cifrados.
    """
    try:
        # Convertir la clave a objeto Fernet
        f = Fernet(key_bytes)
        # Convertir el diccionario a JSON y luego a bytes
        message_bytes = json.dumps(message_dict).encode('utf-8')
        # Cifrar
        encrypted_bytes = f.encrypt(message_bytes)
        return encrypted_bytes
    except Exception as e:
        print(f"Error al cifrar el mensaje: {e}")
        return None

def enviarMensajeTaxiOrden(taxi_id, destino, letraDestino):
    global estadoEmergencia
    try:
        # Leer clave del archivo
        key_bytes = leer_clave_de_fichero(taxi_id)
        if not key_bytes:
            print(f"No se pudo obtener la clave para el taxi {taxi_id}.")
            return

        # Crear el mensaje a enviar
        message = {
            'taxi_id': taxi_id,
            'posDestino': destino,
            'letraDestino': letraDestino,
            'estadoEmergencia': estadoEmergencia
        }

        # Cifrar el mensaje
        encrypted_message = encrypt_message(message, key_bytes)
        if not encrypted_message:
            print(f"No se pudo cifrar el mensaje para el taxi {taxi_id}.")
            return

        # Enviar mensaje cifrado a Kafka
        producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: v)
        topic = 'procesar_viaje'
        producerTaxi.send(topic, key=taxi_id.encode(), value=encrypted_message)
        producerTaxi.flush()
        print("Enviado orden de viaje al Taxi " + str(taxi_id))
    except Exception as e:
        print(f"Error al enviar el mensaje al taxi {taxi_id}: {e}")


def enviarMensajeCustomer(codigoMensaje, customerID, mensaje):
    producerCustomer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'mensajes_a_consumer'
    message = {'codigoMensaje': codigoMensaje, 'customerID': customerID, 'mensaje': mensaje}
    producerCustomer.send(topic, value=message)
    producerCustomer.flush()

def recibirMensajeTaxi():
    global estadoEmergencia
    
    consumer = KafkaConsumer(
        'actualizaciones_taxi',
        bootstrap_servers='localhost:9092',
        # value_deserializer=None -> Porque vamos a recibir bytes cifrados
        value_deserializer=lambda v: v, 
        auto_offset_reset='earliest', 
        enable_auto_commit=True,        
        group_id='central_taxi_group'
    )

    peticiones = []
    start_time = time.time()

    try:
        while time.time() - start_time < REFRESCO_MAPA:
            message = consumer.poll(timeout_ms=100)
            
            if not message:
                continue

            for topic_partition, records in message.items():
                for record in records:
                    print("RECIBIDO ORDEN CIFRADA")
                    
                    # 1) Obtenemos el taxi_id a partir de la key
                    #    (porque record.value está cifrado y no lo podemos parsear tal cual)
                    if record.key:
                        taxi_id = record.key.decode('utf-8')
                    else:
                        taxi_id = None
                    
                    if not taxi_id:
                        print("No se ha recibido taxi_id en la key. Ignorando mensaje.")
                        continue

                    # 2) Descifrar el value (record.value) con la clave de este taxi
                    key_str = leer_clave_de_fichero(taxi_id)
                    try:
                        taxi_status = decrypt_message(record.value, key_str)
                    except Exception as e:
                        print(f"Error descifrando el mensaje para taxi {taxi_id}: {e}")
                        continue

                    # 3) Ahora ya procesas igual que antes
                    print(f"Mensaje descifrado para taxi {taxi_id}: {taxi_status}")

                    # ...
                    # (El resto sigue igual, usando 'taxi_status.get(...)')
                    # ...
                    token = taxi_status.get('token')
                    
                    tokens = leer_tokens()
                    if taxi_id in tokens and tokens[taxi_id] == token:
                        print(f"Token válido para taxi {taxi_id}. Procesando mensaje.")
                        peticion = (
                            taxi_id, 
                            taxi_status.get('taxi_posicion'), 
                            taxi_status.get('viaje_finalizado'), 
                            taxi_status.get('antigua_posicion'), 
                            taxi_status.get('estado_sensor'),
                            taxi_status.get('letra')
                        )
                        peticiones.append(peticion)
                    else:
                        print(f"Token inválido para taxi {taxi_id}. Ignorando mensaje.")

        return peticiones

    except Exception as e:
        print(f"Error al recibir el mensaje del taxi: {e}")
    finally:
        consumer.close()

    return None

def obtener_lista_ubicaciones(ruta_archivo, lista_letras):
    lista_ubicaciones = []
    
    try:
        with open(ruta_archivo, 'r') as archivo:
            datos = json.load(archivo)
            for location in datos["locations"]:
                letra = location["Id"]
                x, y = map(int, location["POS"].split(","))
                if x <= ANCHO and x > 0 and y <= ALTO and y > 0:
                    lista_ubicaciones.append((letra, x-1, y-1))
                else:
                    print("Error. Revisa el json de localizaciones.")
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print("Error al leer el archivo:", e)
        return []

    solucion = []

    for letra in lista_letras:
        for destino in lista_ubicaciones:
            if letra == destino[0]:
                solucion.append(destino)
                estados_actualizados = leerEstadosDeTaxis(OK, letra)    # Estado, destino
                meterEnMapa(destino[1], destino[2], destino[0], estados_actualizados)

    if lista_ubicaciones:
        estados_actualizados = leerEstadosDeTaxis(OK, letra) 
        meterEnMapa(lista_ubicaciones[0][1], lista_ubicaciones[0][2], lista_ubicaciones[0][0], estados_actualizados)
        
    estados_actualizados = leerEstadosDeTaxis(OK, letra) 
    return solucion

def leerEstadosDeTaxis(estado, destino):
    
    estados = []
    with open(ARCHIVO_TAXI, 'r') as archivo:
        lineas = archivo.readlines()

    if not lineas:
        return None

    for linea in lineas:
        datos = linea.strip().replace('<', '').split('>')

        if len(datos) >= 5:
            taxi_asignado = datos[0]
            taxi_id = datos[2]
            
            if taxi_asignado == "SI":
                solicitud = {
                    "id": taxi_id,
                    "destino": destino,            # Implementar para poner el destino al que va
                    "estado": estado
                }
            
                estados.append(solicitud)

    return estados

def recibirPeticionesCustomers():
    consumer = KafkaConsumer(
        'solicitud_taxi',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',  
        group_id='central_group',  
        enable_auto_commit=False  
    )

    peticiones = []
    start_time = time.time()

    try:
        while time.time() - start_time < REFRESCO_MAPA:
            messages = consumer.poll(timeout_ms=100)

            if not messages:
                continue
            
            for topic_partition, records in messages.items():
                for record in records:
                    info = record.value
                    customer_id = asignar_id_cliente()

                    if not existeMismaID(customer_id):
                        consumer.commit({topic_partition: OffsetAndMetadata(record.offset + 1, None)})
                        
                        numero_identificacion = info['numero_identificacion']
                        enviarMensajeCustomer('1', customer_id, str(numero_identificacion))
                        
                        destinos = info['destinos']

                        posCliente = destinos[0]['POS']
                        
                        destinos.pop(0)
                        
                        array_destinos = obtener_lista_ubicaciones(ARCHIVO_DESTINOS, destinos)
                        
                        array_destinos.insert(0, (customer_id, posCliente[0], posCliente[1]))
                        
                        peticion = (customer_id, array_destinos)
                        peticiones.append(peticion)
                            
                    else:
                        print("No hay IDs de clientes disponibles.")
                        return None
                    
        return peticiones

    except Exception as e:
        print(f"Error al recibir peticiones: {e}")

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
    origen_ip = conn.getpeername()[0]  # Obtiene la IP del cliente

    try:
        with open(ARCHIVO_TAXI, 'r') as archivo:
            lineas = archivo.readlines()

        for linea in lineas:
            datos = linea.strip().replace('<', '').split('>')
            taxi_id = datos[2]
            pos_x, pos_y = datos[3], datos[4]

            if taxi_id == id_taxi:
                taxiEncontrado = True
                nuevo_token = generar_token()
                nueva_clave = generar_cifrado()
                mensaje = f"<{OK}><{pos_x}><{pos_y}><{nuevo_token}><{nueva_clave.decode()}>"
                conn.send(mensaje.encode(FORMAT))

                # Actualizar el archivo de tokens
                with open(ARCHIVO_TOKENS, 'a') as archivo_tokens:
                    archivo_tokens.write(f"{taxi_id}:{nuevo_token}\n")
                    
                with open(ARCHIVO_CLAVES, 'a') as archivo_claves:
                    archivo_claves.write(f"{taxi_id}:{nueva_clave}\n")

            else:
                nuevasLineas.append(linea)

        if not taxiEncontrado:
            registrar_evento(origen_ip, "Autenticación fallida", {"taxi_id": id_taxi})
            conn.send(FIN.encode(FORMAT))
            return

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
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(DIRECCION)
        print(DIRECCION)
        startTaxi(server)

    except Exception as e:
        print("EXCEPCION ---> Dirección incorrecta.")

###################### MAPA ######################

def inicMapa(arrayPos):
    global mapa
    mapa = [[' ' for _ in range(ANCHO)] for _ in range(ALTO)]
    for item in arrayPos:
        if item['id'].isdigit():
            mapa[item['x']][item['y']] = (str(item['id']) + 'r')

def existeMismaID(customer_id):
    global mapa
    for y in range(ALTO):
        for x in range(ANCHO):
            if mapa[x][y] == customer_id:
                return True
    return False

def cambiarPosicionTaxiMapa(taxi_id, taxi_posicion, antigua_posicion, estado_sensor, destino):
    mapa[antigua_posicion[0]][antigua_posicion[1]] = ' '
    if estado_sensor == OK:
        mapa[taxi_posicion[0]][taxi_posicion[1]] = f"{taxi_id}v"
    else:
        mapa[taxi_posicion[0]][taxi_posicion[1]] = f"{taxi_id}r"

    estado_taxi = leerEstadosDeTaxis(estado_sensor, destino)
    mapaPygame.actualizar_mapa_pygame(mapa, estado_taxi)
    EC_API.actualizar_mapa(mapa)

################## BASE DE DATOS ##################

def cambiarEstadoTaxiFichero(id_taxi, operativo):
    with lock:
        taxiEncontrado = False
        nuevasLineas = []
        
        try:
            with open(ARCHIVO_TAXI, 'r') as archivo:
                lineas = archivo.readlines()

            for linea in lineas:
                datos = linea.strip().replace('<', '').split('>')
                taxi_asignado = datos[0]
                taxi_id = datos[2]
                taxi_x = datos[3]
                taxi_y = datos[4]
                
                if taxi_id == id_taxi:
                    taxiEncontrado = True
                    nuevaLinea = f"<{taxi_asignado}><{operativo}><{taxi_id}><{taxi_x}><{taxi_y}>\n"
                    nuevasLineas.append(nuevaLinea)
                else:
                    nuevasLineas.append(linea)
            
            with open(ARCHIVO_TAXI, 'w') as archivo:
                archivo.writelines(nuevasLineas)

            if not taxiEncontrado:
                print("No se ha podido cambiar el estado al taxi: " + id_taxi)
                return False
            else:
                return True

        except Exception as e:
            print(f"EXCEPCIÓN ---> No se ha podido comunicar con la base de datos del taxi: {e}")

def actualizarPosicionTaxiFichero(id_taxi, posicion, finalizado, estadoSensor):
    with lock:
        nuevo_x, nuevo_y = posicion
        
        try:
            with open(ARCHIVO_TAXI, 'r') as archivo:
                lineas = archivo.readlines()
        except Exception as e:
            print(f"Error al leer el archivo de taxis: {e}")
            return
        
        taxiActualizado = False
        nuevasLineas = []

        # Procesar cada línea y actualizar la información del taxi si es necesario
        for linea in lineas:
            datos = linea.strip().replace('<', '').split('>')

            # Verificar que la línea tenga al menos 5 elementos
            if len(datos) >= 5:
                taxi_id = datos[2]
                if taxi_id == str(id_taxi):
                    taxi_x = str(nuevo_x)
                    taxi_y = str(nuevo_y)
                    taxiActualizado = True

                    if finalizado and not posicion == (1,1):
                        nueva_linea = f"<SI><SI><{taxi_id}><{taxi_x}><{taxi_y}>\n"
                    elif estadoSensor == FIN:
                        nueva_linea = f"<NO><NO><{taxi_id}><{taxi_x}><{taxi_y}>\n"
                    else:
                        nueva_linea = f"<SI><NO><{taxi_id}><{taxi_x}><{taxi_y}>\n"

                    nuevasLineas.append(nueva_linea)
                else:
                    nuevasLineas.append(linea)
            else:
                # Línea no válida, agregar advertencia opcionalmente
                print(f"Advertencia: línea inválida detectada: {linea.strip()}")
                nuevasLineas.append(linea)

        if not taxiActualizado:
            print(f"No se encontró el taxi con ID {id_taxi} para actualizar.")

        try:
            with open(ARCHIVO_TAXI, 'w') as archivo:
                archivo.writelines(nuevasLineas)
        except Exception as e:
            print(f"Error al actualizar la posición del taxi en el fichero: {e}")

        time.sleep(0.1)

def hayTaxisDisponibles():
    with open(ARCHIVO_TAXI, 'r') as archivo:
        lineas = archivo.readlines()

    if not lineas:
        return None

    for linea in lineas:
        datos = linea.strip().replace('<', '').split('>')

        if len(datos) >= 5:
            taxi_asignado = datos[0]
            taxi_operativo = datos[1]
            taxi_id = datos[2]
            taxi_x = int(datos[3])
            taxi_y = int(datos[4])

            # Verifica si el taxi está disponible
            if taxi_asignado == "SI" and taxi_operativo == "SI":
                return (taxi_id, taxi_x, taxi_y)

    return None  # Devuelve None si no hay taxis disponibles

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
                posicion_x = int(datos[1])
                posicion_y = int(datos[2])

                arrayPos.append({
                    'id': id_localizacion,
                    'x': posicion_x,
                    'y': posicion_y
                })
            else:
                print(f"Línea malformada: {linea}")

    return arrayPos

############### CIFRADO / DESCIFRADO ##############

def leer_clave_de_fichero(taxi_id):
    with open("claves.txt", "r") as f:
        for linea in f:
            partes = linea.strip().split(":")  # ["1", "b'...'"]
            if partes[0] == taxi_id:
                # Partes[1] es "b'WNLJBbS_9DWAkQQIS6L9F1zc-2Hpqlv-KygYN3OLJBM='"
                # lo convertimos a bytes reales
                key_bytes = ast.literal_eval(partes[1])
                return key_bytes
    return None

def encrypt_message(message_dict, key_str):
    """
    Cifra un diccionario (message_dict) con la clave (key_str).
    Devuelve bytes cifrados.
    """
    # Convertir la clave a objeto Fernet
    f = Fernet(key_str)
    # Convertir el diccionario a JSON y luego a bytes
    message_bytes = json.dumps(message_dict).encode('utf-8')
    # Cifrar
    encrypted_bytes = f.encrypt(message_bytes)
    return encrypted_bytes

def decrypt_message(encrypted_bytes, key_str):
    """
    Descifra el contenido (encrypted_bytes) con la clave (key_str).
    Devuelve un diccionario.
    """
    f = Fernet(key_str)
    decrypted_bytes = f.decrypt(encrypted_bytes)
    # Convertir de bytes a diccionario
    message_dict = json.loads(decrypted_bytes.decode('utf-8'))
    return message_dict

def generar_cifrado():
    return Fernet.generate_key()

arrayPos = leerMapaFichero()
meterTaxisBaseDatos(arrayPos)
inicMapa(arrayPos)

print("[EMPEZANDO] Servidor inicializando...")


mapaPygame.iniciar_pygame(ANCHO, ALTO)

# Inicia la API en un hilo separado
api_thread = threading.Thread(target=EC_API.iniciar_api, daemon=True)
api_thread.start()


hiloTaxi = threading.Thread(target=conexionTaxi)
hiloTaxi.start()

hiloKafka = threading.Thread(target=conexionKafka)
hiloKafka.start()

hiloTrafico = threading.Thread(target=verificarEstadoTrafico, daemon=True)
hiloTrafico.start()

mapaPygame.main_pygame_loop()