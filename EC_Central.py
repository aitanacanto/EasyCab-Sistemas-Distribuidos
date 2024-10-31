import socket
import sys
import threading
import math
import time
from kafka import KafkaProducer, KafkaConsumer, OffsetAndMetadata
import json
import mapaPygame
from threading import Lock

lock = threading.Lock()


############ COSAS A MEJORARA ############
# Vaciar la mensajería de las solicitudes del customer a la central

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
ARCHIVO_DESTINOS = 'EC_locations.json'
DIRECCION = (IP_GESTOR, PUERTO_ESCUCHA)

global mapa
global listaEsperaClientes
listaEsperaClientes = []

FIN = "<FIN>"
OK = "OK"

#################### KAFKA #####################

def meterEnMapa(pos_x, pos_y, elemento):
    mapa[pos_x][pos_y] = elemento
    mapaPygame.actualizar_mapa_pygame(mapa)

def envioMuchasPeticiones(peticion, customerXtaxis, peticionesXtaxis, taxi):
    global viajeTerminado
    # peticion = (customer_id, destinos)
    
    print(f"Enviando ordenes al taxi {str(taxi[0])}: {str(peticion)}")    
    cambiarEstadoTaxiFichero(taxi[0], "NO")
    
    for unDestino in peticion:
        enviarMensajeTaxiMapa()
        with lock:
            viajeTerminado = False
            customerXtaxis.append((peticion[0], taxi[0]))
            peticionesXtaxis.append((peticion,taxi[0]))
            meterEnMapa(unDestino[1], unDestino[2], unDestino[0])
            enviarMensajeTaxiOrden(taxi[0],(unDestino[1], unDestino[2]), unDestino[0])
        enviarMensajeTaxiMapa()
        
        while True:
            with lock:
                if viajeTerminado:
                    break
            time.sleep(0.1)
            
    cambiarEstadoTaxiFichero(taxi[0], "SI")

def conexionKafka():
    global listaEsperaClientes, viajeTerminado

    peticionesXtaxis = []
    customerXtaxis = []

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
            for peticion in arrayPeticiones:
                
                peticionf = list(peticion)
                peticionf.pop(0)  
                peticion = tuple(peticionf)  
                
                peticion = peticion[0]  
                
                print("Peticion recibido: " +str(peticion))

                taxi = hayTaxisDisponibles(peticion[0][1], peticion[0][2])

                if taxi is not None:
                    hiloSolicitudes = threading.Thread(target=envioMuchasPeticiones, args=(peticion, customerXtaxis, peticionesXtaxis, taxi))
                    hiloSolicitudes.start()
                else:
                    listaEsperaClientes.append(peticion)
                

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

                cambiarPosicionTaxiMapa(movimientoTaxi[0], movimientoTaxi[1], movimientoTaxi[3], movimientoTaxi[4])
                actualizarPosicionTaxiFichero(movimientoTaxi[0], movimientoTaxi[1], movimientoTaxi[2], movimientoTaxi[4])
                enviarMensajeTaxiMapa()

def enviarMensajeTaxiMapa():
    global mapa
    with lock:
        producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producerTaxi.send('actualizar_mapa', value=mapa)
        producerTaxi.flush()
    print("Mapa actualizado enviado")
    
def enviarMensajeTaxiOrden(taxi_id, destino, letraDestino):
    producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'procesar_viaje'
    message = {'taxi_id': taxi_id, 'posDestino': destino, 'letraDestino': letraDestino}
    producerTaxi.send(topic, key=taxi_id.encode(), value=message)
    producerTaxi.flush()
    print("Enviado orden de viaje al Taxi " + str(taxi_id))

def enviarMensajeCustomer(codigoMensaje, customerID, mensaje):
    producerCustomer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'mensajes_a_consumer'
    message = {'codigoMensaje': codigoMensaje, 'customerID': customerID, 'mensaje': mensaje}
    producerCustomer.send(topic, value=message)
    producerCustomer.flush()

def recibirMensajeTaxi():
    consumer = KafkaConsumer(
        'actualizaciones_taxi',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',  # Leer desde el principio si es necesario
        enable_auto_commit=True,        # Permite confirmar automáticamente los offsets
        group_id='central_taxi_group'   # Asegurar que el grupo es el adecuado
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
                    print("RECIBIDO ORDEN")
                    taxi_status = record.value
                    taxi_id = taxi_status.get('taxi_id')
                    taxi_posicion = taxi_status.get('taxi_posicion')
                    viaje_finalizado = taxi_status.get('viaje_finalizado')
                    antigua_posicion = taxi_status.get('antigua_posicion')
                    estado_sensor = taxi_status.get('estado_sensor')
                    peticion = (taxi_id, taxi_posicion, viaje_finalizado, antigua_posicion, estado_sensor)
                    peticiones.append(peticion)
                    enviarMensajeTaxiMapa()

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
                lista_ubicaciones.append((letra, x, y))
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print("Error al leer el archivo:", e)
        return []

    solucion = []

    for letra in lista_letras:
        for destino in lista_ubicaciones:
            if letra == destino[0]:
                solucion.append(destino)
                meterEnMapa(destino[1], destino[2], destino[0])

    if lista_ubicaciones:
        meterEnMapa(lista_ubicaciones[0][1], lista_ubicaciones[0][2], lista_ubicaciones[0][0])
    
    return solucion

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

    try:
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
                    nueva_linea = f"<SI><SI><{taxi_id}><{datos[3]}><{datos[4]}>\n"
                    nuevasLineas.append(nueva_linea)
                    mensaje = "<" + OK + "><"+ datos[3] +"><" + datos[4] + ">"
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

def cambiarPosicionTaxiMapa(taxi_id, taxi_posicion, antigua_posicion, estado_sensor):
    # Limpiar la antigua posición del taxi
    mapa[antigua_posicion[0]][antigua_posicion[1]] = ' '
    # Añadir el nuevo estado del taxi en la nueva posición
    if estado_sensor == OK:
        mapa[taxi_posicion[0]][taxi_posicion[1]] = f"{taxi_id}v"
    else:
        mapa[taxi_posicion[0]][taxi_posicion[1]] = f"{taxi_id}r"

    mapaPygame.actualizar_mapa_pygame(mapa)  # Actualizar Pygame después de modificar el mapa

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
                #Aitana ha hecho esto

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

                    if finalizado:
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

    return taxiMasCercano

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

arrayPos = leerMapaFichero()
meterTaxisBaseDatos(arrayPos)
inicMapa(arrayPos)

print("[EMPEZANDO] Servidor inicializando...")

mapaPygame.iniciar_pygame(ANCHO, ALTO)

hiloTaxi = threading.Thread(target=conexionTaxi)
hiloTaxi.start()

hiloKafka = threading.Thread(target=conexionKafka)
hiloKafka.start()

mapaPygame.main_pygame_loop()

hiloTaxi.join()
hiloKafka.join()