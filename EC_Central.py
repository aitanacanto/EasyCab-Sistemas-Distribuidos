import socket
import sys
import threading
import math
import time
from kafka import KafkaProducer, KafkaConsumer

from faker import Faker
import json

# Entradas: Puerto de escucha
#           Ip y puerto del gestor de colas
#           Ip y puerto de la base de datos [Si utilizamos BBDD (si utilizamos archivos de texto no hace falta)]

############# APUNTES #############

# CENTRAL <-> DE (para mapa/posición actualizado): actualizaciones_taxi
# CENTRAL --> DE (para ordenes de clientes)

###################################

FORMAT = 'utf-8'

ANCHO = 20
ALTO = 20
MAX_CONEXIONES = 22

if len(sys.argv) != 4:
    print("ERROR: Número de argumentos arróneos")    
    print("Fomrato correcto -> python3 EC_Central.py <Puerto escucha> <IP gestor> <Puerto gestor>\n")
    exit()

argumentos = sys.argv
PUERTO_ESCUCHA = int(argumentos[1])

IP_GESTOR = argumentos[2]
PUERTO_GESTOR = int(argumentos[3])

ARCHIVO_MAPA = 'mapa.txt'
ARCHIVO_TAXI = 'taxi.txt'

SERVER = socket.gethostbyname(socket.gethostname())
DIRECCION = (SERVER, PUERTO_ESCUCHA)

# Esto es un array de String, en resumen, es el mapa actualizado
global mapa 

FIN = "FIN"
OK = "OK"

############ FUNCIONES #############

def conexionKafka():
    global mapa

    while True:
        peticion = recibirPeticionesCustomers()                                                     #[costumer_id, cliente_x, cliente_y, destino_x, destino_y]

        if peticion is not None:
            taxi = hayTaxisDisponibles(peticion[1], peticion[2])                                        #(taxi_id, taxi_x, taxi_y)
            if taxi is not None:
                enviarMensajeTaxiOrden(taxi[0], (peticion[1], peticion[2]), (peticion[3], peticion[4]))

        movimientoTaxi = recibirMensajeTaxi()                                                           #(taxi_id, taxi_posicion, viaje_finalizado)

        if movimientoTaxi is not None:
            cambiarPosicionTaxiMapa(movimientoTaxi[0], movimientoTaxi[1])
            actualizarPosicionTaxiFichero(movimientoTaxi[0], movimientoTaxi[1], movimientoTaxi[2])
            enviarMensajeTaxiMapa(mapa)

###################### KAFKA #######################
def enviarMensajeTaxiMapa(mapa):                                            # ENVIO A TAXI      Envia mapa actualizado
    producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producerTaxi.send('actualizar_mapa', mapa)
    producerTaxi.flush()  
    return
def enviarMensajeTaxiOrden(taxi_id, cliente, destino):                      # ENVIO A TAXI      Envia tanto el cliente a quien tiene que recoger como al destino al que tiene que irs
    producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'procesar_viaje'
    message = {'taxi_id': taxi_id, 'posCliente': cliente, 'posDestino': destino}
    producerTaxi.send(topic, message)
    producerTaxi.flush()  
    return
def recibirMensajeTaxi():                                                   # RECIBIR DE TAXI           Devuelve (taxi_id, taxi_posicion, viaje_finalizado)
    # Configuración del consumidor
    consumer = KafkaConsumer(
        'actualizaciones_taxi',                                             # El tópico desde el que se consumen los mensajes
        bootstrap_servers='localhost:9092',                                 # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),         # Deserializar los datos JSON
        auto_offset_reset='earliest',                                       # Leer desde el principio si no hay offset
        group_id='central_group'                                            # Grupo de consumidores
    )

    try:
        # Recibir y procesar solo el primer mensaje, luego cerramos el consumidor
        for message in consumer:
            taxi_status = message.value                                     # Obtener el contenido del mensaje
            taxi_id = taxi_status.get('taxi_id')
            taxi_posicion = taxi_status.get('taxi_posicion')
            viaje_finalizado = taxi_status.get('viaje_finalizado')

            # Cerramos el consumidor después de procesar el primer mensaje relevante
            consumer.close()
            return (taxi_id, taxi_posicion, viaje_finalizado)

    except Exception as e:
        print(f"Error al recibir el mensaje del taxi: {e}")

    # En caso de que no se pueda procesar ningún mensaje
    return None
def recibirPeticionesCustomers():                                       # RECIBIR DE CUSTOMER       Devuelve [costumer_id, cliente_x, cliente_y, destino_x, destino_y]
    global mapa
    consumer = KafkaConsumer(
        'solicitud_taxi',                                                       # El tópico desde el que va a consumir
        bootstrap_servers='localhost:9092',                                     # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),             # Decodifica los mensajes recibidos
        auto_offset_reset='earliest',                                           # Consume desde el principio si no hay offset
    )

    for message in consumer:
        info = message.value
        customer_id = info['cliente_id']
        
        if not existeMismaID(customer_id):
            consumer.close()                                                    # Así no leemos todos los mensajes del topico, solo el primero :)
            return [customer_id, info['ubicacion_x'], info['ubicacion_y'], info['destino_x'], info['destino_y']]

    return None

########## SOCKET PARA CONEXIÓN CON TAXIS ##########
def validarUbicacion(id_taxi, conn):
    taxiEncontrado = False
    nuevasLineas = []

    try:
        # Leer todas las líneas del archivo de taxis
        with open(ARCHIVO_TAXI, 'r') as archivo:
            lineas = archivo.readlines()

        for linea in lineas:
            datos = linea.strip().replace('<', '').split('>')

            taxi_asignado = datos[0]                        # Estado de asignación (NO o SI)
            taxi_operativo = datos[1]                       # Estado operativo (NO o SI)
            taxi_id = datos[2]                              # ID del taxi

            if taxi_id == id_taxi:
                taxiEncontrado = True
                if taxi_asignado == "NO" and taxi_operativo == "NO":
                    print(f"ID {id_taxi} asignado al taxi")
                    # Cambiar el primer NO por SI
                    nueva_linea = f"<SI><NO><{taxi_id}><{datos[3]}><{datos[4]}>\n"
                    nuevasLineas.append(nueva_linea)
                    conn.send(OK.encode(FORMAT))  # Enviar confirmación al Digital Engine
                else:
                    # Si ya está asignado o operativo, dejar la línea como está
                    nuevasLineas.append(linea)
            else:
                # Si no es el taxi buscado, mantener la línea sin cambios
                nuevasLineas.append(linea)

        if not taxiEncontrado:
            print(f"ID {id_taxi} no disponible")
            conn.send(FIN.encode(FORMAT))
            return

        # Reescribir el archivo de taxis con las líneas actualizadas
        with open(ARCHIVO_TAXI, 'w') as archivo:
            archivo.writelines(nuevasLineas)

    except Exception as e:
        print(f"EXCEPCIÓN ---> No se ha podido comunicar con el taxi: {e}")

    return
def verificacionTaxi(conn, addr):
    print(f"[NUEVA CONEXION] {conn},{addr} CONECTADO")

    try:
        mensaje = conn.recv(4096)
        id_taxi = mensaje.decode(FORMAT)

        validarUbicacion(id_taxi, conn)

        conn.close()
        
        print("Hay que cambiar en la base de datos el NO de esa ID por un SI porque pasa a estar asociado")

    except Exception as e:
        print("EXCEPCTION ---> NO SE HA PODIDO COMUNICAR CON EL TAXI")

    print("CONEXIÓN CON TAXI DESCONECTADO")
    conn.close()
def startTaxi(server):
    server.listen()
    print(f"[ESCUCHANDO] Servidor a la escucha en {SERVER}")
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count() - 1
        if(CONEX_ACTIVAS <= MAX_CONEXIONES):
            thread = threading.Thread(target=verificacionTaxi, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
def conexionTaxi():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(DIRECCION)
    startTaxi(server)
 
####################### MAPA #######################
def mostrarMapa():
    global mapa                                                                  # GESTION MATRIZ
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
        fila = []
        for x in range(ANCHO): 
            if mapa[x][y] == customer_id:
                return True
    return False
def cambiarPosicionTaxiMapa(taxi_id, taxi_posicion):                 # Actualiza la posición del Taxi en el mapa :)
    global mapa

    for y in range(ALTO):
        for x in range(ANCHO): 
            if mapa[x][y] == str(taxi_id):
                mapa[x][y] = '.'
                break
    mapa[taxi_posicion[0]][taxi_posicion[1]] = str(taxi_id)

    return 

################## BASE DE DATOS ###################
def actualizarPosicionTaxiFichero(id_taxi, posicion, finalizado):
    nuevo_x, nuevo_y = posicion
    with open(ARCHIVO_TAXI, 'r') as archivo:
        lineas = archivo.readlines()

    taxiActualizado = False
    nuevasLineas = []

    # Recorrer cada línea y buscar el taxi con el ID especificado
    for linea in lineas:
        datos = linea.strip().replace('<', '').split('>')
        
        # Comprobamos que tenemos al menos 5 elementos: ['SI', 'SI', 'taxi_id', 'taxi_x', 'taxi_y']
        if len(datos) >= 5:
            taxi_asignado = datos[0]
            taxi_operativo = datos[1]
            taxi_id = datos[2]
            taxi_x = datos[3]
            taxi_y = datos[4]

            # Si el ID coincide con el taxi que queremos actualizar
            if taxi_id == str(id_taxi):
                # Actualizamos la posición con los nuevos valores
                taxi_x = str(nuevo_x)
                taxi_y = str(nuevo_y)
                taxiActualizado = True

                # Dependiendo de si el viaje ha finalizado, actualizamos el estado operativo
                if finalizado:
                    nueva_linea = f"<SI><NO><{taxi_id}><{taxi_x}><{taxi_y}>\n"
                else:
                    nueva_linea = f"<SI><SI><{taxi_id}><{taxi_x}><{taxi_y}>\n"

                nuevasLineas.append(nueva_linea)
            else:
                nuevasLineas.append(linea)
        else:
            # En caso de que la línea esté mal formada, la mantenemos tal cual
            nuevasLineas.append(linea)

    # Si el taxi no fue encontrado, mostrar un mensaje de advertencia
    if not taxiActualizado:
        print(f"No se encontró el taxi con ID {id_taxi} para actualizar.")

    # Reescribir el archivo con los nuevos datos
    with open(ARCHIVO_TAXI, 'w') as archivo:
        archivo.writelines(nuevasLineas)
def hayTaxisDisponibles(cliente_x, cliente_y):                                          # Coge el taxi más cercano al cliente DEVUELVE (taxi_id, taxi_x, taxi_y)
    with open(ARCHIVO_TAXI, 'r') as archivo:
        lineas = archivo.readlines()

    if not lineas:
        return None

    taxiMasCercano = None
    minDistancia = float('inf')             # Es el infinito

    for linea in lineas:
        datos = linea.strip().replace('<', '').split('>')
        
        if len(datos) >= 3:
            taxi_asignado = datos[0]
            taxi_operativo = datos[1]
            taxi_id = datos[2]
            taxi_x = datos[3]
            taxi_y = datos[4]

        if taxi_asignado == "SI" and taxi_operativo == "SI":
            distancia = math.sqrt((taxi_x - cliente_x) ** 2 + (taxi_y - cliente_y) ** 2)

            if distancia < minDistancia:
                minDistancia = distancia
                taxiMasCercano = (taxi_id, taxi_x, taxi_y)

    if taxiMasCercano:
        print("Cambiar el NO por el SI en la operatividad del taxi (posición 2)")   # @Implementar
        return taxiMasCercano

    else:
        return None
def meterTaxisBaseDatos(arrayPos):
    arrayTaxis = []
    for item in arrayPos:                                                       # Coge solo los taxis del tablero que nos habian pasado
        if item['id'].isdigit():                                                # Seleccionar los taxis del mapa :)
            arrayTaxis.append(item)
            
    with open(ARCHIVO_TAXI, 'w') as archivo:                                    # Guardamos la informacion en el fichero taxis.txt
        for item in arrayTaxis:
            archivo.write(f"<NO><NO><{item['id']}><{item['x']}><{item['y']}>\n")    # No está asociado a ningún taxi / No está haciendo ningún trayecto
def leerMapaFichero():
    arrayPos = []

    with open(ARCHIVO_MAPA, 'r') as archivo:                                                # Leer el archivo mapa.txt
        for linea in archivo:
            # Usar split() para manejar espacios en blanco
            datos = linea.strip().replace('<', '').replace('>', '').split()
            
            if len(datos) == 3:  # Asegúrate de que haya exactamente 3 elementos
                id_localizacion = datos[0]  # Puede ser letra o número
                posicion_x = int(datos[1])-1    # Convertir a entero                        # ¡POSIBLE ERROR inicializar sitios con -1!
                posicion_y = int(datos[2])-1    # Convertir a entero    

                arrayPos.append({
                    'id': id_localizacion,
                    'x': posicion_x,
                    'y': posicion_y
                })
            else:
                print(f"Línea malformada: {linea}")

    return arrayPos

####################### MAIN #######################

arrayPos = leerMapaFichero()
meterTaxisBaseDatos(arrayPos)
inicMapa()

print("[EMPEZANDO] Servidor inicializando...")

# Creamos el hilo para los Taxis y la ejecutamos
hiloTaxi = threading.Thread(target=conexionTaxi)
hiloTaxi.start()

hiloKafka = threading.Thread(target=conexionKafka)
hiloKafka.start()

hiloTaxi.join()
hiloKafka.join()