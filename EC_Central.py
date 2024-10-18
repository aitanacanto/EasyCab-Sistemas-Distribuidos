import socket
import sys
import threading
import math
import time
from kafka import KafkaProducer, KafkaConsumer

from faker import Faker
import json

################ LO QUE FALTA ##############
# Mirar si el fichero que nos dan cumple con las posiciones correctas en el mapa (mapa.txt)

# Entradas: Puerto de escucha
#           Ip y puerto del gestor de colas
#           Ip y puerto de la base de datos [Si utilizamos BBDD (si utilizamos archivos de texto no hace falta)]

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

mapa = [[' ' for _ in range(10)] for _ in range(10)]

SERVER = socket.gethostbyname(socket.gethostname())
DIRECCION = (SERVER, PUERTO_ESCUCHA)

FIN = "FIN"
OK = "OK"

############ FUNCIONES #############     

def enviarMensajeTaxiMapa(mapa):  # Se pasa "mapa" como parámetro, que es un array de strings

    producerTaxi = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producerTaxi.send('actualizar_mapa', mapa)
    producerTaxi.flush()  

def recibirMensajeTaxi():
    # Configuración del consumidor
    consumer = KafkaConsumer(
        'actualizaciones_taxi',                         # El tópico desde el que se consumen los mensajes
        bootstrap_servers='localhost:9092',             # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Deserializar los datos JSON
        auto_offset_reset='earliest',                   # Leer desde el principio si no hay offset
        group_id='central_group'                        # Grupo de consumidores
    )

    # Bucle para recibir y procesar mensajes
    for message in consumer:
        taxi_status = message.value                     # Obtener el contenido del mensaje
        taxi_id = taxi_status['taxi_id']
        taxi_posicion = taxi_status['taxi_posicion']
        # print(f"Actualización recibida para Taxi {taxi_id}: {taxi_posicion}")
    
    return (taxi_id, taxi_posicion)

def movimientoTaxi(id_taxi, movimiento):                                                # Movimiento puede ser "A"(Arriba)/"B"(Abajo)/"D"(Derecha)/"I"(Izquierda)
    print("Cambiar la posición del taxi con esa ID hacia el movimiento que ha hecho")
    posAntiguaX = 0
    posAntiguaY = 0

    for y in range(ALTO):
        for x in range(ANCHO): 
            if mapa[x][y] == str(id_taxi):
                posAntiguaX = x
                posAntiguaY = y
                break

    posNuevaX = 0
    posNuevaY = 0

    if movimiento == "A":
        posNuevaX = posAntiguaX
        if posAntiguaY == 0:
            posNuevaY = ALTO - 1
        else: 
            posNuevaY = posAntiguaY - 1
            
    elif movimiento == "B":
        posNuevaX = posAntiguaX
        if posAntiguaY == ALTO-1:
            posNuevaY = 0
        else:  
            posNuevaY = posAntiguaY + 1

    elif movimiento == "D":
        posNuevaY = posAntiguaY
        if posAntiguaX == ANCHO-1:
            posNuevaX = 0
        else:
            posNuevaX = posAntiguaX + 1

    else:
        posAntiguaY = posAntiguaY
        if posAntiguaX == 0:
            posNuevaX = ANCHO - 1
        else:
            posNuevaX = posAntiguaX - 1

    return (posNuevaX, posNuevaY)

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

def verificacionTaxi(conn, addr):
    print(f"[NUEVA CONEXION] {conn},{addr} CONECTADO")

    try:
        mensaje = conn.recv(4096)
        id_taxi = mensaje.decode[FORMAT]

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

def actualizarPosicionTaxiFichero(id_taxi, nuevo_x, nuevo_y, finalizado):               # Se actualiza la posición del taxi en la base de datos, se ejecuta cada segundo (finalizado es un bool, dice si se ha acabado el viaje)
    with open(ARCHIVO_TAXI, 'r') as archivo:
        lineas = archivo.readlines()

    taxiActualizado = False
    nuevasLineas = []

    # Recorrer cada línea y buscar el taxi con el ID especificado
    for linea in lineas:
        datos = linea.strip().replace('<', '').split('>')

    if len(datos) >= 3:
        taxi_id = datos[2]  # El ID del taxi está en la segunda posición
        taxi_x = datos[3]   # Posición X actual del taxi
        taxi_y = datos[4]   # Posición Y actual del taxi

        # Si el ID coincide con el taxi que queremos actualizar
        if taxi_id == str(id_taxi):
            # Actualizamos la posición con los nuevos valores
            taxi_x = nuevo_x
            taxi_y = nuevo_y
            taxiActualizado = True

        # Agregar la línea actualizada (o la misma línea si no es el taxi buscado)
        if finalizado:
            nuevasLineas.append(f"<SI><NO><{taxi_id}><{taxi_x}><{taxi_y}>\n")
        else:
            nuevasLineas.append(f"<SI><SI><{taxi_id}><{taxi_x}><{taxi_y}>\n")

    else:
        nuevasLineas.append(linea)  # Agregar las líneas no válidas también

    if not taxiActualizado:
        print(f"No se encontró el taxi con ID {id_taxi} para actualizar.")

    # Reescribir el archivo con los nuevos datos
    with open(ARCHIVO_TAXI, 'w') as archivo:
        archivo.writelines(nuevasLineas)

def recibirPeticionesCustomers():

    consumer = KafkaConsumer(
        'solicitud_taxi',                              # El tópico desde el que va a consumir
        bootstrap_servers='localhost:9092',             # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Decodifica los mensajes recibidos
        auto_offset_reset='earliest',                   # Consume desde el principio si no hay offset
    )

    return consumer

def procesar_peticion_customer():                       # Cola CENTRAL-TAXI informar cliente

    cliente = recibirPeticionesCustomers()
    src = cliente[1]
    cliente_x = src[0]
    cliente_y = src[1]

    taxiAsignado = hayTaxisDisponibles(cliente_x, cliente_y)

    if taxiAsignado:
        producer_taxi_escogido = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        topic = 'procesar_viaje'
        message = {'taxi_id': taxiAsignado[0],'x': cliente_x, 'y': cliente_y}
        producer_taxi_escogido.send(topic, message)
        producer_taxi_escogido.flush()
        print("Enviando el taxi más cercano, Taxi: ", taxiAsignado[0])


def hayTaxisDisponibles(cliente_x, cliente_y): # Sé pasa por parámetro la posición del cliente para escoger el taxi más cercano

    with open(ARCHIVO_TAXI, 'r') as archivo:
        lineas = archivo.readlines()

    if not lineas:
        print("NO HAY TAXIS :(")            # @implementar
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
        print("No se encuentran Taxis libres")     # @implementar
        return None

def mostrarMapa(mapa):                                                                  # GESTION MATRIZ
    for y in range(ALTO):
        fila = []
        for x in range(ANCHO): 
            fila.append(mapa[x][y])

        print(' '.join(fila))

def meterTaxisBaseDatos(arrayPos):
    arrayTaxis = []
    for item in arrayPos:                                                       # Coge solo los taxis del tablero que nos habian pasado
        if item['id'].isdigit():                                                # Seleccionar los taxis del mapa :)
            arrayTaxis.append(item)
            
    with open(ARCHIVO_TAXI, 'w') as archivo:                                    # Guardamos la informacion en el fichero taxis.txt
        for item in arrayTaxis:
            archivo.write(f"<NO><NO><{item['id']}><{item['x']}><{item['y']}>\n")    # No está asociado a ningún taxi / No está haciendo ningún trayecto

def inicMapa(mapa):
    for item in arrayPos:
        mapa[item['x']][item['y']] = str(item['id'])

############### MAIN ###############

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

meterTaxisBaseDatos(arrayPos)
inicMapa(mapa)
mostrarMapa(mapa)
print("[EMPEZANDO] Servidor inicializando...")


# Creamos el hilo para los Taxis y la ejecutamos
hiloTaxi = threading.Thread(target=conexionTaxi)
hiloTaxi.start()

hiloTaxi.join()
