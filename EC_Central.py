import socket
import sys
import threading
import math
import time

################ LO QUE FALTA ##############
# Falta por implementar la comunicación entre el servidor y los clientes y la gestión de dichas solicitudes





# Entradas: Puerto de escucha
#           Ip y puerto del gestor de colas
#           Ip y puerto de la base de datos [Si utilizamos BBDD (si utilizamos archivos de texto no hace falta)]

FORMAT = 'utf-8'

TAXI_MOVIENDO = 0x40  # Bit 6 en 1 para "moviendo"
TAXI_PARADO = 0x00    # Bit 6 en 0 para "parado"
NADA = 0x00           # Una celda vacía

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

mapa = bytearray([NADA] * (ANCHO * ALTO))

SERVER = socket.gethostbyname(socket.gethostname())
DIRECCION = (SERVER, PUERTO_ESCUCHA)
FIN = "FIN"

############ FUNCIONES #############                 

def gestorTaxi(conn, addr):
    global mapa

    print(f"[NUEVA CONEXION] {conn} CONECTADO")

    conectado = True
    while conectado:   
        try:
            # @KAFKA -> Enviar el mapa al DigitalEngine
            time.sleep(1)
            # @KAFKA -> Recibir el mapa actualizado del DigitalEngine
            #if msg == FIN:
            #    conectado = False
            print(f" He recibido del cliente [{addr}] el mapa actualizado :)")
        except Exception as e:
            break

    print("CONEXIÓN CON TAXI DESCONECTADO")
    conn.close()

def startTaxi(server):
    server.listen()
    print(f"[ESCUCHANDO] Servidor a la escucha en {SERVER}")
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count() - 1
        if(CONEX_ACTIVAS <= MAX_CONEXIONES):
            thread = threading.Thread(target=gestorTaxi, args=(conn, addr))
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

def hayTaxisDisponibles(cliente_x, cliente_y):                              # se pasa por parámetro la posición del cliente para escoger el taxi más cercano

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
            taxi_id = datos[0]
            taxi_x = datos[1]
            taxi_y = datos[2]
        
        distancia = math.sqrt((taxi_x - cliente_x) ** 2 + (taxi_y - cliente_y) ** 2)

        if distancia < minDistancia:
            minDistancia = distancia
            taxiMasCercano = (taxi_id, taxi_x, taxi_y)

    if taxiMasCercano:
        print("Enviando al taxi más cercano")
        return taxiMasCercano

    else:
        print("No se encuentran Taxis")     # @implementar
        return None

def colocarTaxi(mapa, id_taxi, x, y, estado):                               # Estado puede ser "miviendo" o "parado"

    index = y * ANCHO + x                               # Convertir coordenadas 2D en índice lineal

    if estado == "moviendo":
        mapa[index] = id_taxi                           # Usar bitwise OR para el estado "moviendo"
    else:
        mapa[index] = id_taxi                           # Usar bitwise OR para el estado "parado"

def colocarLocalizacion(mapa, letra, x, y):                                 # Guardamos el destino
    index = y * ANCHO + x
    mapa[index] = ord(letra)                            # Almacenar el valor ASCII de la letra

def colocarCliente(mapa, letra, x, y):
    index = y * ANCHO + x
    mapa[index] = ord(letra)

def mostrarMapa(mapa):
    for y in range(ALTO):
        fila = []
        for x in range(ANCHO): 
            index = y * ANCHO + x
            valor = mapa[index]

            if valor == NADA:
                fila.append('.')                    # Celda vacía
            elif 32 <= valor <= 126:                # De 32 a 126 es el rango para representar el ASSCI
                fila.append(chr(valor))             # Representa el byte como char
            elif (valor & 0x3F) != 0:               # Si el taxi está parado
                fila.append(f"{valor & 0x3F}")     # Solo muestra el ID del taxi
            else:
                fila.append('?')                    # Elemento desconocido

        print(' '.join(fila))


############### MAIN ###############

arrayPos = []

# Leer el archivo mapa.txt
with open(ARCHIVO_MAPA, 'r') as archivo:
    for linea in archivo:
        # Usar split() para manejar espacios en blanco
        datos = linea.strip().replace('<', '').replace('>', '').split()
        
        if len(datos) == 3:  # Asegúrate de que haya exactamente 3 elementos
            id_localizacion = datos[0]  # Puede ser letra o número
            posicion_x = int(datos[1])    # Convertir a entero
            posicion_y = int(datos[2])    # Convertir a entero

            arrayPos.append({
                'id': id_localizacion,
                'x': posicion_x,
                'y': posicion_y
            })
        else:
            print(f"Línea malformada: {linea}")


arrayTaxis = []

for item in arrayPos:                                           # Coge solo los taxis del tablero que nos habian pasado
    if item['id'].isdigit():                                    # Seleccionar los taxis del mapa :)
        arrayTaxis.append(item)
        

for item in arrayPos:
    if item['id'].isdigit():  # Verifica si es un ID de taxi
        colocarTaxi(mapa, int(item['id']), item['x'], item['y'], "moviendo")
    elif item['id'].isupper():  # Verifica si es una letra mayúscula (localización)
        colocarLocalizacion(mapa, item['id'], item['x'], item['y'])  # Colocar localización
    elif item['id'].islower():  # Verifica si es una letra minúscula (cliente)
        colocarCliente(mapa, item['id'], item['x'], item['y'])  # Colocar cliente


with open(ARCHIVO_TAXI, 'w') as archivo:                                    # Guardamos la informacion en el fichero taxis.txt
    for item in arrayTaxis:
        archivo.write(f"<{item['id']}><{item['x']}><{item['y']}>\n")


mostrarMapa(mapa)
print("[EMPEZANDO] Servidor inicializando...")

# Creamos el hilo para los Taxis y la ejecutamos
hiloTaxi = threading.Thread(target=conexionTaxi)
hiloTaxi.start()

hiloTaxi.join()
