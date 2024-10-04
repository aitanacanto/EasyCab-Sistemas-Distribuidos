import socket
import sys
import threading

# Entradas: Puerto de escucha
#           Ip y puerto del gestor de colas
#           Ip y puerto de la base de datos [Si utilizamos BBDD (si utilizamos archivos de texto no hace falta)]

HEADER = 64
FORMAT = 'utf-8'

if len(sys.argv) != 4:
    print("ERROR: Número de argumentos arróneos")    
    exit()

argumentos = sys.argv
PUERTO_ESCUCHA = argumentos[1]
IP_GESTOR = argumentos[2]
PUERTO_GESTOR = argumentos[3]
ANCHO = 20
ALTO = 20
MAX_CONEXIONES = 22
SERVER = socket.gethostbyname(socket.gethostname())
FIN = "<FIN>"

############ FUNCIONES #############

def clienteNuevo(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected")
    conectado = True
    while conectado:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            print(f" He recibido del cliente [{addr}] el mensaje: {msg}")
            conn.send(f"HOLA CLIENTE: He recibido tu mensaje: {msg} ".encode(FORMAT))
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()

def start():
    server.listen()
    print(f"[ESCUCHANDO] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
        if(CONEX_ACTIVAS <= MAX_CONEXIONES):
            thread = threading.Thread(target=clienteNuevo, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTIVAS = threading.active_count()-1

############### MAIN ###############

arrayPos = []

with open('mapa.txt', 'r') as archivo:                                      # Lee la informacion sobre las posiciones del mapa
    for linea in archivo:
        datos = linea.strip().replace('<', '').split('>')
        
        if len(datos) >= 3:
            id_localizacion = datos[0]
            posicion_x = datos[1]
            posicion_y = datos[2]

        arrayPos.append({
            'id' : id_localizacion, 
            'x'  : posicion_x,
            'y'  : posicion_y
        })

nombreArchivo = 'taxis.txt'
arrayTaxis = []

for item in arrayPos:                                                       # Coge solo los taxis del tablero que nos habian pasado
    if item['id'].isdigit():                                                # Seleccionar los taxis del mapa : )
        arrayTaxis.append(item)


with open(nombreArchivo, 'w') as archivo:                                   # Guardamos la informacion en el fichero taxis.txt
    for item in arrayTaxis:
        archivo.write(f"<{item['id']}><{item['x']}><{item['y']}>\n")

DIRECCION = (SERVER, PUERTO_ESCUCHA)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(DIRECCION)

print("[EMPEZANDO] Servidor inicializando...")

start()



