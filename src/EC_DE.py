# Levan Kipiani, Aitana Cantó
import socket
import sys
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata
import json
import traceback
import requests
import tkinter as tk
from cryptography.fernet import Fernet
BASE_URL = "http://localhost:5001"


#CLAVE_TAXI = b'example'       # OJO --> Sustituir aquí por la clave buena
CLAVE_TAXI = b''

lock = threading.Lock()

# Argumentos y configuraciones
if len(sys.argv) != 8:
    print("ERROR: Número de argumentos incorrecto")
    print("Formato correcto -> python3 src/EC_DE.py <IP central> <Puerto central> <IP gestor> <Puerto gestor> <IP Sensor> <Puerto Sensor> <ID Taxi>")
    sys.exit(1)

argumentos = sys.argv
FIN = "<FIN>"
OK = "OK"
KO = "KO"

global token
token = ''

global clave
clave = b''

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
destino = None
clienteDentro = False
mapa = None
permisoMovimiento = False
mapa_lock = threading.Lock()

global estadoEmergencia
estadoEmergencia = False

# Consumidor de Kafka para actualizaciones del mapa (reutilizable)
consumer_actualizacion_mapa = KafkaConsumer(
    'actualizar_mapa',
    bootstrap_servers=[f"{IP_GESTOR}:{PUERTO_GESTOR}"],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=f'taxi_{ID_TAXI}'  # Asegurar un group_id único para cada instancia
)

####### ESTADO DE EMERGENCIA #######

def comprobarEstadoAlarma():
    global destino, letraUbicacion, estadoEmergencia
    global CLAVE_TAXI

    while True:
        time.sleep(1)
        
        if CLAVE_TAXI != b'':
            print("Comprobando Estado de Emergencia")
            group_id = f"taxi_group_{ID_TAXI}"
            consumer = KafkaConsumer(
                'procesar_viaje',
                bootstrap_servers=[f"{IP_GESTOR}:{PUERTO_GESTOR}"],
                value_deserializer=lambda v: v,  # Recibimos bytes
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id=group_id
            )

            try:
                messages = consumer.poll(timeout_ms=1000) 

                if not(not messages or stop_event.is_set()):
                    for topic_partition, msgs in messages.items():
                        for message in msgs:
                            # 1) Desciframos con nuestra CLAVE_TAXI
                            try:
                                info = decrypt_message(message.value, CLAVE_TAXI)
                            except Exception as e:
                                print(f"Error descifrando mensaje: {e}")
                                continue

                            print("Mensaje recibido con ID: " + info['taxi_id'])
                            key = message.key.decode('utf-8') if message.key else None
                            estadoEmergenciaa = info['estadoEmergencia']
                            if estadoEmergenciaa:
                                print("VOLVER A BASE!!!!!!!!")
                                destino = (1,1)
                                estadoEmergencia = True
                                if key == ID_TAXI or info['taxi_id'] == ID_TAXI:
                                    consumer.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})
                                
            except Exception as e:
                print(f"Error al recibir la orden del servicio: {e}")
                traceback.print_exc()
            finally:
                consumer.close()

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
        global permisoMovimiento, clienteDentro, ubicacion, destino, mapa, letraUbicacion, estadoEmergencia

        if permisoMovimiento:
            if estadoEmergencia:
                print("VOLVIENDO A BASE!!")
                
            mostrarMapa(mapa)
            antiguaPosicion = ubicacion

            ubicacion = actualizarPosicion(destino, mapa, letraUbicacion)
            
            print(f"Destino --> {str(destino[0])}, {str(destino[1])}")
            
            if ubicacion == (1,1) and estadoEmergencia:
                print("Taxi ha llegado a la base.")
                enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, True, OK)
                time.sleep(1)
                desconectar_taxi()
                break
            
            elif ubicacion[0] == destino[0] and ubicacion[1] == destino[1]:
                enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, True, OK)
                break
            else:
                enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, OK)

            while not recibirActualizacionMapa() and not stop_event.is_set():
                time.sleep(0.1)
        else:
            antiguaPosicion = ubicacion
            enviarPosicionTaxi(ID_TAXI, ubicacion, antiguaPosicion, False, KO)
            
        time.sleep(1)

def cerebroDigitalEngine():
    global clienteDentro

    mapaAct = False
    ordenRecibido = False

    while not stop_event.is_set():

        if not mapaAct and recibirActualizacionMapa():
            mapaAct = True

        if not ordenRecibido and recibirPosicionCliente():
            ordenRecibido = True

        if mapaAct and ordenRecibido:
            clienteDentro = False
            realizarViaje()
            mapaAct = False
            ordenRecibido = False
            
        time.sleep(0.1)

def recibirActualizacionMapa():
    global mapa
    global consumer_actualizacion_mapa
    with lock:
        try:
            # Utiliza poll para obtener mensajes disponibles
            messages = consumer_actualizacion_mapa.poll(timeout_ms=1000)
            
            if messages:
                for topic_partition, msgs in messages.items():
                    for message in msgs:
                        with mapa_lock:
                            mapa = message.value
                            consumer_actualizacion_mapa.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})
                            return True
        except Exception as e:
            print(f"Error al recibir la actualización del mapa: {e}")
            traceback.print_exc()
            
    return False

def recibirPosicionCliente():
    global destino, letraUbicacion, estadoEmergencia
    with lock:
        consumer = KafkaConsumer(
            'procesar_viaje',
            bootstrap_servers=[f"{IP_GESTOR}:{PUERTO_GESTOR}"],
            value_deserializer=lambda v: v,                     # Asegúrate de que los mensajes sean JSON
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=f"taxi_group_{ID_TAXI}"
        )

        try:
            messages = consumer.poll(timeout_ms=1000)
            if not messages or stop_event.is_set():
                return False

            for topic_partition, msgs in messages.items():
                for message in msgs:
                    try:
                        info = decrypt_message(message.value, CLAVE_TAXI)
                    except Exception as e:
                        print(f"Error descifrando mensaje: {e}")
                        continue
                    print(f"Mensaje recibido: {info}")

                    # Validar que el mensaje tenga una clave válida
                    if not message.key or message.key.decode('utf-8') != ID_TAXI:
                        print("Mensaje sin clave válida o no dirigido a este taxi. Ignorado.")
                        continue

                    # Procesar el mensaje
                    estadoEmergencia = info.get('estadoEmergencia', False)
                    letraUbicacion = info.get('letraDestino')
                    destino = (1, 1) if estadoEmergencia else info.get('posDestino')

                    print(f"Orden procesada. Destino: {destino}, Emergencia: {estadoEmergencia}")
                    consumer.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})
                    return True
        except Exception as e:
            print(f"Error al recibir la orden del servicio: {e}")
            traceback.print_exc()
        finally:
            consumer.close()

        return False

def enviarPosicionTaxi(taxi_id, posicion, antiguaPosicion, finalizado, estadoSensor):
    global token
    global CLAVE_TAXI
    global letraUbicacion

    producerTAXI = KafkaProducer(
        bootstrap_servers=[f"{IP_GESTOR}:{PUERTO_GESTOR}"],
        # Mandamos bytes, no JSON
        value_serializer=lambda v: v
    )
    topic = 'actualizaciones_taxi'
    
    # Diccionario en claro
    clear_message = {
        'taxi_id': taxi_id,
        'taxi_posicion': posicion,
        'viaje_finalizado': finalizado,
        'antigua_posicion': antiguaPosicion,
        'estado_sensor': estadoSensor,
        'token': token,
        'letra': letraUbicacion
    }

    # Ciframos con la clave
    encrypted_message = encrypt_message(clear_message, CLAVE_TAXI)

    producerTAXI.send(topic, key=taxi_id.encode(), value=encrypted_message)
    producerTAXI.flush()
    print("POSICION ENVIADA A CENTRAL (cifrada)")

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

    while not stop_event.is_set():
        try:
            mensaje = conexion.recv(4096).decode()
            
            print("Mensaje del sensor --> " + str(mensaje))
            
            if mensaje == OK:
                permisoMovimiento = True
                last_message_time = time.time()
            elif mensaje == KO:
                permisoMovimiento = False
                last_message_time = time.time()
            elif mensaje == FIN:
                conexion.sendall(FIN.encode())
                print("Recibido FIN, terminando conexión...")
                stop_event.set()
                break

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
    if posX > objetivo[0]:
        posX -= 1
    elif posX < objetivo[0]:
        posX += 1
    if posY > objetivo[1]:
        posY -= 1
    elif posY < objetivo[1]:
        posY += 1

    # Si ya estamos en la posición objetivo, mantener
    if (posX, posY) == objetivo:
        return (posX, posY)

    # Asegurar que se puede avanzar (sin bloqueos)
    if mapa[posX][posY] in [' ', letraUbicacion] or (posX, posY) == objetivo:
        return (posX, posY)
    else:
        print("Movimiento bloqueado, intentando alternativa.")
        return (posX, posY)  # Quedarse en la posición actual si no se puede avanzar

def actualizarPosicion(destino, mapa, letra):
    global ubicacion
    global clienteDentro
    global letraUbicacion

    posX = ubicacion[0]
    posY = ubicacion[1]

    return movimiento(posX, posY, destino, mapa, letra)

def registrar_taxi_en_registry(taxi_id, pos_x, pos_y):
    try:
        url = f"http://localhost:5001/register_taxi"
        payload = {
            "taxi_id": taxi_id,
            "pos_x": pos_x,
            "pos_y": pos_y
        }

        print(f"Enviando datos a EC_Registry: {payload}")
        response = requests.post(url, json=payload)

        print(f"Estado HTTP: {response.status_code}")
        print(f"Respuesta del servidor: {response.text}")

        if response.status_code == 201:
            print(f"Taxi {taxi_id} registrado con éxito en EC_Registry.")
        elif response.status_code == 200:
            print(f"Taxi {taxi_id} actualizado correctamente en EC_Registry.")
        else:
            print(f"Error al registrar el taxi {taxi_id}. Respuesta: {response.json()}")

    except requests.exceptions.RequestException as e:
        print(f"Error al comunicarse con EC_Registry: {e}")
        sys.exit(1)

def validarID(socketCentral):
    global ubicacion, token, CLAVE_TAXI
    socketCentral.send(ID_TAXI.encode())
    respuesta = socketCentral.recv(4096).decode()

    if respuesta == FIN:
        print("ERROR: ID del taxi ya está en uso")
        socketCentral.close()
        sys.exit(1)
    else:
        datos = respuesta.strip().replace('<', '').split('>')
        ubicacion = (int(datos[1]), int(datos[2]))
        token = datos[3]
        try:
            CLAVE_TAXI = bytes(datos[4], 'utf-8')  # Configura la clave correctamente
            print(f"ID asignado con éxito. Token recibido: {token}")
        except Exception as e:
            print(f"Error al asignar clave: {e}")
            socketCentral.close()
            sys.exit(1)

def conectarConServidor():
    global socketCentral
    socketCentral = socket.socket()
    socketCentral.connect((IP_CENTRAL, PUERTO_CENTRAL))
    validarID(socketCentral)
    socketCentral.close()

####### CONEXIÓN/DESCONEXIÓN #######

def conectar_taxi():
    try:
        conectarConServidor()
        url = f"{BASE_URL}/register_taxi"
        payload = {
            "taxi_id": ID_TAXI,
            "pos_x": 1,
            "pos_y": 1
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            resultado_label.config(text=f"Taxi {ID_TAXI} conectado correctamente.")
            print(f"Taxi {ID_TAXI} conectado correctamente.")
        else:
            resultado_label.config(text=f"Error al conectar: {response.text}")
            print(f"Taxi {ID_TAXI} no se ha llegado a desconectar ERROR!!")
    except requests.exceptions.RequestException as e:
        resultado_label.config(text=f"Error al conectar: {e}")
        print(f"Error al conectar: {e}")

def desconectar_taxi():
    print("DESCONECTANDO TAXI...")
    try:
        url = f"{BASE_URL}/unregister_taxi"
        payload = {
            "taxi_id": ID_TAXI,
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            resultado_label.config(text=f"Taxi {ID_TAXI} desconectado correctamente.")
        else:
            resultado_label.config(text=f"Error al desconectar: {response.text}")
    except requests.exceptions.RequestException as e:
        resultado_label.config(text=f"Error al desconectar: {e}")

###### CIFRADO / DESCIFRADO ########

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

############## MAIN ################

hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

hiloKafka = threading.Thread(target=cerebroDigitalEngine)
hiloKafka.start()

hiloEstadoAlarma = threading.Thread(target=comprobarEstadoAlarma)
hiloEstadoAlarma.start()

ventana = tk.Tk()
ventana.title("Control de DigitalEngine")
ventana.geometry("400x200")

# Etiqueta para mostrar resultados
resultado_label = tk.Label(ventana, text="Estado: Esperando acción", wraplength=300)
resultado_label.pack(pady=10)

# Botones para conectar
boton_conectar = tk.Button(ventana, text="Conectar Taxi", command=conectar_taxi, bg="green", fg="white")
boton_conectar.pack(pady=5)

# Iniciar el loop principal de Tkinter
ventana.mainloop()

if stop_event.is_set():
    consumer_actualizacion_mapa.close()
    sys.exit("Programa terminado correctamente.")