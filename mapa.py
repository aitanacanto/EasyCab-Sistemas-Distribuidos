import pygame

# Configuraciones básicas
ANCHO = 20
ALTO = 20
TAMAÑO_CELDA = 40
COLORES = {
    "A": (0, 0, 255),   # Color para taxis
    "B": (0, 255, 0),   # Otro taxi
    "C": (255, 0, 0),   # Cliente
    ".": (200, 200, 200),  # Espacio vacío
    "grass": (0, 128, 0),   # Césped
    "water": (0, 0, 128),   # Agua
    "rock": (128, 128, 128) # Roca
}

class Mapa:
    def __init__(self, ancho=ANCHO, alto=ALTO, tamaño_celda=TAMAÑO_CELDA):
        self.ancho = ancho
        self.alto = alto
        self.tamaño_celda = tamaño_celda
        self.mapa = [['.' for _ in range(self.ancho)] for _ in range(self.alto)]
        self.pantalla = None

    def inicializar_pantalla(self):
        pygame.init()
        self.pantalla = pygame.display.set_mode((self.ancho * self.tamaño_celda, self.alto * self.tamaño_celda))
        pygame.display.set_caption("Simulación de Taxis y Clientes")

    def dibujar_mapa(self):
        for y in range(self.alto):
            for x in range(self.ancho):
                celda = self.mapa[x][y]
                color = COLORES.get(celda, (255, 255, 255))  # Color blanco por defecto
                pygame.draw.rect(
                    self.pantalla, color,
                    (x * self.tamaño_celda, y * self.tamaño_celda, self.tamaño_celda, self.tamaño_celda)
                )

    def actualizar_mapa(self, mapa_nuevo):
        self.mapa = mapa_nuevo
        self.dibujar_mapa()
        pygame.display.update()

    def cerrar(self):
        pygame.quit()