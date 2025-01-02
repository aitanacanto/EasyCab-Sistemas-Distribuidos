import React, { useEffect, useState } from "react";

const RealTimeMap = () => {
  const [mapa, setMapa] = useState(() => {
    // Inicializa una copia única de cada fila
    return Array.from({ length: 20 }, () => Array(20).fill(" "));
  });
  const ANCHO = 20;
  const ALTO = 20;
  const TAMANO_CELDA = 30;

  const COLOR_FONDO = "lightgray";
  const COLOR_TAXI_VERDE = "limegreen";
  const COLOR_TAXI_ROJO = "red";
  const COLOR_TAXI_AMARILLO = "yellow";
  const COLOR_DESTINO = "blue";
  const COLOR_CLIENTE = "orange";

  // Función para determinar el color de cada celda
  const determinarColor = (celda) => {
    if (celda === " ") return COLOR_FONDO;
    if (celda[0] >= "0" && celda[0] <= "9") {
      if (celda[1] === "r") return COLOR_TAXI_ROJO;
      if (celda[1] === "a") return COLOR_TAXI_AMARILLO;
      return COLOR_TAXI_VERDE;
    }
    if (celda >= "A" && celda <= "Z") return COLOR_DESTINO;
    if (celda >= "a" && celda <= "z") return COLOR_CLIENTE;
    return COLOR_FONDO;
  };

  // Función para obtener el mapa desde la API
  const fetchMapa = async () => {
    try {
      const response = await fetch("http://localhost:5002/mapa");
      if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
      const data = await response.json();
      if (data.mapa) {
        // Crea una nueva copia del mapa para asegurarse de que React detecta el cambio
        const nuevoMapa = data.mapa.map((fila) => [...fila]);
        setMapa(nuevoMapa);
        console.log("Mapa actualizado:", nuevoMapa);
      } else {
        console.error("Mapa vacío o formato inválido.");
      }
    } catch (error) {
      console.error("Error al obtener el mapa:", error);
    }
  };

  useEffect(() => {
    fetchMapa(); // Llamar al cargar la página
    const interval = setInterval(fetchMapa, 500); // Actualizar cada 0.5 segundos
    return () => clearInterval(interval); // Limpiar el intervalo al desmontar
  }, []);

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", minHeight: "100vh", backgroundColor: "#f3f3f3" }}>
      <h3>Mapa en Tiempo Real</h3>
      {console.log("Estado del mapa:", mapa)} {/* Log para verificar el estado */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: `repeat(${ANCHO}, ${TAMANO_CELDA}px)`,
          gridTemplateRows: `repeat(${ALTO}, ${TAMANO_CELDA}px)`,
          gap: "1px",
        }}
      >
        {mapa.map((fila, y) =>
          fila.map((celda, x) => (
            <div
              key={`${x}-${y}`}
              style={{
                width: `${TAMANO_CELDA}px`,
                height: `${TAMANO_CELDA}px`,
                backgroundColor: determinarColor(celda),
                display: "flex",
                justifyContent: "center",
                alignItems: "center",
                border: "1px solid #444",
              }}
            >
              {celda[0] >= "0" && celda[0] <= "9" ? celda[0] : ""}
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default RealTimeMap;
