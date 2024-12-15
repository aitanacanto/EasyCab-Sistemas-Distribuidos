import React, { useState, useEffect } from "react";

function RealTimeMap() {
  const [map, setMap] = useState([]); // Estado para el mapa

  useEffect(() => {
    // Función para obtener el mapa desde el servidor Flask
    async function fetchMap() {
      try {
        const response = await fetch("http://localhost:4321/api/mapa");
        const data = await response.json();
        setMap(data.mapa); // Actualiza el estado con el mapa recibido
      } catch (error) {
        console.error("Error fetching map:", error);
      }
    }

    fetchMap(); // Llamar por primera vez
    const interval = setInterval(fetchMap, 1000); // Llamar cada segundo

    // Limpia el intervalo cuando el componente se desmonta
    return () => clearInterval(interval);
  }, []);

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", minHeight: "100vh", backgroundColor: "#f3f3f3" }}>
      {/* Encabezado del mapa */}
      <div>
        <h3>Mapa en Tiempo Real</h3>
        <pre>{JSON.stringify(map, null, 2)}</pre> {/* Muestra el mapa en formato JSON */}
      </div>

      {/* Renderizado de la cuadrícula */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(20, 30px)", gridTemplateRows: "repeat(20, 30px)", border: "1px solid #000", marginTop: "20px" }}>
        {Array(20 * 20) // Crea una cuadrícula de 400 celdas
          .fill(0)
          .map((_, index) => {
            const row = Math.floor(index / 20) + 1; // Fila de la celda
            const col = (index % 20) + 1; // Columna de la celda
            const cellData = map.find(cell => cell.row === row && cell.col === col); // Busca datos de la celda
            const cellStyle = {
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              border: "2px solid #444",
              backgroundColor: cellData ? (cellData.class === "highlight" ? "yellow" : "limegreen") : "lightgray",
              color: cellData && cellData.class === "taxi" ? "white" : "black",
              fontWeight: cellData ? "bold" : "normal",
            };

            return (
              <div key={index} style={cellStyle}>
                {cellData ? cellData.text : ""}
              </div>
            );
          })}
      </div>
    </div>
  );
}

export default RealTimeMap;
