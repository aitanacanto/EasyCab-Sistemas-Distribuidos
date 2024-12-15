import { useState, useEffect } from 'react';

function RealTimeMap() {
  const [map, setMap] = useState([]);

  // Definir colores como constantes
  const COLOR_FONDO = '#f3f3f3';
  const COLOR_ROJO = 'red';
  const COLOR_AMARILLO = 'yellow';
  const COLOR_VERDE = 'limegreen';
  const COLOR_DESTINO = 'blue';
  const COLOR_CLIENTE = 'purple';
  const COLOR_BORDE = '#444';
  const COLOR_TEXTO = 'black';
  const ANCHO_BORDE = 2;
  const TAMANO_CELDA = 30;

  // Función para actualizar el mapa
  const actualizarMapa = async (nuevoMapa) => {
    try {
      const response = await fetch('http://localhost:4321/api/actualizar_mapa', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ mapa: nuevoMapa }),
      });

      if (response.ok) {
        console.log('Mapa actualizado correctamente');
      } else {
        console.error('Error al actualizar el mapa');
      }
    } catch (error) {
      console.error('Error de conexión:', error);
    }
  };

  useEffect(() => {
    // Función que ejecuta el mapa actualizado cada 2 segundos
    const interval = setInterval(() => {
      // Aquí puedes modificar el mapa como desees
      const nuevoMapa = [...map]; // Modifica según la lógica de tu aplicación

      // Llama a la función para enviar el mapa actualizado
      actualizarMapa(nuevoMapa);
    }, 2000); // Actualiza cada 2 segundos

    return () => clearInterval(interval); // Limpia el intervalo cuando el componente se desmonte
  }, [map]); // Dependencia para actualizar el mapa cuando se modifique

  const renderCell = (cellData, rowIndex, colIndex) => {
    let color = COLOR_FONDO;
    let texto = '';

    // Lógica de colores y texto según el contenido de cada celda
    if (cellData === ' ') {
      color = COLOR_FONDO;
      texto = '';
    } else if (cellData[0].match(/[0-9]/)) {  // Taxi
      if (cellData[1] === 'r') {
        color = COLOR_ROJO;
      } else if (cellData[1] === 'a') {
        color = COLOR_AMARILLO;
      } else {
        color = COLOR_VERDE;
      }
      texto = cellData[0];
    } else if (cellData.match(/[A-Z]/)) {  // Destino
      color = COLOR_DESTINO;
      texto = cellData;
    } else if (cellData.match(/[a-z]/)) {  // Cliente
      color = COLOR_CLIENTE;
      texto = cellData;
    }

    return (
      <div
        key={`${rowIndex}-${colIndex}`}
        className="cell"
        style={{
          backgroundColor: color,
          color: color === COLOR_FONDO ? 'transparent' : COLOR_TEXTO,
          border: `${ANCHO_BORDE}px solid ${COLOR_BORDE}`,
          width: TAMANO_CELDA,
          height: TAMANO_CELDA,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
        }}
      >
        {texto}
      </div>
    );
  };

  return (
    <div>
      <h3>Mapa en Tiempo Real</h3>
      <div
        className="map-container"
        style={{
          display: 'grid',
          gridTemplateColumns: `repeat(20, ${TAMANO_CELDA}px)`, // 20 columnas de 30px
          gridTemplateRows: `repeat(20, ${TAMANO_CELDA}px)`, // 20 filas de 30px
          border: `1px solid ${COLOR_BORDE}`,
        }}
      >
        {map.length > 0 &&
          map.map((row, rowIndex) =>
            row.map((cellData, colIndex) => renderCell(cellData, rowIndex, colIndex))
          )}
      </div>
      <style>
        {`
          .cell {
            border: ${ANCHO_BORDE}px solid ${COLOR_BORDE};
            display: flex;
            justify-content: center;
            align-items: center;
            text-align: center;
            font-weight: bold;
          }

          .map-container {
            display: grid;
            grid-template-columns: repeat(20, 30px);
            grid-template-rows: repeat(20, 30px);
            gap: 2px;
          }
        `}
      </style>
    </div>
  );
}

export default RealTimeMap;
