import { useState, useEffect } from 'react';

function RealTimeMap() {
  const [map, setMap] = useState([]);

  useEffect(() => {
    async function fetchMap() {
      try {
        const response = await fetch('http://localhost:4321');
        const data = await response.json();
        setMap(data.mapa);
      } catch (error) {
        console.error('Error fetching map:', error);
      }
    }

    fetchMap();
    const interval = setInterval(fetchMap, 2000); // Actualiza cada 2 segundos
    return () => clearInterval(interval);
  }, []);

  return (
    <div>
      <h3>Mapa en Tiempo Real</h3>
      <pre>{JSON.stringify(map, null, 2)}</pre>
    </div>
  );
}

export default RealTimeMap;
