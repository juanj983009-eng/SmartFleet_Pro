import React, { useRef, useMemo } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';

function NubeParticulas() {
  const pointsRef = useRef();
  const count = 1000;

  // Matriz balanceada de coordenadas tridimensionales
  const posiciones = useMemo(() => {
    const points = new Float32Array(count * 3);
    for (let i = 0; i < count * 3; i++) {
      points[i * 3]     = (Math.random() - 0.5) * 8;  // X
      points[i * 3 + 1] = (Math.random() - 0.5) * 8;  // Y
      points[i * 3 + 2] = (Math.random() - 0.5) * 5;  // Z (Profundidad)
    }
    return points;
  }, []);

  // Rotación suave continua para telemetría
  useFrame((state) => {
    const time = state.clock.getElapsedTime();
    if (pointsRef.current) {
      pointsRef.current.rotation.x = time * 0.02;
      pointsRef.current.rotation.y = time * 0.03;
    }
  });

  return (
    <points ref={pointsRef}>
      <bufferGeometry>
        <bufferAttribute
          attach="attributes-position"
          count={count}
          array={posiciones}
          itemSize={3}
        />
      </bufferGeometry>
      <pointsMaterial
        color="#a1a1aa"
        size={0.04}
        sizeAttenuation={true}
        transparent={true}
        opacity={0.35}
        depthWrite={false}
      />
    </points>
  );
}

export default function VectoresFondo() {
  return (
    <div 
      className="fixed inset-0 pointer-events-none w-screen h-screen bg-black"
      style={{ zIndex: -50 }}
    >
      <Canvas 
        camera={{ position: [0, 0, 4], fov: 60 }}
        style={{ width: '100vw', height: '100vh', position: 'absolute', top: 0, left: 0 }}
      >
        <NubeParticulas />
      </Canvas>
    </div>
  );
}
