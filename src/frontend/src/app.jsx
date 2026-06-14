import React, { useState, useEffect } from 'react'
import {
  Activity,
  Shield,
  Radio,
  Gauge as GaugeIcon,
  MapPin
} from 'lucide-react'
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip
} from 'recharts'
import { MapContainer, TileLayer, Marker, Polyline } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import VectoresFondo from './components/VectoresFondo'

export default function App() {
  const [analyticsData, setAnalyticsData] = useState([])
  const [apiStatus, setApiStatus] = useState('OFFLINE')

  useEffect(() => {
    const baseApiUrl = "http://localhost:8000/api/v1/fleet/analytics"
    const streamUrl = `${baseApiUrl}/stream`
    let eventSource = null

    async function hydrateInitialState() {
      try {
        const response = await fetch(baseApiUrl)
        if (response.ok) {
          const data = await response.json()
          
          // TRAZA DE CONTROL CRÍTICA
          console.log("[ANTIGRAVITY DEBUG] Payload recibido de la API:", data)
          console.log("[ANTIGRAVITY DEBUG] Tipo de dato:", typeof data, "Es Array:", Array.isArray(data))

          if (data) {
            const formattedData = Array.isArray(data) ? data : [data]
            console.log("[ANTIGRAVITY DEBUG] Dataset formateado para React:", formattedData)
            if (formattedData.length > 0) {
              setAnalyticsData(formattedData)
            }
          }
        } else {
          console.error("[ANTIGRAVITY] Respuesta de API no exitosa:", response.status)
        }
      } catch (err) {
        console.error("[ANTIGRAVITY] Error crítico en fetch de hidratación:", err)
      }
    }

    // 2. Conexión del canal en tiempo real para mutaciones subsecuentes
    function connectStream() {
      eventSource = new EventSource(streamUrl)

      eventSource.onopen = () => {
        setApiStatus('CONNECT_OK')
      }

      eventSource.addEventListener('pipeline_update', (event) => {
        try {
          const payload = JSON.parse(event.data)
          setAnalyticsData((prev) => {
            if (prev.length > 0 && payload.fecha_analisis <= prev[0].fecha_analisis) return prev
            return [payload, ...prev].slice(0, 50)
          })
        } catch (err) {
          console.error("Error parseando stream asíncrono:", err)
        }
      })

      eventSource.onerror = () => {
        setApiStatus('OFFLINE')
        eventSource.close()
        setTimeout(connectStream, 3000) // Reconexión defensiva con backoff
      }
    }

    hydrateInitialState().then(() => {
      connectStream()
    })

    return () => {
      if (eventSource) eventSource.close()
    }
  }, [])

  const displayDoc = analyticsData[0] || null

  // Helper para extraer la velocidad actual de forma defensiva
  const points = displayDoc?.puntos_telemetria || []
  const latestPoint = points.length > 0 ? points[points.length - 1] : null
  const currentSpeed = latestPoint?.velocidad ?? 0.0
  const currentAcceleration = latestPoint?.aceleracion ?? 0.0

  const lat = displayDoc?.metricas_basicas?.posicion_actual?.latitud ?? -12.046374
  const lng = displayDoc?.metricas_basicas?.posicion_actual?.longitud ?? -77.042793
  const historialRuta = displayDoc?.metricas_basicas?.historial_coordenadas ?? []

  // Angulo de rotacion para la aguja del velocimetro (de -90 a 90 grados)
  const speedLimit = 120.0
  const angle = -90 + (Math.min(currentSpeed, speedLimit) / speedLimit) * 180

  // Determinacion del estado de riesgo
  const riskScore = displayDoc?.ia_predictiva?.score_riesgo_global ?? 0.0
  let riskStatus = "BAJO"
  let riskColor = "text-emerald-400 border-emerald-500/20 bg-emerald-500/10"
  let riskBadge = "bg-emerald-500"
  if (riskScore > 30.0 && riskScore <= 70.0) {
    riskStatus = "MODERADO"
    riskColor = "text-amber-400 border-amber-500/20 bg-amber-500/10"
    riskBadge = "bg-amber-500"
  } else if (riskScore > 70.0) {
    riskStatus = "ALTO"
    riskColor = "text-rose-400 border-rose-500/20 bg-rose-500/10"
    riskBadge = "bg-rose-500"
  }

  // Formato para el eje X de Recharts (Bypass defensivo de propiedades indefinidas)
  const chartData = points.map((p) => {
    const timeVal = p?.tiempo ?? ""
    const timeParts = typeof timeVal === 'string' ? timeVal.split(' ') : []
    const timeOnly = timeParts.length > 1 ? timeParts[1].split('.')[0] : (timeVal || "N/A")
    const vel = typeof p?.velocidad === 'number' ? p.velocidad : 0.0
    const acel = typeof p?.aceleracion === 'number' ? p.aceleracion : 0.0
    return {
      name: timeOnly,
      velocidad: parseFloat(vel.toFixed(2)),
      aceleracion: parseFloat(acel.toFixed(2))
    }
  })


  return (
    <div className="bg-transparent text-zinc-100 min-h-screen relative p-6 font-sans" style={{ backgroundImage: 'linear-gradient(to right, rgba(255, 255, 255, 0.015) 1px, transparent 1px), linear-gradient(to bottom, rgba(255, 255, 255, 0.015) 1px, transparent 1px)', backgroundSize: '20px 20px' }}>
      <VectoresFondo />
      {/* Header */}
      <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 mb-6 bg-zinc-950 border-b border-zinc-800 p-4 rounded-none">
        <div>
          <h1 className="text-xl font-extrabold tracking-tight text-white flex items-center gap-2">
            <Radio className={`w-5 h-5 ${apiStatus === 'CONNECT_OK' ? 'text-emerald-400 animate-pulse' : 'text-rose-500 animate-pulse'}`} />
            SmartFleet Pro
            <span className="text-[10px] font-mono px-2 py-0.5 bg-zinc-950 border border-zinc-800 text-zinc-400 rounded-none">
              MISION_CONTROL_SYS_V2
            </span>
          </h1>
          <p className="text-xs text-zinc-400 mt-1 font-mono">
            Canal de telemetria en tiempo real por Server-Sent Events (SSE)
          </p>
        </div>
        <div className="flex items-center gap-3 font-mono text-xs">
          <div className={`px-3 py-1.5 rounded-none border flex items-center gap-2 ${
            apiStatus === 'CONNECT_OK' ? 'text-emerald-400 border-emerald-500/20 bg-emerald-500/5' : 'text-rose-400 border-rose-500/20 bg-rose-500/5'
          }`}>
            <span className={`w-2 h-2 rounded-full ${apiStatus === 'CONNECT_OK' ? 'bg-emerald-400 animate-ping' : 'bg-rose-500'}`} />
            API_STATUS: {apiStatus}
          </div>
          <div className="bg-black border border-zinc-800 px-3 py-1.5 rounded-none text-zinc-400">
            BUFFER: {analyticsData.length} / 100
          </div>
        </div>
      </header>

      {!displayDoc ? (
        <div className="flex flex-col items-center justify-center h-[70vh] border border-dashed border-zinc-800 rounded-none bg-black/20 backdrop-blur-sm">
          <Activity className="w-10 h-10 text-emerald-500 animate-spin mb-4" />
          <h2 className="text-md font-semibold tracking-wider text-zinc-300 font-mono">
            ESPERANDO CAPTURA DE EVENTOS...
          </h2>
          <p className="text-xs text-zinc-500 font-mono mt-2 max-w-md text-center">
            Escuchando cambios activos en mongodb_fleet:analytics_reports a traves del API REST de FastAPI.
          </p>
        </div>
      ) : (
        <div className="space-y-6">
          
          {/* Main Grid */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            
            {/* Columna Izquierda: KPIs Fisicos / Seguridad */}
            <div className="space-y-6 lg:col-span-1">
              
              {/* Card 1: Velocimetro Analogico */}
              <div className="rounded-none border border-zinc-800 bg-zinc-900/40 p-4 flex flex-col items-center justify-center relative overflow-hidden transition-all duration-300 ease-in-out hover:border-emerald-500/80 hover:shadow-[0_0_15px_rgba(16,185,129,0.15)]">
                <div className="absolute top-3 left-4 flex items-center gap-1.5 text-xs text-slate-400 font-mono">
                  <GaugeIcon className="w-3.5 h-3.5 text-emerald-400" />
                  MONITOR_VELOCIDAD_REALTIME
                </div>
                
                {/* SVG Gauge */}
                <div className="w-48 h-40 mt-4 relative flex items-center justify-center">
                  <svg width="200" height="200" className="transform -translate-y-8">
                    {/* Arco de fondo */}
                    <path
                      d="M 30,150 A 80,80 0 0,1 170,150"
                      fill="none"
                      stroke="#27272a"
                      strokeWidth={6}
                      strokeLinecap="round"
                    />
                    {/* Arco de zonas */}
                    {/* Zona Segura: Verde */}
                    <path
                      d="M 30,150 A 80,80 0 0,1 100,70"
                      fill="none"
                      stroke="#059669"
                      strokeWidth={6}
                      strokeOpacity="0.4"
                    />
                    {/* Zona Alerta: Amarillo */}
                    <path
                      d="M 100,70 A 80,80 0 0,1 140,88"
                      fill="none"
                      stroke="#d97706"
                      strokeWidth={6}
                      strokeOpacity="0.4"
                    />
                    {/* Zona Critica: Rojo */}
                    <path
                      d="M 140,88 A 80,80 0 0,1 170,150"
                      fill="none"
                      stroke="#dc2626"
                      strokeWidth={6}
                      strokeOpacity="0.4"
                    />
                    
                    {/* Aguja indicadora */}
                    <line
                      x1="100" y1="100"
                      x2={100 + 70 * Math.cos((210 - (displayDoc.metricas_basicas?.velocidad_promedio_kmh ?? 0) * 1.5) * Math.PI / 180)}
                      y2={100 - 70 * Math.sin((210 - (displayDoc.metricas_basicas?.velocidad_promedio_kmh ?? 0) * 1.5) * Math.PI / 180)}
                      stroke="#ef4444"
                      strokeWidth="1.5"
                      style={{ transition: 'x2 0.3s ease-out, y2 0.3s ease-out', filter: 'drop-shadow(0 0 3px rgba(239,68,68,0.6))' }}
                    />
                  </svg>
                  
                  {/* Digital Display */}
                  <div className="absolute bottom-2 text-center">
                    <span className="font-mono font-bold tracking-tighter text-zinc-100 text-3xl">
                      {currentSpeed.toFixed(1)}
                    </span>
                    <span className="text-[10px] text-slate-400 font-mono block">KM/H</span>
                  </div>
                </div>

                {/* Sub KPI Row */}
                <div className="w-full grid grid-cols-2 gap-2 border-t border-zinc-800/60 pt-4 mt-2 text-center text-xs font-mono">
                  <div>
                    <span className="text-zinc-400 block text-[10px]">ACEL_INSTANT</span>
                    <span className={`font-bold ${currentAcceleration >= 2.0 ? 'text-amber-400' : currentAcceleration < -4.5 ? 'text-rose-400' : 'text-emerald-400'}`}>
                      {currentAcceleration.toFixed(2)} m/s²
                    </span>
                  </div>
                  <div>
                    <span className="text-zinc-400 block text-[10px]">MAX_VEL_TRACK</span>
                    <span className="font-bold text-white">
                      {(displayDoc?.metricas_basicas?.velocidad_maxima_kmh ?? 0.0).toFixed(1)} km/h
                    </span>
                  </div>
                </div>
              </div>

              {/* Card 2: Score de Riesgo Global */}
              <div className="rounded-none border border-zinc-800 bg-zinc-900/40 p-4 relative overflow-hidden transition-all duration-300 ease-in-out hover:border-amber-500/80 hover:shadow-[0_0_15px_rgba(245,158,11,0.15)]">
                <div className="absolute top-3 left-4 flex items-center gap-1.5 text-xs text-zinc-400 font-mono">
                  <Shield className="w-3.5 h-3.5 text-emerald-400" />
                  SCORE_IA_PREDICTIVA
                </div>

                <div className="flex items-center gap-6 mt-4">
                  {/* Anillo de riesgo */}
                  <div className="relative w-24 h-24 flex items-center justify-center">
                    <svg className="w-full h-full transform -rotate-90">
                      <circle
                        cx="48"
                        cy="48"
                        r="40"
                        fill="none"
                        stroke="#27272a"
                        strokeWidth="8"
                      />
                      <circle
                        cx="48"
                        cy="48"
                        r="40"
                        fill="none"
                        stroke={riskScore > 70.0 ? '#f43f5e' : riskScore > 30.0 ? '#fbbf24' : '#34d399'}
                        strokeWidth="8"
                        strokeDasharray={2 * Math.PI * 40}
                        strokeDashoffset={2 * Math.PI * 40 - (riskScore / 100.0) * (2 * Math.PI * 40)}
                        strokeLinecap="round"
                        style={{
                          transition: 'stroke-dashoffset 0.8s ease-out'
                        }}
                      />
                    </svg>
                    <div className="absolute text-center flex flex-col items-center">
                      <span className="text-2xl font-extrabold text-white font-mono drop-shadow-[0_0_6px_rgba(255,255,255,0.25)]" style={{ transition: 'color 0.5s ease-out' }}>{riskScore.toFixed(0)}</span>
                      <span className="text-[9px] text-zinc-400 font-mono font-semibold">SCORE</span>
                    </div>
                  </div>

                  {/* Detalles del riesgo */}
                  <div className="flex-1 space-y-2">
                    <div className="rounded-none border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-mono tracking-wider text-amber-400 uppercase">
                      RIESGO: {riskStatus}
                    </div>
                    <p className="text-[10px] text-zinc-400 font-mono">
                      Viaje ID: {displayDoc.id_viaje}
                    </p>
                    <p className="text-[10px] text-zinc-400 font-mono">
                      Varianza: {displayDoc.ia_predictiva.aceleracion_varianza_kmhs2.toFixed(1)} (km/h/s)²
                    </p>
                  </div>
                </div>

                {/* Desglose de Ponderaciones */}
                <div className="mt-4 pt-4 border-t border-zinc-800/60 space-y-3 text-xs font-mono">
                  <div className="flex justify-between items-center text-[10px] text-zinc-400 mb-1">
                    <span>MATRIZ_PONDERACION</span>
                    <span>PESO %</span>
                  </div>
                  <div className="space-y-3">
                    {[
                      { label: 'Varianza Aceleracion', field: 'varianza_acel' },
                      { label: 'Excesos de Velocidad', field: 'exceso_velocidad' },
                      { label: 'Frenadas Bruscas', field: 'frenadas_bruscas' }
                    ].map(({ label, field }) => {
                      const weight = displayDoc.ia_predictiva?.ponderaciones_matriz?.[field] ?? 0;
                      return (
                        <div key={field}>
                          <div className="flex justify-between text-[10px] mb-1 text-slate-300">
                            <span>{label}</span>
                            <span>{(weight * 100).toFixed(0)}%</span>
                          </div>
                          <div className="flex gap-0.5 h-1.5 w-full bg-zinc-950 border border-zinc-900 p-0.5">
                            {[...Array(10)].map((_, idx) => {
                              const isFilled = idx < (weight * 10);
                              return (
                                <div 
                                  key={idx} 
                                  className={`h-full flex-1 rounded-none ${
                                    isFilled 
                                      ? (label.includes('Frenadas') ? 'bg-red-500/80' : label.includes('Excesos') ? 'bg-amber-500/80' : 'bg-emerald-500/80') 
                                      : 'bg-zinc-800/30'
                                  }`}
                                />
                              );
                            })}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>

            </div>

            {/* Columna Derecha: Graficos de Analitica */}
            <div className="lg:col-span-2 space-y-6">
              
              {/* Tarjetas Rapidas de Estadisticas */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="rounded-none border border-zinc-800/60 bg-zinc-900/40 backdrop-blur-sm p-4 transition-all duration-300 ease-in-out hover:border-cyan-500/80 hover:shadow-[0_0_15px_rgba(6,182,212,0.15)]">
                  <span className="text-[10px] font-semibold tracking-widest text-zinc-500 uppercase block">TOTAL_MUESTRAS</span>
                  <span className="text-3xl font-bold font-mono tracking-tighter text-zinc-100 block mt-1 drop-shadow-[0_0_4px_rgba(255,255,255,0.15)] transition-all duration-500">
                    {displayDoc?.metricas_basicas?.total_muestras ?? 0}
                  </span>
                </div>
                <div className="rounded-none border border-zinc-800/60 bg-zinc-900/40 backdrop-blur-sm p-4 transition-all duration-300 ease-in-out hover:border-cyan-500/80 hover:shadow-[0_0_15px_rgba(6,182,212,0.15)]">
                  <span className="text-[10px] font-semibold tracking-widest text-zinc-500 uppercase block">VEL_PROMEDIO</span>
                  <span className="text-3xl font-bold font-mono tracking-tighter text-zinc-100 block mt-1 drop-shadow-[0_0_4px_rgba(255,255,255,0.15)] transition-all duration-500">
                    {(displayDoc?.metricas_basicas?.velocidad_promedio_kmh ?? 0.0).toFixed(1)}
                    <span className="text-xs font-normal text-zinc-500 ml-1">km/h</span>
                  </span>
                </div>
                <div className="rounded-none border border-zinc-800/60 bg-zinc-900/40 backdrop-blur-sm p-4 transition-all duration-300 ease-in-out hover:border-cyan-500/80 hover:shadow-[0_0_15px_rgba(6,182,212,0.15)]">
                  <span className="text-[10px] font-semibold tracking-widest text-zinc-500 uppercase block">EXCESOS_VEL</span>
                  <span className={`text-3xl font-bold font-mono tracking-tighter block mt-1 drop-shadow-[0_0_4px_rgba(255,255,255,0.15)] transition-all duration-500 ${(displayDoc?.metricas_basicas?.alertas_exceso_velocidad ?? 0) > 0 ? 'text-rose-400' : 'text-zinc-100'}`}>
                    {displayDoc?.metricas_basicas?.alertas_exceso_velocidad ?? 0}
                  </span>
                </div>
                <div className="rounded-none border border-zinc-800/60 bg-zinc-900/40 backdrop-blur-sm p-4 transition-all duration-300 ease-in-out hover:border-cyan-500/80 hover:shadow-[0_0_15px_rgba(6,182,212,0.15)]">
                  <span className="text-[10px] font-semibold tracking-widest text-zinc-500 uppercase block">FRENADAS_BRUSCAS</span>
                  <span className={`text-3xl font-bold font-mono tracking-tighter block mt-1 drop-shadow-[0_0_4px_rgba(255,255,255,0.15)] transition-all duration-500 ${(displayDoc?.ia_predictiva?.frenadas_bruscas_count ?? 0) > 0 ? 'text-amber-400' : 'text-zinc-100'}`}>
                    {displayDoc?.ia_predictiva?.frenadas_bruscas_count ?? 0}
                  </span>
                </div>
              </div>

              {/* Grafico 1: Serie Temporal de Velocidad y Aceleracion */}
              <div className="bg-transparent backdrop-blur-none rounded-none border border-zinc-800/60 p-4 relative transition-all duration-300 ease-in-out hover:border-emerald-500/80 hover:shadow-[0_0_15px_rgba(16,185,129,0.15)]">
                <div className="flex justify-between items-center mb-4 font-mono text-xs text-zinc-400">
                  <div className="flex items-center gap-1.5">
                    <Activity className="w-3.5 h-3.5 text-emerald-400" />
                    SERIE_TEMPORAL_VELOCIDAD_ACELERACION
                  </div>
                  <span>ID_VIAJE: {displayDoc.id_viaje}</span>
                </div>
  
                <div className="h-64 bg-transparent">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartData} margin={{ top: 5, right: 10, left: -20, bottom: 0 }} className="bg-transparent">
                      <defs>
                        <linearGradient id="colorVel" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#10b981" stopOpacity={0.03}/>
                          <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                        </linearGradient>
                        <linearGradient id="colorAcel" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#f43f5e" stopOpacity={0.03}/>
                          <stop offset="95%" stopColor="#f43f5e" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <CartesianGrid stroke="#27272a" strokeDasharray="1 6" vertical={false} />
                      <XAxis dataKey="name" stroke="#71717a" fontSize={9} tickLine={false} />
                      <YAxis yAxisId="left" stroke="#10b981" fontSize={9} tickLine={false} label={{ value: 'km/h', angle: -90, position: 'insideLeft', offset: 10, fill: '#71717a' }} />
                      <YAxis yAxisId="right" orientation="right" stroke="#f43f5e" fontSize={9} tickLine={false} label={{ value: 'm/s²', angle: 90, position: 'insideRight', offset: 10, fill: '#71717a' }} />
                      <Tooltip
                        contentStyle={{ backgroundColor: '#09090b', borderColor: '#27272a', borderRadius: '0px' }}
                        labelStyle={{ color: '#a1a1aa', fontFamily: 'monospace', fontSize: '10px' }}
                        itemStyle={{ color: '#fff', fontFamily: 'monospace', fontSize: '12px' }}
                      />
                      <Area yAxisId="left" type="monotone" dataKey="velocidad" stroke="#10b981" strokeWidth={1.5} dot={false} activeDot={{ r: 4, strokeWidth: 0 }} fillOpacity={1} fill="url(#colorVel)" name="Velocidad (km/h)" />
                      <Area yAxisId="right" type="monotone" dataKey="aceleracion" stroke="#f43f5e" strokeWidth={1.5} dot={false} activeDot={{ r: 4, strokeWidth: 0 }} fillOpacity={1} fill="url(#colorAcel)" name="Aceleracion (m/s²)" />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>
                          {/* Grafico 2: Componentes de Riesgo y Pesos */}
              <div className="bg-transparent backdrop-blur-none rounded-none border border-zinc-800/60 p-4 transition-all duration-300 ease-in-out hover:border-cyan-500/80 hover:shadow-[0_0_15px_rgba(6,182,212,0.15)]">
                <div className="flex items-center gap-1.5 mb-4 font-mono text-xs text-zinc-400">
                  <MapPin className="w-3.5 h-3.5 text-emerald-400" />
                  LOGISTICA_TRACKING_GEO
                </div>
  
                <div className="h-[340px] w-full bg-transparent rounded-none border border-zinc-800/60 mt-2 relative overflow-hidden">
                  <MapContainer 
                    center={[lat, lng]} 
                    zoom={14} 
                    style={{ height: '100%', width: '100%', background: '#09090b' }}
                    zoomControl={false}
                    attributionControl={false}
                  >
                    {/* Capa de Azulejos Oscuros de Precisión */}
                    <TileLayer
                      url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
                      attribution='&copy; <a href="https://carto.com/">CARTO</a>'
                    />
                    
                    {/* Trazado de la trayectoria recorrida del viaje */}
                    {historialRuta.length > 0 && (
                      <Polyline 
                        positions={historialRuta} 
                        pathOptions={{ color: '#06b6d4', weight: 2, opacity: 0.8, dashArray: '4, 4' }} 
                      />
                    )}

                    {/* Marcador vectorial de la posición en tiempo real */}
                    <Marker position={[lat, lng]} />
                  </MapContainer>
                </div>
              </div>

            </div>

          </div>

          {/* Marquesinas Animadas / Tickers */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 pt-2">
            
            {/* Ticker 1: Fisica y Telemetria Reciente */}
            <div className="h-48 overflow-y-auto bg-black/10 backdrop-blur-[1px] rounded-none border border-zinc-800/60 p-3 font-mono text-xs transition-all duration-300 ease-in-out hover:border-amber-500/80 hover:shadow-[0_0_15px_rgba(245,158,11,0.15)]">
              <span className="text-[11px] text-zinc-500 font-bold tracking-normal border-b border-zinc-800 pb-1 mb-2 block">
                root@smartfleet:~# telemetry.log
              </span>
              <div className="space-y-1">
                {points.slice().reverse().map((p, idx) => {
                  const timeVal = p?.tiempo ?? "N/A"
                  const velVal = typeof p?.velocidad === 'number' ? p.velocidad : 0.0
                  const acelVal = typeof p?.aceleracion === 'number' ? p.aceleracion : 0.0
                  const isNew = idx === 0
                  return (
                    <div
                      key={`phys-tick-${idx}`}
                      className="flex items-center gap-1"
                      style={isNew ? { animation: 'fadeInLog 0.2s ease-out' } : {}}
                    >
                      <span className="text-zinc-500">[{timeVal}]</span>
                      <span className={`font-mono ${ isNew ? 'text-emerald-400 drop-shadow-[0_0_2px_rgba(16,185,129,0.35)]' : 'text-emerald-500/70' }`}>
                        [OK] VEL: {velVal.toFixed(1)} km/h | ACEL: {acelVal.toFixed(2)} m/s²
                      </span>
                    </div>
                  )
                })}
              </div>
            </div>

            {/* Ticker 2: Auditoria y Arquitectura de Pipeline */}
            <div className="h-48 overflow-y-auto bg-black/10 backdrop-blur-[1px] rounded-none border border-zinc-800/60 p-3 font-mono text-xs transition-all duration-300 ease-in-out hover:border-amber-500/80 hover:shadow-[0_0_15px_rgba(245,158,11,0.15)]">
              <span className="text-[11px] text-zinc-500 font-bold tracking-normal border-b border-zinc-800 pb-1 mb-2 block">
                root@smartfleet:~# pipeline_audit.log
              </span>
              <div className="space-y-1">
                {[
                  { label: 'MOTOR_PROCESAMIENTO', value: displayDoc?.arquitectura?.motor_procesamiento },
                  { label: 'PATRON_ETL',           value: displayDoc?.arquitectura?.patron_etl },
                  { label: 'ALGORITMO_VARIANZA',   value: displayDoc?.arquitectura?.algoritmo_varianza },
                  { label: 'PATRON_PERSISTENCIA',  value: displayDoc?.arquitectura?.patron_persistencia },
                  { label: 'PRINCIPIOS',           value: displayDoc?.arquitectura?.principios },
                ].map(({ label, value }, idx) => (
                  <div
                    key={label}
                    className="flex items-center gap-1"
                    style={idx === 0 ? { animation: 'fadeInLog 0.2s ease-out' } : {}}
                  >
                    <span className="text-zinc-500">[{displayDoc?.timestamp_procesamiento ?? "N/A"}]</span>
                    <span className={`font-mono ${ idx === 0 ? 'text-emerald-400 drop-shadow-[0_0_2px_rgba(16,185,129,0.35)]' : 'text-emerald-500/70' }`}>
                      [INFO] {label}: {value ?? "N/A"}
                    </span>
                  </div>
                ))}
              </div>
            </div>

          </div>

        </div>
      )}
    </div>
  )
}
