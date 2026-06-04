const { useState, useEffect, useRef } = React;

function AnimatedNumber({ value, duration = 800, suffix = "" }) {
    const [displayValue, setDisplayValue] = useState(value);
    const [flash, setFlash] = useState(false);
    const prevValueRef = useRef(value);

    useEffect(() => {
        if (prevValueRef.current === value) return;
        prevValueRef.current = value;

        // Activar flash de actualización
        setFlash(true);
        const flashTimeout = setTimeout(() => setFlash(false), 800);

        const start = parseFloat(displayValue) || 0;
        const end = parseFloat(value) || 0;
        if (isNaN(start) || isNaN(end)) {
            setDisplayValue(value);
            return () => clearTimeout(flashTimeout);
        }

        const startTime = performance.now();

        let animationFrameId;
        const animate = (now) => {
            const progress = Math.min((now - startTime) / duration, 1);
            const ease = progress * (2 - progress); // easeOutQuad
            const current = start + (end - start) * ease;
            
            if (Number.isInteger(end)) {
                setDisplayValue(Math.round(current));
            } else {
                setDisplayValue(parseFloat(current.toFixed(2)));
            }

            if (progress < 1) {
                animationFrameId = requestAnimationFrame(animate);
            } else {
                setDisplayValue(end);
            }
        };

        animationFrameId = requestAnimationFrame(animate);

        return () => {
            clearTimeout(flashTimeout);
            cancelAnimationFrame(animationFrameId);
        };
    }, [value]);

    return (
        <span class={`transition-all duration-300 ${flash ? 'text-emerald-300 font-bold drop-shadow-[0_0_8px_rgba(52,211,153,0.6)]' : ''}`}>
            {typeof displayValue === 'number' ? displayValue.toLocaleString() : displayValue}
            {suffix}
        </span>
    );
}

function CustomDropdown({ label, options, value, onChange }) {
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef(null);

    const selectedOption = options.find(opt => opt.value === value) || options[0];

    useEffect(() => {
        function handleClickOutside(event) {
            if (containerRef.current && !containerRef.current.contains(event.target)) {
                setIsOpen(false);
            }
        }
        document.addEventListener("mousedown", handleClickOutside);
        return () => document.removeEventListener("mousedown", handleClickOutside);
    }, []);

    return (
        <div class="flex flex-col relative" ref={containerRef}>
            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">{label}</span>
            
            {/* Trigger Button */}
            <button 
                onClick={() => setIsOpen(!isOpen)}
                type="button"
                class={`bg-slate-900/80 hover:bg-slate-850/80 border rounded-lg px-3 py-1.5 text-xs text-slate-200 focus:outline-none font-medium font-mono flex items-center justify-between min-w-[180px] text-left transition duration-200 ${
                    isOpen 
                    ? 'border-emerald-500/50 shadow-[0_0_10px_rgba(16,185,129,0.15)]' 
                    : 'border-white/10 hover:border-emerald-500/30'
                }`}
            >
                <span>{selectedOption.label}</span>
                <svg class={`w-4 h-4 ml-2 text-slate-500 transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M19 9l-7 7-7-7" />
                </svg>
            </button>

            {/* Dropdown Options List */}
            {isOpen && (
                <div class="absolute top-[100%] left-0 mt-1.5 w-full z-50 bg-slate-950 border border-slate-800/80 shadow-[0_4px_20px_rgba(0,0,0,0.5)] rounded-lg py-1 overflow-hidden backdrop-blur-md animate-fade-in">
                    {options.map((option) => {
                        const isSelected = option.value === value;
                        return (
                            <div 
                                key={option.value}
                                onClick={() => {
                                    onChange(option.value);
                                    setIsOpen(false);
                                }}
                                class={`px-3 py-2 text-xs font-mono cursor-pointer transition duration-150 flex items-center justify-between ${
                                    isSelected 
                                    ? 'bg-emerald-500/10 text-emerald-400 font-bold' 
                                    : 'text-slate-300 hover:bg-slate-900 hover:text-white'
                                }`}
                            >
                                <span>{option.label}</span>
                                {isSelected && (
                                    <svg class="w-3.5 h-3.5 text-emerald-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                        <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
                                    </svg>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

function App() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [isLoading, setIsLoading] = useState(true);
    const [apiFinished, setApiFinished] = useState(false);
    const [timerFinished, setTimerFinished] = useState(false);
    const [error, setError] = useState(null);
    const [apiConnected, setApiConnected] = useState(false);
    const [animateIn, setAnimateIn] = useState(false);
    
    const [selectedFleet, setSelectedFleet] = useState("all");
    const [selectedPeriod, setSelectedPeriod] = useState("24h");
    
    // Constantes para el anillo SVG
    const circumference = 2 * Math.PI * 40;
    const [dashOffset, setDashOffset] = useState(circumference);

    const fetchData = async (isFirstLoad = false) => {
        try {
            const response = await fetch("http://127.0.0.1:8000/api/v1/fleet/analytics");
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const json = await response.json();
            setData(json);
            setApiConnected(true);
            setError(null);
            
            const score = json.ia_predictiva.score_riesgo_global;
            const targetOffset = circumference * (1 - score / 100);

            // Animar la carga circular del anillo
            if (isFirstLoad || loading) {
                setDashOffset(circumference);
                setTimeout(() => {
                    setDashOffset(targetOffset);
                }, 150);
            } else {
                setDashOffset(targetOffset);
            }
        } catch (err) {
            setError(err.message);
            setApiConnected(false);
        } finally {
            setLoading(false);
            if (isFirstLoad) {
                setApiFinished(true);
            }
        }
    };

    // Consulta inicial y polling cada 4 segundos
    useEffect(() => {
        fetchData(true);
        const interval = setInterval(() => {
            fetchData(false);
        }, 4000);
        return () => clearInterval(interval);
    }, []);

    // Temporizador de 3 segundos para asegurar un mínimo de visualización del Splash Screen
    useEffect(() => {
        const timer = setTimeout(() => {
            setTimerFinished(true);
        }, 3000);
        return () => clearTimeout(timer);
    }, []);

    // Ocultar Splash Screen solo cuando la API haya terminado y el temporizador de 3s se haya cumplido
    useEffect(() => {
        if (apiFinished && timerFinished) {
            setIsLoading(false);
        }
    }, [apiFinished, timerFinished]);

    // Desvanecimiento suave controlado una vez finaliza el loading
    useEffect(() => {
        if (!loading && data) {
            const timer = setTimeout(() => setAnimateIn(true), 50);
            return () => clearTimeout(timer);
        } else {
            setAnimateIn(false);
        }
    }, [loading, data]);

    // Clasificación de scoring y estilos adaptativos
    const getRiskLevel = (score) => {
        if (score <= 30) return { label: "Seguro", color: "text-emerald-400", border: "border-emerald-500/30", bg: "bg-emerald-500/10" };
        if (score <= 65) return { label: "Precaución", color: "text-amber-400", border: "border-amber-500/30", bg: "bg-amber-500/10" };
        return { label: "Crítico", color: "text-rose-400", border: "border-rose-500/30", bg: "bg-rose-500/10" };
    };

    const risk = data ? getRiskLevel(data.ia_predictiva.score_riesgo_global) : null;

    return (
        <div class="flex h-screen w-screen relative overflow-hidden">
            {/* Splash Screen Loader (Capa de carga a pantalla completa) */}
            {isLoading && (
                <div class="fixed inset-0 bg-[#0A0F1D] flex flex-col items-center justify-center z-[9999] transition-all duration-700 ease-in-out">
                    {/* Cyber-Grid de fondo técnico */}
                    <div class="absolute inset-0 pointer-events-none bg-[radial-gradient(#0f172a_1px,transparent_1px)] [background-size:24px_24px] opacity-25"></div>
                    <div class="absolute inset-0 pointer-events-none" style={{
                        backgroundImage: `
                            linear-gradient(rgba(16, 185, 129, 0.03) 1px, transparent 1px), 
                            linear-gradient(90deg, rgba(16, 185, 129, 0.03) 1px, transparent 1px)
                        `,
                        backgroundSize: '40px 40px'
                    }}></div>
                    
                    {/* Glow esmeralda difuminado */}
                    <div class="absolute w-[450px] h-[450px] bg-emerald-500/10 rounded-full blur-[120px] mix-blend-screen pointer-events-none animate-pulse" style={{ animationDuration: '4s' }}></div>
                    <div class="absolute w-[250px] h-[250px] bg-emerald-700/5 rounded-full blur-[80px] pointer-events-none"></div>

                    <div class="relative flex flex-col items-center text-center max-w-sm px-6 space-y-8 animate-fade-in">
                        {/* Glowing Logo Circle with Drop Shadow */}
                        <div class="relative flex items-center justify-center w-28 h-28 rounded-3xl border border-emerald-500/30 bg-slate-950/80 shadow-[0_0_50px_rgba(16,185,129,0.15)] filter drop-shadow-[0_0_20px_rgba(16,185,129,0.3)] transition-transform duration-500 hover:scale-105">
                            {/* Anillos de pulsación */}
                            <div class="absolute -inset-1.5 rounded-3xl border border-emerald-500/20 animate-pulse opacity-40"></div>
                            <div class="absolute -inset-3 rounded-3xl border border-emerald-500/10 animate-ping opacity-25" style={{ animationDuration: '3s' }}></div>
                            
                            {/* Isotipo con drop-shadow */}
                            <svg class="w-12 h-12 text-emerald-400 filter drop-shadow-[0_0_8px_rgba(52,211,153,0.5)]" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M9 17a2 2 0 11-4 0 2 2 0 014 0zM19 17a2 2 0 11-4 0 2 2 0 014 0z" />
                                <path stroke-linecap="round" stroke-linejoin="round" d="M13 16V6a1 1 0 00-1-1H4a1 1 0 00-1-1H4a1 1 0 00-1 1v10a1 1 0 001 1h1m8-1a1 1 0 011-1v-4h4.182a1 1 0 01.778.368l2.678 3.346a1 1 0 01.162.586V16a1 1 0 01-1 1h-1m-4-1a1 1 0 011-1h2" />
                            </svg>
                        </div>

                        <div class="space-y-4 flex flex-col items-center">
                            <div class="space-y-1">
                                <h2 class="text-3xl font-black tracking-[0.2em] text-white font-sans mr-[-0.2em] filter drop-shadow-[0_2px_10px_rgba(0,0,0,0.5)]">SMARTFLEET</h2>
                                <p class="text-[10px] font-bold tracking-[0.4em] text-emerald-400/90 uppercase font-mono mr-[-0.4em]">Predictive Analytics</p>
                            </div>
                            
                            {/* Barra de progreso lineal de carga de sistema */}
                            <div class="w-56 h-[3px] bg-slate-950/80 rounded-full overflow-hidden border border-white/5 relative shadow-inner">
                                <div class="absolute top-0 bottom-0 left-0 w-1/2 bg-gradient-to-r from-transparent via-emerald-400 to-transparent animate-infinity-load rounded-full"></div>
                            </div>
                            
                            <div class="flex items-center space-x-2 text-[10px] font-semibold font-mono text-slate-400">
                                <span class="h-1.5 w-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
                                <span>Inicializando sistema de telemetría...</span>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Elementos Geométricos en Movimiento (Floating Data Lines) */}
            <div class="absolute inset-0 z-0 overflow-hidden pointer-events-none">
                {/* Line 1 - Horizontal Jade */}
                <div class="absolute top-[15%] left-0 w-full h-[1px] bg-gradient-to-r from-transparent via-emerald-500/35 to-transparent animate-float-data-1"></div>
                {/* Line 2 - Horizontal Teal */}
                <div class="absolute top-[45%] left-0 w-full h-[1px] bg-gradient-to-r from-transparent via-teal-500/25 to-transparent animate-float-data-2"></div>
                {/* Line 3 - Horizontal Lime */}
                <div class="absolute top-[75%] left-0 w-full h-[1px] bg-gradient-to-r from-transparent via-lime-500/30 to-transparent animate-float-data-1"></div>
                {/* Line 4 - Vertical Emerald */}
                <div class="absolute left-[25%] top-0 w-[1px] h-full bg-gradient-to-b from-transparent via-emerald-500/20 to-transparent animate-float-data-3"></div>
                {/* Line 5 - Vertical Teal */}
                <div class="absolute left-[75%] top-0 w-[1px] h-full bg-gradient-to-b from-transparent via-teal-500/25 to-transparent animate-float-data-4"></div>
            </div>

            {/* 1. SIDEBAR DE ARQUITECTURA */}
            <aside class="w-80 bg-slate-950/40 border-r border-slate-900/30 p-6 flex flex-col justify-between shrink-0 h-full overflow-y-auto backdrop-blur-md z-10">
                <div class="space-y-6">
                    {/* Logo */}
                    <div class="flex items-center space-x-3">
                        <div class="p-1.5 rounded-lg bg-slate-900/50 border border-slate-800 shadow-sm shrink-0">
                            <svg class="w-5 h-5 text-emerald-400 transition-all duration-300" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M9 17a2 2 0 11-4 0 2 2 0 014 0zM19 17a2 2 0 11-4 0 2 2 0 014 0z" />
                                <path stroke-linecap="round" stroke-linejoin="round" d="M13 16V6a1 1 0 00-1-1H4a1 1 0 00-1 1v10a1 1 0 001 1h1m8-1a1 1 0 011-1v-4h4.182a1 1 0 01.778.368l2.678 3.346a1 1 0 01.162.586V16a1 1 0 01-1 1h-1m-4-1a1 1 0 011-1h2" />
                            </svg>
                        </div>
                        <div>
                            <h1 class="font-extrabold text-lg tracking-tight text-white">SmartFleet Pro</h1>
                            <p class="text-xs text-slate-400 font-medium">Decoupled UI v2.0.0</p>
                        </div>
                    </div>
                    
                    <hr class="border-slate-900/40" />

                    {/* Estado de la API */}
                    <div>
                        <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Conectividad</h2>
                        <div class={`flex items-center space-x-3 rounded-lg border p-3 ${apiConnected ? 'bg-emerald-500/5 border-emerald-500/20 shadow-[0_0_10px_rgba(16,185,129,0.2)]' : 'bg-rose-500/5 border-rose-500/20'}`}>
                            <span class="relative flex h-3.5 w-3.5">
                                {apiConnected && <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>}
                                <span class={`relative inline-flex rounded-full h-3.5 w-3.5 ${apiConnected ? 'bg-emerald-500 animate-pulse-fast' : 'bg-rose-500'}`}></span>
                            </span>
                            <div>
                                <div class="text-sm font-bold text-slate-200">{apiConnected ? 'API Conectada' : 'API Desconectada'}</div>
                                <div class="text-xs text-slate-400 font-mono">http://localhost:8000</div>
                            </div>
                        </div>
                    </div>

                    {/* Especificaciones Técnicas */}
                    <div>
                        <div class="flex items-center space-x-2 mb-2">
                            <div class="p-1.5 rounded-lg bg-slate-900/50 border border-slate-800 shadow-sm shrink-0">
                                <svg class="w-4 h-4 text-slate-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                    <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                                </svg>
                            </div>
                            <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Architecture Spec</h2>
                        </div>
                        <pre class="bg-slate-950/80 border border-slate-900/30 rounded-lg p-3 text-[10.5px] text-slate-300 font-mono leading-relaxed overflow-x-auto whitespace-pre-wrap select-all">
{`[Pipeline Engine]
- PySpark 3.5 (JVM)
- Cassandra Temporal
- MongoDB Documental
- Postgres Bitácora ACID

[Mathematical Core]
Teorema Koenig-Huygens:
Var(X) = E[X²] - (E[X])²
Double Precision Casting

[Design Patterns]
- Clean Architecture
- SOLID (DI + Repository)
- 12-Factor App Config`}
                        </pre>
                    </div>
                </div>

                {/* Footer */}
                <div class="text-[10px] text-slate-400 font-medium tracking-tight">
                    SmartFleet Pro v2.0.0 | Developed by Juan José Parra
                </div>
            </aside>

            {/* MAIN CONTENT AREA WITH 3D PERSPECTIVE */}
            <main class="flex-1 h-screen flex flex-col overflow-hidden p-8" style={{ perspective: "1200px" }}>
                {/* Header Hero (Static at the top) */}
                <header class="shrink-0 mb-6 bg-slate-950/80 backdrop-blur-md border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-2xl p-6 overflow-hidden glow-card glow-emerald">
                    <div class="absolute right-0 top-0 w-80 h-80 bg-emerald-500/5 rounded-full blur-3xl pointer-events-none"></div>
                    <div class="flex items-center space-x-2 text-xs font-semibold text-emerald-400 tracking-wider uppercase mb-1">
                        <span class="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
                        <span>Reactivo Dashboard</span>
                    </div>
                    <h2 class="text-2xl font-extrabold text-slate-100 tracking-tight mb-2">Centro de Inteligencia de Flota</h2>
                    <p class="text-sm text-slate-400 max-w-xl font-normal leading-relaxed">
                        Ingestión políglota distribuida a gran escala. Exposición REST desacoplada consumida en caliente mediante React hooks.
                    </p>
                </header>

                {/* Scrollable Content Area */}
                <div class="flex-1 overflow-y-auto pr-1 space-y-6">
                    {/* Fila de Controles Interactivos (Filtros - Primer elemento en scroll) */}
                    <div class="flex flex-wrap items-center justify-between gap-4 p-4 rounded-xl border border-white/10 bg-slate-950/80 backdrop-blur-md glow-card relative z-30">
                        <div class="flex flex-wrap items-center gap-4">
                            {/* Selector de Flota/Vehículo */}
                            <CustomDropdown 
                                label="Selección Activa"
                                options={[
                                    { value: "all", label: "Flota Completa (Zona Norte)" },
                                    { value: "v1", label: "Vehículo 101 - Volvo FMX" },
                                    { value: "v2", label: "Vehículo 102 - Scania R450" },
                                    { value: "v3", label: "Vehículo 103 - Mercedes Actros" }
                                ]}
                                value={selectedFleet}
                                onChange={setSelectedFleet}
                            />

                            {/* Rango de fechas / Selector de Tiempo */}
                            <CustomDropdown 
                                label="Período de Análisis"
                                options={[
                                    { value: "24h", label: "Últimas 24 horas" },
                                    { value: "7d", label: "Últimos 7 días" },
                                    { value: "30d", label: "Últimos 30 días" }
                                ]}
                                value={selectedPeriod}
                                onChange={setSelectedPeriod}
                            />
                        </div>

                        {/* Botón de Exportar Datos */}
                        <button class="bg-emerald-500/10 hover:bg-emerald-500/20 border border-emerald-500/30 hover:border-emerald-500/50 rounded-lg px-4 py-2 text-xs text-emerald-400 hover:text-emerald-300 font-bold flex items-center space-x-2 transition-all duration-300 shadow-[0_0_15px_rgba(16,185,129,0.1)] hover:shadow-[0_0_20px_rgba(16,185,129,0.2)] md:mt-0 mt-2">
                            <svg class="w-4 h-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                            </svg>
                            <span>Exportar Datos</span>
                        </button>
                    </div>

                {/* SKELETON LOADER / ERROR STATES */}
                {loading && !data ? (
                    <div class="space-y-6 animate-pulse animate-fade-in mt-8">
                        {/* Hero Card Skeleton */}
                        <div class="h-44 bg-slate-950/60 border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-2xl flex items-center p-6 space-x-6">
                            <div class="w-20 h-20 rounded-full bg-slate-950 border border-emerald-500/10 animate-pulse shrink-0"></div>
                            <div class="flex-1 space-y-3">
                                <div class="h-4 bg-slate-950 rounded w-1/4"></div>
                                <div class="h-3 bg-slate-950 rounded w-3/4"></div>
                                <div class="h-3 bg-slate-950 rounded w-1/2"></div>
                            </div>
                        </div>
                        {/* KPIs Grid Skeleton */}
                        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
                            <div class="h-28 bg-slate-950/60 border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] p-5 space-y-3">
                                <div class="h-3 bg-slate-950 rounded w-2/3"></div>
                                <div class="h-6 bg-slate-950 rounded w-1/3"></div>
                                <div class="h-3 bg-slate-950 rounded w-1/2"></div>
                            </div>
                            <div class="h-28 bg-slate-950/60 border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] p-5 space-y-3">
                                <div class="h-3 bg-slate-950 rounded w-2/3"></div>
                                <div class="h-6 bg-slate-950 rounded w-1/3"></div>
                                <div class="h-3 bg-slate-950 rounded w-1/2"></div>
                            </div>
                            <div class="h-28 bg-slate-950/60 border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] p-5 space-y-3">
                                <div class="h-3 bg-slate-950 rounded w-2/3"></div>
                                <div class="h-6 bg-slate-950 rounded w-1/3"></div>
                                <div class="h-3 bg-slate-950 rounded w-1/2"></div>
                            </div>
                        </div>
                    </div>
                ) : error ? (
                    <div class="bg-rose-500/10 border border-rose-500/20 rounded-xl p-6 text-center space-y-3 max-w-md mx-auto my-12 shadow-xl animate-fade-in mt-8">
                        <div class="flex justify-center">
                            <div class="p-1.5 rounded-lg bg-slate-900/50 border border-slate-800 shadow-sm">
                                <svg class="w-6 h-6 text-rose-500 transition-all duration-300" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                    <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                                </svg>
                            </div>
                        </div>
                        <h3 class="text-lg font-bold text-slate-200">Fallo de Comunicación NoSQL</h3>
                        <p class="text-sm text-slate-400 leading-relaxed font-mono">
                            {error}. Compruebe que el servicio API de FastAPI esté levantado con <code>python app_api.py</code> en el puerto 8000.
                        </p>
                        <button onClick={() => fetchData(true)} class="mt-2 bg-slate-900 border border-slate-800 hover:bg-slate-850 px-4 py-2 rounded-lg text-xs font-semibold transition">
                            Reintentar conexión
                        </button>
                    </div>
                ) : (
                    <div class={`space-y-6 transition-opacity duration-1000 ease-in-out ${animateIn ? 'opacity-100' : 'opacity-0'} mt-8`}>
                        {/* 4. PANEL HERO DE IA (Card 1: Global Risk Score - Esmeralda) */}
                        <section class={`relative border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-2xl p-6 overflow-hidden bg-slate-950/80 backdrop-blur-md glow-card glow-emerald ${risk.border}`}>
                            <div class="flex items-center justify-between mb-4">
                                <div>
                                    <span class="text-xs font-bold text-slate-400 uppercase tracking-widest">Analytics Engine v2.0</span>
                                    <h3 class="text-lg font-extrabold text-slate-200">Global Risk Score · IA</h3>
                                </div>
                                <div class={`px-3 py-1 rounded-full text-xs font-bold ${risk.bg} ${risk.color}`}>
                                    Nivel: {risk.label}
                                </div>
                            </div>
                            
                            <div class="flex items-center space-x-6">
                                {/* Progress Ring */}
                                <div class="relative shrink-0 flex items-center justify-center">
                                    <svg class="w-24 h-24 transform -rotate-90">
                                        <defs>
                                            <linearGradient id="risk-gradient" x1="0%" y1="0%" x2="100%" y2="100%">
                                                <stop offset="0%" stop-color="#34d399" />
                                                <stop offset="100%" stop-color="#047857" />
                                            </linearGradient>
                                        </defs>
                                        <circle cx="48" cy="48" r="40" stroke="rgba(30, 41, 59, 0.3)" stroke-width="8" fill="transparent" />
                                        <circle cx="48" cy="48" r="40" stroke-width="8" fill="transparent"
                                            stroke="url(#risk-gradient)"
                                            class="ring-progress"
                                            stroke-dasharray={`${2 * Math.PI * 40}`}
                                            stroke-dashoffset={dashOffset}
                                        />
                                    </svg>
                                    <div class="absolute text-xl font-black text-slate-100"><AnimatedNumber value={data.ia_predictiva.score_riesgo_global} suffix="%" /></div>
                                </div>

                                <div class="flex-1 space-y-2">
                                    <div class="flex justify-between text-xs font-bold">
                                        <span class="text-slate-400">Puntaje General de Riesgo</span>
                                        <span class={risk.color}>{data.ia_predictiva.score_riesgo_global} / 100</span>
                                    </div>
                                    <div class="w-full bg-slate-900/60 rounded-full h-3 overflow-hidden border border-slate-800/20">
                                        <div class={`h-full rounded-full transition-all duration-1000 ease-out ${
                                            data.ia_predictiva.score_riesgo_global <= 30 ? 'bg-emerald-500' :
                                            data.ia_predictiva.score_riesgo_global <= 65 ? 'bg-amber-500' : 'bg-rose-500'
                                        }`} style={{ width: `${data.ia_predictiva.score_riesgo_global}%` }}></div>
                                    </div>
                                    <p class="text-xs text-slate-400 leading-relaxed font-normal">
                                        Comportamiento calibrado ponderando excesos de velocidad ({data.ia_predictiva.ponderaciones_matriz.exceso_velocidad * 100}%), varianza de aceleración ({data.ia_predictiva.ponderaciones_matriz.varianza_acel * 100}%) y frenadas bruscas ({data.ia_predictiva.ponderaciones_matriz.frenadas_bruscas * 100}%).
                                    </p>
                                </div>
                            </div>
                        </section>

                        {/* 3. TARJETAS KPI GRID (3 columnas) */}
                        <section class="grid grid-cols-1 md:grid-cols-3 gap-6">
                            {/* Card 2: Muestras GPS - Verde Eléctrico */}
                            <div class="bg-slate-950/80 backdrop-blur-md border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-xl p-5 relative group glow-card glow-green">
                                <div class="flex items-center justify-between mb-2">
                                    <span class="text-sm font-semibold text-slate-400">Muestras GPS Procesadas</span>
                                    <div class="bg-green-500/10 p-2 rounded-xl border border-green-500/30 shadow-[0_0_15px_rgba(74,222,128,0.2)]">
                                        <svg class="w-5 h-5 text-green-400 transition-all duration-300" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                            <path stroke-linecap="round" stroke-linejoin="round" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
                                        </svg>
                                    </div>
                                </div>
                                <div class="text-2xl font-black text-slate-100 mb-1">
                                    <AnimatedNumber value={data.metricas_basicas.total_muestras} /> <span class="text-xs text-slate-400 font-medium">pts</span>
                                </div>
                                <div class="text-[11px] text-emerald-400 font-bold px-2 py-0.5 rounded-full border border-emerald-500/20 bg-emerald-500/10 inline-flex items-center w-fit">
                                    Data Lake Cassandra
                                </div>
                            </div>

                            {/* Card 3: Velocidad Promedio - Teal Menta */}
                            <div class="bg-slate-950/80 backdrop-blur-md border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-xl p-5 relative group glow-card glow-teal">
                                <div class="flex items-center justify-between mb-2">
                                    <span class="text-sm font-semibold text-slate-400">Velocidad Promedio</span>
                                    <div class="bg-teal-500/10 p-2 rounded-xl border border-teal-500/30 shadow-[0_0_15px_rgba(45,212,191,0.2)]">
                                        <svg class="w-5 h-5 text-teal-400 transition-all duration-300" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                            <path stroke-linecap="round" stroke-linejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646m.354 5.656V4.5m4.5 4.5l3-3m-3 3h4.5" />
                                        </svg>
                                    </div>
                                </div>
                                <div class="text-2xl font-black text-slate-100 mb-1">
                                    <AnimatedNumber value={data.metricas_basicas.velocidad_promedio_kmh} /> <span class="text-xs text-slate-400 font-medium">km/h</span>
                                </div>
                                <div class={`text-[11px] font-bold px-2 py-0.5 rounded-full border inline-flex items-center w-fit ${
                                    data.metricas_basicas.velocidad_promedio_kmh <= data.metricas_basicas.umbral_velocidad_kmh 
                                    ? 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20' 
                                    : 'text-rose-400 bg-rose-500/10 border-rose-500/20'
                                }`}>
                                    {data.metricas_basicas.velocidad_promedio_kmh <= data.metricas_basicas.umbral_velocidad_kmh 
                                        ? 'Dentro de límites' : 'Límite excedido'}
                                </div>
                            </div>

                            {/* Card 4: Frenadas Bruscas - Lima Reactivo */}
                            <div class="bg-slate-950/80 backdrop-blur-md border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-xl p-5 relative group glow-card glow-lime">
                                <div class="flex items-center justify-between mb-2">
                                    <span class="text-sm font-semibold text-slate-400">Frenadas Bruscas</span>
                                    <div class={
                                        data.ia_predictiva.frenadas_bruscas_count === 0 
                                        ? "bg-lime-500/10 p-2 rounded-xl border border-lime-500/30 shadow-[0_0_15px_rgba(163,230,53,0.2)]"
                                        : "bg-red-950/40 p-2 rounded-xl border border-red-700/50 shadow-[0_0_15px_rgba(239,68,68,0.2)]"
                                    }>
                                        <svg class={`w-5 h-5 transition-all duration-300 ${
                                            data.ia_predictiva.frenadas_bruscas_count === 0 ? 'text-lime-400' : 'text-red-400'
                                        }`} xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                            <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                                        </svg>
                                    </div>
                                </div>
                                <div class="text-2xl font-black text-slate-100 mb-1">
                                    <AnimatedNumber value={data.ia_predictiva.frenadas_bruscas_count} /> <span class="text-xs text-slate-400 font-medium">eventos</span>
                                </div>
                                <div class={`text-[11px] font-bold px-2 py-0.5 rounded-full border inline-flex items-center w-fit ${
                                    data.ia_predictiva.frenadas_bruscas_count === 0 
                                    ? 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20' 
                                    : 'text-rose-400 bg-red-950/40 border-red-700/50'
                                }`}>
                                    {data.ia_predictiva.frenadas_bruscas_count === 0 
                                        ? 'Conducción estable' : `${data.ia_predictiva.frenadas_bruscas_count} frenados extremos`}
                                </div>
                            </div>
                        </section>

                         {/* Métricas Secundarias */}
                         <section class="grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
                             <div class="bg-slate-950/80 backdrop-blur-md border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-xl p-5 glow-card glow-malaquita">
                                 <h4 class="text-xs font-bold text-slate-400 uppercase mb-3 tracking-wider">Detalle Estadístico Físico</h4>
                                 <div class="space-y-2 font-mono text-xs text-slate-400 mt-4">
                                     <div class="flex justify-between py-2 border-b border-slate-900/20">
                                         <span class="text-slate-500 font-medium">Aceleración Varianza:</span>
                                         <span class="font-bold text-emerald-400/90">{data.ia_predictiva.aceleracion_varianza_kmhs2} (m/s²)²</span>
                                     </div>
                                     <div class="flex justify-between py-2 border-b border-slate-900/20">
                                         <span class="text-slate-500 font-medium">Velocidad Máxima:</span>
                                         <span class="font-bold text-slate-200">{data.metricas_basicas.velocidad_maxima_kmh} km/h</span>
                                     </div>
                                     <div class="flex justify-between py-2 border-b border-slate-900/20">
                                         <span class="text-slate-500 font-medium">Alertas Exceso Velocidad:</span>
                                         <span class="font-bold text-slate-200">{data.metricas_basicas.alertas_exceso_velocidad} eventos</span>
                                     </div>
                                 </div>
                             </div>
                             <div class="bg-slate-950/80 backdrop-blur-md border border-white/10 shadow-[inset_0_0_15px_rgba(255,255,255,0.03)] rounded-xl p-5 glow-card glow-menta">
                                 <h4 class="text-xs font-bold text-slate-400 uppercase mb-3 tracking-wider">Trazabilidad del Pipeline</h4>
                                 <div class="space-y-2 font-mono text-xs text-slate-400 mt-4">
                                     <div class="flex justify-between py-2 border-b border-slate-900/20">
                                         <span class="text-slate-500 font-medium">Motor de Cómputo:</span>
                                         <span class="font-bold text-teal-400/90">{data.arquitectura.motor_procesamiento}</span>
                                     </div>
                                     <div class="flex justify-between py-2 border-b border-slate-900/20">
                                         <span class="text-slate-500 font-medium">Patrón ETL:</span>
                                         <span class="font-bold text-slate-200">{data.arquitectura.patron_etl}</span>
                                     </div>
                                     <div class="flex justify-between py-2 border-b border-slate-900/20">
                                         <span class="text-slate-500 font-medium">Fecha Análisis:</span>
                                         <span class="font-bold text-slate-200">{new Date(data.fecha_analisis).toLocaleString()}</span>
                                     </div>
                                 </div>
                             </div>
                         </section>
                    </div>
                )}
                </div>
            </main>
        </div>
    );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
