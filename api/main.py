from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import time

# Inicializar API
app = FastAPI(
    title="MentalPRO Backend API",
    description="Motor de Datos de Riesgo Psicosocial (Capa 2)",
    version="3.0.0"
)

# Configurar CORS para permitir que Next.js (Localhost Front) lea los datos
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restringir en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {
        "status": "online", 
        "engine": "MentalPRO ETL Data Server",
        "timestamp": time.time()
    }

from .routers import v1_riesgo
from .routers import v3_gerencial_asis

# Registrar routers
app.include_router(v1_riesgo.router)
app.include_router(v3_gerencial_asis.router)

# Los routers v2_gestion y v4_asis se añadirán tras validación ETL.
