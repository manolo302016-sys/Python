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

# Registrar routers
app.include_router(v1_riesgo.router)

# Los routers v2_gestion, v3_gerencial, v4_asis se añadirán tras validación ETL.
