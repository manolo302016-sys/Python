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

# IMPORTANTE:
# Los routers como v1_riesgo y v2_gestion se inyectarán aquí
# cuando el usuario defina los nuevos endpoints en el Documento Marco.
