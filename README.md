# MentalPRO — Analítica y Riesgo Psicosocial

Esta es la rama principal **Backend (ETL y API de Datos)** del proyecto MentalPRO. Este repositorio se encarga exclusivamente del procesamiento, limpieza, validación (según Resolución 2764/2022) y estructuración de los datos, listos para ser consumidos por aplicaciones de nivel Frontend (P.ej. Next.js).

## Stack Tecnológico

- **Core Backend:** Python 3.11+, Pandas, PyArrow
- **API (Opcional/Futuro próximo):** FastAPI, Uvicorn (Para servir los archivos `.parquet` al Frontend)
- **Frontend (Repositorio Externo):** Next.js, React, Tailwind CSS

## Estructura del Repositorio

* `data/raw/`: Datasets originales (ignorados en git).
* `data/processed/`: Archivos `.parquet` generados por el pipeline, limpios y validados.
* `docs/`: Documentación estricta de reglas de negocio, pipeline y metadatos.
* `scripts/`: Módulos de Python que componen el ETL y el Scoring de la batería.
* `Dashboards/`: Antiguos visualizadores en HTML estático (Deprecados en favor de interfaz Next.js).

## Configuración y Ejecución

1. Activar el entorno virtual:
   `.\venv\Scripts\Activate.ps1`
2. Instalar dependencias:
   `pip install -r requirements.txt`
3. Ejecutar el pipeline (En orden):
   Ver la carpeta `docs/agents.md` o `docs/pipeline.md` para el paso a paso detallado.
