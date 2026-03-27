# RAG.md — Arquitectura del Motor RAG MentalPRO
> Sistema de Generación Aumentada por Recuperación para protocolos de intervención | v5.0

---

## 1. ARQUITECTURA GENERAL

```
PIPELINE ANALYTICS (este repo)
  ↓ Genera JSON_RAG_OUTPUT
MOTOR RAG (sistema externo)
  → Corpus v5: 4,178 chunks / 20 protocolos PROT-01…PROT-20
  → Recupera chunks relevantes por empresa + sector + líneas críticas
  → Genera plan PHVA personalizado
  → Output: PDF Protocolo de Intervención
```

---

## 2. CORPUS RAG v5.0

```
Total chunks: 4,178
Protocolos:   20 (PROT-01 a PROT-20)
Chunks por protocolo: ~15 (promedio 209 chunks c/u)
Idioma: Español (Colombia)
Dominio: Salud mental ocupacional, bienestar laboral, SST
Versión corpus: v5.0 (última actualización diciembre 2024)
```

---

## 3. PROTOCOLOS PROT-01 a PROT-20

| PROT_ID | Nombre | Línea de gestión |
|---------|--------|-----------------|
| PROT-01 | Gestión del estrés laboral | Bienestar emocional |
| PROT-02 | Apoyo psicológico individual | Bienestar emocional |
| PROT-03 | Autocuidado y salud mental | Bienestar cognitivo |
| PROT-04 | Prevención burnout sector salud | Bienestar emocional |
| PROT-05 | Manejo demandas emocionales | Bienestar emocional |
| PROT-06 | Habilidades de afrontamiento | Afrontamiento activo |
| PROT-07 | Fortalecimiento capital psicológico | Capital psicológico |
| PROT-08 | Gestión riesgo físico-biomecánico | Condiciones físicas |
| PROT-09 | Bienestar financiero | Bienestar financiero |
| PROT-10 | Prevención accidentalidad | Conductas seguras |
| PROT-11 | Convivencia y respeto laboral | Ecosistema convivencia |
| PROT-12 | Liderazgo saludable | Ecosistema liderazgo |
| PROT-13 | Gestión carga laboral | Carga trabajo saludable |
| PROT-14 | Seguridad y condiciones físicas | Condiciones físicas |
| PROT-15 | Balance vida-trabajo | Balance vida-trabajo |
| PROT-16 | Jornadas y turnos saludables | Jornadas saludables |
| PROT-17 | Gestión cambio organizacional | Cambio organizacional |
| PROT-18 | Claridad de roles y objetivos | Claridad roles |
| PROT-19 | Desarrollo de competencias | Desarrollo competencias |
| PROT-20 | Engagement y motivación | Engagement organizacional |

---

## 4. JSON_RAG_OUTPUT — Estructura

```json
{
  "empresa": "nombre_empresa",
  "sector_rag": "Servicios",
  "n_evaluados": 150,
  "fecha_evaluacion": "2024-12-01",
  "ejes_criticos": [
    {
      "eje": "Bienestar biopsicosocial",
      "score_0a1": 0.42,
      "nivel_gestion": "Intervención correctiva",
      "lineas_criticas": [
        {
          "linea": "Bienestar emocional",
          "score_0a1": 0.31,
          "nivel_gestion": "Intervención urgente",
          "prot_id": "PROT-02",
          "indicadores_criticos": ["Desgaste emocional", "Pérdida de sentido"]
        }
      ]
    }
  ],
  "protocolos_prioritarios": ["PROT-09", "PROT-18", "PROT-13"],
  "kpis_gerenciales": {
    "IAEE": 72.3, "IBET": 68.1, "IBE": 75.4
  },
  "alertas": [
    {"tipo": "critico", "mensaje": "Desgaste emocional >40% en Servicios"},
    {"tipo": "reevaluacion", "mensaje": "IntraA nivel Alto: re-evaluar en 1 año"}
  ]
}
```

---

## 5. REGLAS DE ACTIVACIÓN PROTOCOLO

```
Regla CRÍTICO (activación inmediata):
  score_eje ≤ 0.29 (Intervención urgente) → activar PROT-ID de esa línea
  % población en urgente+correctiva > 40% → notificar como crítico

Regla PRIORITARIO (activación en <90 días):
  score_eje 0.29–0.45 (Intervención correctiva) → plan acción 90 días
  % población en urgente+correctiva 20-39% → prioridad media

Regla PREVENTIVO (activación en <180 días):
  score_eje 0.45–0.65 (Mejora selectiva) → programa preventivo

Regla SECTOR (siempre aplicar):
  Siempre incluir protocolos prioritarios del sector (V2-Paso7)
  Independientemente del score — son obligaciones técnico-legales

Regla RE-EVALUACIÓN:
  Factor intralaboral total (A o B) en nivel Alto o Muy alto
  → Obligatorio re-evaluar toda la batería en máximo 12 meses
```

---

## 6. SECTORES RAG Y PROTOCOLOS PRIORITARIOS

```
Agricultura:         PROT-08, PROT-06, PROT-07, PROT-16
Comercio/financiero: PROT-09, PROT-15, PROT-20, PROT-07
Construcción:        PROT-08, PROT-14, PROT-16, PROT-10
Manufactura:         PROT-14, PROT-08, PROT-15, PROT-16
Servicios:           PROT-09, PROT-18, PROT-13, PROT-03
Transporte:          PROT-16, PROT-08, PROT-14, PROT-09
Salud:               PROT-04, PROT-05, PROT-13, PROT-10
Educación:           PROT-18, PROT-13, PROT-02, PROT-11
Admón. Pública:      PROT-17, PROT-11, PROT-19, PROT-20
Minas/energía:       PROT-08, PROT-14, PROT-10, PROT-05
```

---

## 7. ENDPOINT API RAG

```python
# API FastAPI para recibir JSON_RAG_OUTPUT y generar protocolo
# Implementada en sistema externo — integración vía HTTP

POST /api/rag/generar-protocolo
Content-Type: application/json
Body: JSON_RAG_OUTPUT (estructura arriba)

Response:
{
  'protocolo_pdf_url': 'https://...',
  'plan_phva': {...},
  'chunks_recuperados': 45,
  'modelos_usados': ['PROT-09', 'PROT-18', 'PROT-13']
}

# En dashboard V2 Sección F:
# Botón 'Generar Protocolo de Intervención' → llama al endpoint
# Placeholder en desarrollo: ##endpoint##
```