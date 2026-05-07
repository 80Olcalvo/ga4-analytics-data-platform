# Arquitectura de Datos GA4 — Referencia Técnica

## Resumen

Platforma de datos end-to-end que ingiere eventos crudos de Google Analytics 4 exportados a S3, los transforma mediante una arquitectura Medallion, y expone tablas analíticas en Redshift Serverless para consumo en Power BI y QuickSight.

---

## Arquitectura Medallion

```
┌─────────────────────────────────────────────────────────────────┐
│                   ARQUITECTURA MEDALLION                    │
├────────────────────┬────────────────────┬───────────────────────┤
│   🥉 BRONCE          │   🥈 PLATA           │   🥇 ORO               │
│   Datos crudos      │   Aplanados,        │   Modelados para BI  │
│   S3 raw bucket     │   particionados     │   Redshift gold      │
└────────────────────┴────────────────────┴───────────────────────┘
```

### Capa Bronce
**Ubicación:** `s3://google-analytics-80oeste/`

Datos crudos tal como los exporta GA4: Parquet con estructuras anidadas (arrays de structs). Inmutable, fuente de verdad histórica.

- Formato: Parquet
- Catalogado por: Glue Crawler `mi-crawler-g4goole` → DB `analisysgoogle`

### Capa Plata
**Ubicación:** `s3://google-analytics-80oeste-curated/ga4_events/`

Glue ETL Job `ga4-flatten-etl` transforma los datos:
- Aplana structs y arrays anidados en columnas planas
- Convierte timestamps de microsegundos a formato legible
- Convierte `event_date` de string `yyyyMMdd` a tipo `DATE`
- Extrae parámetros clave de `event_params`
- Particiona por `event_date`

- Formato: Parquet (Snappy)
- Catalogado por: Glue Crawler `ga4-curated-crawler` → DB `ga4_curated` (6am diario)

### Capa Oro
**Ubicación:** Redshift Serverless `ga4-workgroup` → schema `gold`

Tablas materializadas con modelos de negocio listos para BI.

---

## Componentes AWS

| Componente | Nombre | Propósito |
|---|---|---|
| S3 (bronce) | `google-analytics-80oeste` | Datos crudos GA4 |
| S3 (plata) | `google-analytics-80oeste-curated` | Datos aplanados y particionados |
| Glue Crawler (bronce) | `mi-crawler-g4goole` | Cataloga datos crudos |
| Glue DB (bronce) | `analisysgoogle` | Catálogo capa bronce |
| Glue ETL Job | `ga4-flatten-etl` | Transforma bronce → plata |
| Glue Crawler (plata) | `ga4-curated-crawler` | Cataloga datos curados |
| Glue DB (plata) | `ga4_curated` | Catálogo capa plata |
| IAM Role | `redshift-spectrum-ga4` | Permisos Redshift → S3 + Glue |
| Redshift Serverless | `ga4-workgroup` | Motor de consultas, 8 RPU |
| Redshift Namespace | `ga4-analytics` | Base de datos `ga4db` |
| Vista analítica | `public.ga4_analytics` | Acceso Spectrum → plata |
| Lambda | `ga4-gold-refresh` | Refresca tablas Gold |
| Step Functions | `ga4-etl-pipeline` | Orquesta el pipeline completo |
| EventBridge | `ga4-s3-parquet-trigger` | Trigger en nuevo .parquet |
| SNS | `ga4-pipeline-notifications` | Alertas email |

---

## Tablas Gold

| Tabla | Granularidad | Descripción |
|---|---|---|
| `gold.dim_users` | 1 fila / usuario | Perfil de usuario: adquisición, dispositivo, revenue |
| `gold.dim_sessions` | 1 fila / sesión | Métricas de sesión: páginas, engagement, conversión |
| `gold.fact_daily_events` | 1 fila / día+evento+geo+device+source | Métricas diarias agregadas |
| `gold.fact_page_performance` | 1 fila / día+página+geo+device+source | Rendimiento por página |
| `gold.dim_visitor_profile` | 1 fila / visitante | Perfil conductual completo |

---

## Campos de ga4_analytics

| Campo | Tipo | Descripción |
|---|---|---|
| event_date | DATE | Fecha del evento |
| event_name | STRING | Tipo de evento (page_view, purchase, etc.) |
| user_pseudo_id | STRING | ID anónimo del usuario |
| user_id | STRING | ID autenticado del usuario |
| session_id | STRING | ID de sesión |
| page_title | STRING | Título de la página |
| page_location | STRING | URL de la página |
| page_referrer | STRING | URL de referencia |
| device_category | STRING | desktop / mobile / tablet |
| device_os | STRING | Sistema operativo |
| device_browser | STRING | Navegador |
| geo_country | STRING | País del usuario |
| geo_region | STRING | Región/Estado |
| geo_city | STRING | Ciudad |
| traffic_source | STRING | Fuente (google, facebook, etc.) |
| traffic_medium | STRING | Medio (organic, cpc, email, etc.) |
| traffic_campaign | STRING | Nombre de campaña |
| platform | STRING | WEB / IOS / ANDROID |
| engagement_time_msec | BIGINT | Tiempo de engagement en ms |
| ga_session_number | INT | Número de sesión del usuario |
| purchase_revenue_usd | DOUBLE | Ingresos por compra en USD |
| transaction_id | STRING | ID de transacción ecommerce |
| total_item_quantity | BIGINT | Cantidad de items comprados |
| is_active_user | BOOLEAN | Si el usuario estuvo activo |

---

## Conexión Power BI

| Parámetro | Valor |
|---|---|
| Tipo | Amazon Redshift |
| Server | `ga4-workgroup.092908647087.us-east-1.redshift-serverless.amazonaws.com` |
| Puerto | `5439` |
| Base de datos | `ga4db` |
| Schema | `gold` |
| Driver | Amazon Redshift ODBC Driver v2 |

---

*Cuenta AWS: 092908647087 | Región: us-east-1*
