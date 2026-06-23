# Diccionario de datos — `test_tradetrack_out.parquet`

> Análisis del archivo de salida de **TradeTrack** (seguimiento de exportación de fruta fresca).
> Generado sobre datos de **prueba**: 34 filas × 93 columnas.

## 1. Resumen general

| Propiedad | Valor |
|---|---|
| Filas | 34 |
| Columnas | 93 |
| Tipo de dato (todas) | `object` (texto / string) |
| Dominio | Exportación agrícola — trazabilidad de pedido → packing → despacho → transporte → aduanas → facturación → liquidación |
| Productos | CEREZA, FRESA, ARÁNDANO |
| Modalidades | MARÍTIMO / AÉREO |
| Codificación | UTF-8 correcta (acentos y Ñ presentes; si ves `�` es solo render del terminal) |

### Hallazgo principal sobre tipos de datos
**Las 93 columnas están almacenadas como texto (`object`)**, aunque por su contenido muchas deberían ser:
- **numéricas** (pesos, cantidades, calibres, montos de factura),
- **fechas / fecha-hora** (ETD, ETA, fechas de despacho, etc.),
- **booleanas** (`CO_CORRECTO` guarda `"True"` como string).

Esto es típico de un volcado plano desde un formulario web. Antes de analizar en serio conviene **castear tipos** (ver notebook adjunto).

---

## 2. Etapas del proceso (agrupación de columnas)

### A. Registro / cabecera del pedido
| Columna | Tipo actual | Tipo sugerido | Descripción | Notas de calidad |
|---|---|---|---|---|
| `USUARIO` | object | category | Usuario que registró el pedido | 4 valores (Andy, Ansony, Edwardo/Piero) |
| `FECHA_Y_HORA` | object | datetime | Fecha y hora de registro | Formato `YYYY-MM-DD HH:MM:SS` |
| `NRO_FCL_CLIENTE` | object | string | Nº de FCL/contenedor asignado por el cliente | Hay valores combinados (`FCL002-M / FCL001-M`) |
| `CLIENTE_FINAL` | object | category | Cliente final / destinatario comercial | 7 clientes |
| `EMPRESA` | object | category | Empresa/razón social emisora (QB, QP, AP…) | 9 valores |

### B. Comercial / destino
| Columna | Tipo actual | Tipo sugerido | Descripción | Notas |
|---|---|---|---|---|
| `MODALIDAD_DE_TRANSPORTE` | object | category | MARÍTIMO / AÉREO | — |
| `MODALIDAD` | object | category | **Duplicado** de `MODALIDAD_DE_TRANSPORTE` | Candidato a eliminar |
| `CONSIGNATARIO` | object | string | Consignatario en destino | — |
| `NOTIFICANTE` | object | string | Parte a notificar (notify party) | — |
| `EXPORTADOR` | object | category | Empresa exportadora | 6 valores |
| `CONTINENTE` | object | category | Continente de destino | AMERICA/EUROPA/ASIA/… |
| `PAIS_DESTINO` | object | string | País(es) de destino | Valores combinados (`BAHAREIN / ESPAÑA`) |
| `PUERTO_DESTINO` | object | string | Puerto(s) de destino | Valores combinados |
| `PUERTO_DESTINO_1` | object | string | **Duplicado** de `PUERTO_DESTINO` | Candidato a eliminar |
| `SEMANA_DE_DESPACHO` | object | Int | Semana ISO de despacho | Numérico como texto |
| `SEMANA_OCULTO` | object | Int | **Duplicado** de `SEMANA_DE_DESPACHO` (campo auxiliar) | Candidato a eliminar |

### C. Producto / configuración de caja
| Columna | Tipo actual | Tipo sugerido | Descripción | Notas |
|---|---|---|---|---|
| `ID_PEDIDO` | object | string | Clave = `DESCRIPCION` + `NRO_FCL_CLIENTE` | Identificador derivado |
| `DESCRIPCION` | object | category | Producto/fruta | CEREZA, FRESA, ARÁNDANO… |
| `VARIEDAD` | object | category | Variedad (BILOXI, SEKOYA) | Mucho nulo (13/34) |
| `CONDICION` | object | category | Condición del producto | Solo "CONVENCIONAL" |
| `FORMATO_CAJA` | object | category | Formato de caja (GRANDE/MEDIANO/PEQUEÑO) | — |
| `CLIENTE_ETIQUETA` | object | category | Marca/etiqueta del cliente | — |
| `CLAMSHELLSCAJA` | object | Int | Nº de clamshells por caja | Mezcla "CAJA"/números |
| `PESOCAJA` | object | float | Peso por caja (kg) | Numérico como texto |
| `CAJASPALLET` | object | Int | Cajas por pallet | Numérico como texto |
| `CANT_CAJAS_TOTAL` | object | Int | Cantidad total de cajas | Numérico como texto |
| `CANT_PALLET_TOTAL` | object | Int | Cantidad total de pallets | Numérico como texto |
| `CALIBRE` | object | Int | Calibre del fruto | Numérico como texto |
| `OBSERVACIONES` | object | string | Observaciones libres | Valores "NN"/"PRUEBA"/vacío |

### D. Booking / nave / despacho
| Columna | Tipo actual | Tipo sugerido | Descripción | Notas |
|---|---|---|---|---|
| `NRO_BOOKING` | object | string | Nº de booking | Nulos (19/34); datos basura de prueba |
| `NOMBRE_DE_NAVE` | object | string | Nombre de la nave | idem |
| `NAVIERAAEROLINEA` | object | string | Naviera o aerolínea | idem |
| `PLANTA` | object | category | Planta de packing | QPACK, ALZA PERU GROUP… |
| `PUERTO_DE_ZARPE` | object | category | Puerto de zarpe (Chancay, Paita, Lima) | — |
| `PUERTO_DE_SALIDA` | object | category | Puerto/aeropuerto de salida | Casi todo nulo (2/34) |
| `PUERTO_DE_SALIDA_MARITIMO` | object | category | Puerto marítimo de salida | Chancay/Paita/Callao |
| `ETD` | object | date | Estimated Time of Departure | Formato `YYYY-MM-DD` |
| `ETA` | object | date | Estimated Time of Arrival | Formato `YYYY-MM-DD` |
| `OPERADOR__LOGISTICO` | object | category | Operador logístico (MAERSK, TPP…) | doble guion bajo en el nombre |
| `DEPOT` | object | string | Depósito de contenedores | — |
| `FECHA_DE_DESPACHO` | object | date | Fecha de despacho | `YYYY-MM-DD` |
| `FECHA_PROBABLE_DESPACHO` | object | date | Fecha probable de despacho | Casi todo nulo (3/34) |
| `FECHA_DE_DESPACHO_OCULTO` | object | date | Fecha de despacho (auxiliar) | Formato **`DD/MM/YYYY`** (distinto al resto) |
| `NRO_FCL_PACKING` | object | string | Nº FCL asignado en packing | — |
| `nro_Pedido` | object | string | Nº de pedido (código corto, p.ej. `EXC-BB-001`) | Naming inconsistente (minúsculas) |
| `FLETE_AEREO` | object | float | Costo de flete aéreo | 1 solo valor no nulo |

### E. Transporte terrestre (camión)
| Columna | Tipo actual | Tipo sugerido | Descripción |
|---|---|---|---|
| `TRANSPORTISTA` | object | string | Empresa transportista |
| `RUC_DE_TRANSPORTISTA` | object | string | RUC del transportista |
| `CHOFER` | object | string | Nombre del chofer |
| `NRO_DE_BREVETE` | object | string | Nº de licencia de conducir |
| `PLACAS` | object | string | Placa del vehículo |
| `NRO_DE_CONTENEDOR` | object | string | Nº de contenedor (transporte) |
| `FECHA_DE_RETIRO` | object | date | Fecha de retiro del contenedor |
| `HORA_DE_LLEGADA_A_PLANTA` | object | datetime (tz) | Llegada a planta | Formato ISO con zona `America/Lima` |
| `HORA_DE_SALIDA_A_PLANTA` | object | datetime (tz) | Salida de planta | idem |

### F. Aduanas / DAM
| Columna | Tipo actual | Tipo sugerido | Descripción |
|---|---|---|---|
| `ADUANAS` | object | category | Agencia/oficina de aduanas |
| `N_DAM` | object | string | Nº de DAM (Declaración Aduanera de Mercancías) |
| `CANAL` | object | category | Canal aduanero |
| `FECHA_DE_CARGA__DAM_PROV` | object | date | Fecha carga DAM provisional |
| `FECHA_DE_CARGA_DAM_REG` | object | date | Fecha carga DAM regularizada |
| `FECHA_DE_CARGA_DAM_REC` | object | date | Fecha carga DAM rectificada |

### G. Guía de remisión / packing list (PL / GR)
| Columna | Tipo actual | Tipo sugerido | Descripción |
|---|---|---|---|
| `NUMERO_DE_GUIA_DE_REMISION_GR` | object | string | Nº de guía de remisión |
| `NUMERO_DE_PALLETS_GR` | object | Int | Nº de pallets (guía) |
| `FECHA_DE_SALIDA_DE_PACKING_GR` | object | date | Fecha de salida de packing |
| `NUMERO_DE_CONTENEDOR` | object | string | Nº de contenedor (packing list) |
| `CANTIDADES_DE_CAJAS_PL` | object | Int | Cantidad de cajas (PL) |
| `KILOS_NETOS_PL` | object | float | Kilos netos (PL) |
| `KILOS_BRUTOS_PL` | object | float | Kilos brutos (PL) |
| `FUNDO_01` / `FUNDO_02` / `FUNDO_03` | object | string | Código/identificador de fundo (origen) |
| `CANTIDAD_DE_KG_FUNDO_01/02/03` | object | float | Kg aportados por cada fundo |
| `TERMOGRAFO_1_PL` / `TERMOGRAFO_2_PL` | object | string | Nº de termógrafo (cadena de frío) |

### H. Facturación
| Columna | Tipo actual | Tipo sugerido | Descripción |
|---|---|---|---|
| `NRO_BL` | object | string | Nº de Bill of Lading |
| `NRO_FACTURA` | object | string | Nº de factura |
| `TOTAL_FACTURA` | object | float | Monto total de la factura |
| `EMISION_DE_LA_FACTURA` | object | date | Fecha de emisión |
| `VENCIMIENTO_DE_LA_FACTURA` | object | date | Fecha de vencimiento |
| `NUMERO_DE_LA_GUIA_DE_REMISION` | object | string | Nº guía de remisión (factura) |
| `CANTIDAD_DE_CAJAS_FACT` | object | Int | Cantidad de cajas facturadas |
| `KILOS_NETOS_FACTURA` | object | float | Kilos netos facturados |
| `NRO_DE_EXPEDIENTE_CF` | object | string | Nº de expediente (certificado/aduana) |
| `CO_CORRECTO` | object | **bool** | Certificado de Origen correcto | Guarda `"True"` como texto |
| `VAT` | object | float | Impuesto / IVA | **100% nulo** (0/34) |

### I. Liquidación / contabilidad
| Columna | Tipo actual | Tipo sugerido | Descripción |
|---|---|---|---|
| `NUMERO_DE_GUIA_DHL` | object | string | Nº de guía DHL (envío de documentos) |
| `Fecha_de_Liquidacion` | object | date | Fecha de liquidación |
| `Total_de_Liquidacion` | object | float | Monto total de liquidación |
| `Liquidacion_x_Unidad` | object | float | Liquidación por unidad |
| `NUMERO_DE_NOTA_CONTABLE` | object | string | Nº de nota contable |
| `EMISION_DE_NOTA_CONTABLE` | object | date | Fecha de emisión de nota contable |

---

## 3. Problemas de calidad de datos detectados

1. **Todo es texto.** Conviene castear a numérico/fecha/bool antes de cualquier cálculo.
2. **Columnas duplicadas / auxiliares:** `MODALIDAD` ≈ `MODALIDAD_DE_TRANSPORTE`, `PUERTO_DESTINO_1` ≈ `PUERTO_DESTINO`, `SEMANA_OCULTO` ≈ `SEMANA_DE_DESPACHO`, `FECHA_DE_DESPACHO_OCULTO` ≈ `FECHA_DE_DESPACHO`. Revisar si se necesitan.
3. **Formatos de fecha mixtos:** la mayoría `YYYY-MM-DD`, pero `FECHA_DE_DESPACHO_OCULTO` usa `DD/MM/YYYY` y `FECHA_DE_SALIDA_DE_PACKING_GR` tiene casos como `12/03/09`. Estandarizar al parsear.
4. **Valores basura de prueba** (`asfasf`, `sdfsdf`, etc.) en casi todos los campos de transporte/aduanas/factura/liquidación — esperable por ser data de prueba, pero impide inferir tipos reales en esos campos.
5. **Campos casi vacíos:** `VAT` (0/34), `FLETE_AEREO` (1/34), `FECHA_PROBABLE_DESPACHO` (3/34), `PUERTO_DE_SALIDA` (2/34).
6. **Valores combinados** en `PAIS_DESTINO`, `PUERTO_DESTINO`, `NRO_FCL_CLIENTE` (separador ` / `) — un pedido puede tener múltiples destinos. Normalizar si se requiere análisis por destino.
7. **Inconsistencia de naming:** la mayoría en MAYÚSCULAS_SNAKE, pero `nro_Pedido`, `Fecha_de_Liquidacion`, `Total_de_Liquidacion`, etc. en mixto. Doble guion bajo en `OPERADOR__LOGISTICO` y `FECHA_DE_CARGA__DAM_PROV`.

## 4. Recomendaciones

- Definir un **esquema de tipos** (dtypes) explícito y aplicarlo al leer (ver `analisis_test_tradetrack_out.ipynb`).
- Parsear fechas con `dayfirst` por columna según su formato.
- Convertir `CO_CORRECTO` a booleano real.
- Evaluar eliminar columnas duplicadas/auxiliares antes de cargar a destino (BD / reporte).
- Cuando lleguen datos reales, recalcular % de nulos y cardinalidad para confirmar tipos definitivos.
