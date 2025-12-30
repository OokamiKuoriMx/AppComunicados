/**
 * ============================================================================
 * SERVICIO DE INTELIGENCIA ARTIFICIAL (Gemini 1.5 Flash)
 * Descripción: Procesa archivos PDF usando OCR y LLM para extracción estructurada.
 * ============================================================================
 */

// Configuración - TIER DE PAGO
const GEMINI_MODEL = 'gemini-2.0-flash-001';  // Modelo estable para producción (con facturación)
const MAX_RETRIES = 5;                     // Reintentos para errores de validación/parseo
const MAX_RATE_LIMIT_RETRIES = 3;          // Reintentos para Rate Limit (menos necesarios con pago)
const BASE_COOLDOWN_MS = 1000;             // 1 segundo entre archivos (tier de pago tiene límites altos)
const BASE_429_WAIT_MS = 5000;             // 5 segundos base si hay 429 (raro con pago)

/**
 * Procesa un PDF en Base64 y devuelve los datos estructurados.
 * NUEVO: Usa Gemini multimodal directamente (sin Drive OCR).
 * @param {object|string} payload - Objeto {content, filename} o content string (legacy).
 * @param {string} [optFilename] - Filename si payload es string.
 * @returns {object} Resultado estructurado { header, lineas, validacion, metadata }.
 */
function procesarPdfIA(payload, optFilename) {
    let base64Content = payload;
    let filename = optFilename || 'documento_desconocido.pdf';

    if (payload && typeof payload === 'object' && payload.content) {
        base64Content = payload.content;
        filename = payload.filename || filename;
    }

    const contexto = `procesarPdfIA(${filename})`;
    console.log(`[${contexto}] Iniciando procesamiento IA (Multimodal directo)...`);

    // Cool-down para evitar Rate Limit (429) en Batch
    Utilities.sleep(BASE_COOLDOWN_MS);

    try {
        // Loop de Intentos con Auto-Corrección
        let intentos = 0;
        let lastError = null;
        let resultadoFinal = null;

        while (intentos < MAX_RETRIES) {
            intentos++;
            console.log(`[${contexto}] Intento AI #${intentos}...`);

            try {
                // Llamada API con PDF multimodal (sin OCR previo)
                // INYECCIÓN DE CATÁLOGOS (RAG Ligero)
                let catalogsContext = {};
                try {
                    const cache = _loadCatalogsCache();

                    // Extract distinctive lists to guide AI
                    // Fenomenos might need to be extracted from Siniestros or a separate table if it exists. 
                    // Assuming 'siniestros' table has 'fenomeno' column or similar description.
                    // Checking TABLE_DEFINITIONS or guessing based on user request.
                    // User said "siniestros ajustadores fenomenos existentes".

                    catalogsContext = {
                        ajustadores: [...new Set(cache.ajustadores.map(a => a.nombre || a.nombreAjustador).filter(Boolean))],
                        siniestros: [...new Set(cache.siniestros.map(s => s.siniestro).filter(Boolean))],
                        // Fenomenos often live in Siniestros description or separte catalog. 
                        // I'll grab unique 'fenomeno' from siniestros table if available, or just map descriptions.
                        fenomenos: [...new Set(cache.siniestros.map(s => s.fenomeno).filter(Boolean))],
                        distritos: [...new Set(cache.distritosRiego.map(d => d.distritoRiego).filter(Boolean))] // Added Distritos
                    };
                } catch (errCatalogs) {
                    console.warn(`[${contexto}] No se pudieron cargar catálogos para contexto AI:`, errCatalogs);
                }

                const jsonResponse = _callGeminiWithPdf(base64Content, filename, lastError, catalogsContext);

                // Validar Lógica de Negocio básica (Suma)
                const validacion = _validarLogicaNegocio(jsonResponse);

                if (validacion.esValido) {
                    resultadoFinal = jsonResponse;
                    break; // Éxito
                } else {
                    // Fallo lógico (la suma no cuadra, etc), reintentar con feedback
                    console.warn(`[${contexto}] Intento #${intentos} falló validación: ${validacion.mensaje}`);
                    lastError = `Tu respuesta anterior tenía errores lógicos: ${validacion.mensaje}. Por favor rectifica y verifica los cálculos.`;
                }

            } catch (e) {
                console.warn(`[${contexto}] Error en Intento #${intentos}: ${e.message}`);
                lastError = `Ocurrió un error de formato o parseo: ${e.message}. Asegúrate de devolver JSON válido.`;
            }
        }

        if (!resultadoFinal) {
            throw new Error(`Falló tras ${MAX_RETRIES} intentos. Último error: ${lastError}`);
        }

        // Estructurar para el Importador
        return {
            success: true,
            data: {
                header: resultadoFinal.header,
                lineas: resultadoFinal.lineas
            },
            analisis: {
                intentos: intentos,
                modelo: GEMINI_MODEL
            }
        };

    } catch (error) {
        console.error(`[${contexto}] ERROR FATAL:`, error);
        return {
            success: false,
            message: error.message,
            filename: filename
        };
    }
}

/**
 * Valida reglas matemáticas y de negocio críticas antes de aceptar la respuesta IA.
 */
function _validarLogicaNegocio(json) {
    if (!json.header || !json.lineas) return { esValido: false, mensaje: "Estructura JSON incompleta (falta header o lineas)" };

    // Regla Importe Cero = Cancelado (No es error)
    const totalDoc = parseFloat(json.header.totalPdf || 0);
    if (totalDoc === 0 && (!json.lineas || json.lineas.length === 0)) {
        // Es un documento cancelado válido
        return { esValido: true };
    }

    // Validar Suma
    const sumaLineas = json.lineas.reduce((sum, l) => sum + (parseFloat(l.importe) || 0), 0);
    const diff = Math.abs(totalDoc - sumaLineas);

    if (diff > 1.0) { // Tolerancia de $1 peso
        return {
            esValido: false,
            mensaje: `El total del documento (${totalDoc}) no coincide con la suma de las líneas (${sumaLineas}). Diferencia: ${diff}`
        };
    }

    return { esValido: true };
}

/**
 * NUEVA: Envía PDF directamente a Gemini usando entrada multimodal.
 * Gemini 1.5/2.0 puede leer PDFs nativamente sin necesidad de OCR previo.
 */
function _callGeminiWithPdf(base64Content, filename, errorFeedback = null, catalogs = null) {
    const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
    if (!apiKey) throw new Error('GEMINI_API_KEY no configurada en Propiedades del Script.');

    const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${apiKey}`;

    // Construcción del Prompt
    let promptSystem = `
Eres un Auditor de Ingeniería experto. Tu misión es extraer datos de reportes financieros técnicos y devolver un JSON válido.

## REGLAS CRÍTICAS DE AUDITORÍA (ESTRICLAS):

1. **CONSULTA DE CATÁLOGOS Y NORMALIZACIÓN (PRIORIDAD ALTA)**:
   - Se te proporcionarán listas de "AJUSTADORES CONOCIDOS" y "ASEGURADORAS".
   - **Paso 1**: Extrae el nombre/sigla del documento (Ej: "CTA", "GNP").
   - **Paso 2**: Búscalo en los catálogos proporcionados.
     - **COINCIDENCIA ÚNICA**: Si coincide claramente con UNO solo (Ej: "CTA" -> "Charles Taylor Adjusting"), USA EL NOMBRE DEL CATÁLOGO ("ajustadorNombre").
     - **AMBIGÜEDAD**: Si puede ser varios o no estás seguro:
       - 'ajustadorNombre': Pon el valor ORIGINAL extraído (Ej: "CTA").
       - 'ajustadorAmbiguo': true.
       - 'advertencias': Agrega "Ambigüedad en Ajustador: CTA".
     - **SIN COINCIDENCIA**: Si no está en catálogo, usa el valor original y marca 'ajustadorAmbiguo': false (es nuevo, no ambiguo).

2. **CLASIFICACIÓN ORIGEN vs ACTUALIZACIÓN**:
   - **ACTUALIZACIÓN/VERSIÓN**:
     - Si el ID tiene letra de sufijo (L30A, L30B) -> 'tipoRegistro' es esa letra ("A", "B").
     - Si el ID es numérico (L30) pero dice "Actualización" -> 'esActualizacionExplicita': true.
   - **ORIGEN**: Si el ID es numérico puro (L30) y NO dice Actualización.

3. **FECHAS E IDENTIDAD**:
   - Extract 'fiInicio' y 'fiFin' de la Fecha de Incidencia.
   - 'fiTexto': El texto literal (Ej: "10 al 11 de octubre de 2023").
   - 'comunicadoId': Solo la parte corta (L30, L30A). Elimina prefijos.

4. **FORMATO**:
    - 'totalPdf': Número float puro.

## FORMATO JSON ESPERADO:
{
  "header": {
    "ajustadorCodigo": "string (Si normalizaste)",
    "ajustadorNombre": "string (Normalizado o Original si es ambiguo)",
    "ajustadorAmbiguo": boolean,
    "valorOriginalAjustador": "string (Lo que decía el PDF)",
    "aseguradoraCodigo": "string",
    "aseguradoraNombre": "string",
    "aseguradoraAmbigua": boolean,
    "valorOriginalAseguradora": "string",
    "refCta": "string (Referencia base)",
    "refSiniestro": "string",
    "comunicadoId": "string (Ej: L30A)",
    "esActualizacionExplicita": boolean,
    "fiInicio": "YYYY-MM-DD",
    "fiFin": "YYYY-MM-DD",
    "fiTexto": "string",
    "descripcionSiniestro": "string",
    "fenomenoResumen": "string",
    "estado": "string",
    "distritoRiego": "string",
    "fechaDoc": "YYYY-MM-DD",
    "tipoRegistro": "string (ORIGEN o Letra)",
    "totalPdf": number,
    "advertencias": ["string"]
  },
  "lineas": [
    {
      "concepto": "string",
      "categoria": "string",
      "importe": number
    }
  ]
}
`;

    if (catalogs) {
        promptSystem += `\n\n## CATÁLOGOS VÁLIDOS (Entity Resolution):\n`;
        if (catalogs.distritos && catalogs.distritos.length > 0)
            promptSystem += `- DISTRITOS/MUNICIPIOS VÁLIDOS: ${JSON.stringify(catalogs.distritos.slice(0, 100))}\n`;
        if (catalogs.ajustadores && catalogs.ajustadores.length > 0)
            promptSystem += `- AJUSTADORES CONOCIDOS: ${JSON.stringify(catalogs.ajustadores.slice(0, 50))}\n`;
        if (catalogs.siniestros && catalogs.siniestros.length > 0)
            promptSystem += `- SINIESTROS CONOCIDOS: ${JSON.stringify(catalogs.siniestros.slice(0, 50))}\n`;
        if (catalogs.fenomenos && catalogs.fenomenos.length > 0)
            promptSystem += `- FENÓMENOS REGISTRADOS: ${JSON.stringify(catalogs.fenomenos.slice(0, 100))}\n`;

        promptSystem += `\nUSA ESTAS LISTAS para normalizar tu salid. Prioriza coincidencias existentes.`;
    }

    if (errorFeedback) {
        promptSystem += `\n\n⚠️ ATENCIÓN: TU INTENTO ANTERIOR FALLÓ CON ESTE ERROR: "${errorFeedback}". \nREVISA TUS CÁLCULOS Y EL FORMATO JSON.`;
    }

    // Payload con PDF como inline_data (multimodal)
    const payload = {
        contents: [{
            parts: [
                { text: promptSystem },
                {
                    inline_data: {
                        mime_type: "application/pdf",
                        data: base64Content
                    }
                },
                { text: `Analiza el documento PDF adjunto (${filename}) y extrae los datos según el formato especificado.` }
            ]
        }],
        generationConfig: {
            response_mime_type: "application/json"
        }
    };

    const options = {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
    };

    // Loop de reintentos con backoff exponencial para Rate Limit (429)
    let rateLimitRetries = 0;

    while (rateLimitRetries < MAX_RATE_LIMIT_RETRIES) {
        const response = UrlFetchApp.fetch(url, options);
        const code = response.getResponseCode();
        const text = response.getContentText();

        console.log(`[Gemini API] Response code: ${code}`);

        if (code === 200) {
            // Éxito - Procesar respuesta
            try {
                const respJson = JSON.parse(text);

                // Verificar si hay candidatos válidos
                if (!respJson.candidates || respJson.candidates.length === 0) {
                    console.error('[Gemini API] No hay candidatos en la respuesta:', text);
                    throw new Error('Gemini no devolvió candidatos. Posible contenido bloqueado o error interno.');
                }

                const candidate = respJson.candidates[0];

                // Verificar si el candidato fue bloqueado
                if (candidate.finishReason === 'SAFETY' || candidate.finishReason === 'BLOCKED') {
                    console.error('[Gemini API] Contenido bloqueado:', candidate);
                    throw new Error(`Contenido bloqueado por políticas de seguridad: ${candidate.finishReason}`);
                }

                if (!candidate.content || !candidate.content.parts || !candidate.content.parts[0]) {
                    console.error('[Gemini API] Estructura de respuesta inesperada:', text);
                    throw new Error('Estructura de respuesta Gemini inválida.');
                }

                const rawContent = candidate.content.parts[0].text;
                console.log(`[Gemini API] Respuesta recibida: ${rawContent.substring(0, 200)}...`);

                // Limpieza de Markdown ```json ... ``` si la IA lo incluye
                let cleanJson = rawContent.replace(/```json/g, '').replace(/```/g, '').trim();
                return JSON.parse(cleanJson);

            } catch (parseError) {
                console.error('[Gemini API] Error parseando respuesta:', parseError.message);
                console.error('[Gemini API] Respuesta cruda:', text.substring(0, 500));
                throw new Error(`Error parseando respuesta Gemini: ${parseError.message}`);
            }
        }

        if (code === 429) {
            rateLimitRetries++;
            const waitTime = BASE_429_WAIT_MS * Math.pow(2, rateLimitRetries - 1);
            console.warn(`[Gemini 429] Rate Limit alcanzado (Intento ${rateLimitRetries}/${MAX_RATE_LIMIT_RETRIES}). Esperando ${waitTime / 1000}s...`);
            Utilities.sleep(waitTime);
        } else {
            console.error(`[Gemini API] Error HTTP ${code}:`, text.substring(0, 500));
            throw new Error(`Gemini API Error (${code}): ${text.substring(0, 300)}`);
        }
    }

    throw new Error(`Rate Limit persistente (429). Se agotaron ${MAX_RATE_LIMIT_RETRIES} reintentos.`);
}

/**
 * Realiza la llamada HTTP a la API de Gemini (versión texto, legacy).
 * Implementa backoff exponencial para Rate Limit (429).
 */
function _callGeminiAPI(textContext, filename, errorFeedback = null) {
    const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
    if (!apiKey) throw new Error('GEMINI_API_KEY no configurada en Propiedades del Script.');

    const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${apiKey}`;

    // Construcción del Prompt
    let promptSystem = `
Eres un Auditor de Ingeniería experto. Tu misión es extraer datos de reportes financieros técnicos ("Charles Taylor Adjusting") y devolver UNICAMENTE un JSON válido.

## REGLAS CRÍTICAS DE AUDITORÍA:

1. **LIMPIEZA NUMÉRICA**: 
   - Campos 'totalPdf' e 'importe' DEBEN ser números puros (float). 
   - Elimina '$', ',', textos, y espacios. Ej: "$ 1,500.00" -> 1500.00
   
2. **IMPORTES CERO (CANCELACIONES)**:
   - Si el documento indica "CANCELADO", "SIN IMPORTE", o el total es $0.00:
   - 'totalPdf' debe ser 0.
   - 'lineas' debe ser un array vacío [].
   - NO inventes líneas.

3. **LINAJE Y VERSIONES (CRÍTICO)**:
   - Identifica el 'comunicadoId' (Ej: L15, L15A).
   - Identifica el 'tipoRegistro':
     - Si no tiene letra al final (L15, L103) -> "ORIGEN".
     - Si tiene letra (L15A, L103C) -> Escribe la letra tal cual (Ej: "A", "C").
   - **DESCRIPCION HISTÓRICA**: Debes construir el historial acumulativo en el campo 'descripcion'.
     - Analiza el texto para encontrar referencias a versiones anteriores.
     - Formato: "{REF_CTA}-{COMUNICADO_RAIZ}, {VERSION_ANTERIOR}, {VERSION_ACTUAL}"
     - Ejemplo L30C: "GL097115-L30, L30A, L30B, L30C"
     - IMPORTANTE: Diferencia por contexto. Si L30A es de un municipio diferente a L30B, NO son familia. Solo enlaza los que comparten Ubicación/Siniestro.

4. **CONTEXTO DE CATÁLOGOS**:
   - 'distritoRiego': Extrae el Distrito o Municipio. Si dice "DTT 020" o "Margaritas", normalizalo.
   - 'siniestro': Busca códigos como "SCNA-xxxx".
   - 'ajustador': Simpre es "CHARLES TAYLOR ADJUSTING" (o lo que diga el doc).

## FORMATO JSON ESPERADO:
{
  "header": {
    "refCta": "string (REFERENCIA)",
    "comunicadoId": "string (Ej: L15A)",
    "descripcion": "string (HISTORIAL ACUMULATIVO)",
    "tipoRegistro": "string (ORIGEN o Letra A/B/C)",
    "fechaDoc": "string (YYYY-MM-DD)",
    "estado": "string (UPPERCASE)",
    "refSiniestro": "string (SCNA...)",
    "aseguradora": "string (AGROASEMEX)",
    "fenomeno": "string",
    "fechaSiniestroFi": "string",
    "distritoRiego": "string",
    "ajustador": "string",
    "totalPdf": number
  },
  "lineas": [
    {
      "concepto": "string (Ubicación/Tramo - NO 'Varios')",
      "categoria": "string (DAÑO FISICO o DESAZOLVES)",
      "importe": number
    }
  ]
}
`;

    if (errorFeedback) {
        promptSystem += `\n\n⚠️ ATENCIÓN: TU INTENTO ANTERIOR FALLÓ CON ESTE ERROR: "${errorFeedback}". \nREVISA TUS CÁLCULOS Y EL FORMATO JSON.`;
    }

    const payload = {
        contents: [{
            parts: [
                { text: promptSystem },
                { text: `--- INICIO DOCUMENTO (${filename}) ---\n${textContext}\n--- FIN DOCUMENTO ---` }
            ]
        }],
        generationConfig: {
            response_mime_type: "application/json"
        }
    };

    const options = {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
    };

    // Loop de reintentos con backoff exponencial para Rate Limit (429)
    let rateLimitRetries = 0;
    let lastResponse = null;

    while (rateLimitRetries < MAX_RATE_LIMIT_RETRIES) {
        const response = UrlFetchApp.fetch(url, options);
        const code = response.getResponseCode();
        const text = response.getContentText();
        lastResponse = { code, text };

        if (code === 200) {
            // Éxito - Procesar respuesta
            const respJson = JSON.parse(text);
            const rawContent = respJson.candidates[0].content.parts[0].text;

            // Limpieza de Markdown ```json ... ``` si la IA lo incluye
            let cleanJson = rawContent.replace(/```json/g, '').replace(/```/g, '').trim();
            return JSON.parse(cleanJson);
        }

        if (code === 429) {
            rateLimitRetries++;
            // Backoff exponencial: 15s, 30s, 60s, 120s, 240s
            const waitTime = BASE_429_WAIT_MS * Math.pow(2, rateLimitRetries - 1);
            console.warn(`[Gemini 429] Rate Limit alcanzado (Intento ${rateLimitRetries}/${MAX_RATE_LIMIT_RETRIES}). Esperando ${waitTime / 1000}s...`);
            Utilities.sleep(waitTime);
            // Continúa al siguiente intento del loop
        } else {
            // Otro error HTTP - No reintentar
            throw new Error(`Gemini API Error (${code}): ${text}`);
        }
    }

    // Si agotamos todos los reintentos de Rate Limit
    throw new Error(`Rate Limit persistente (429). Se agotaron ${MAX_RATE_LIMIT_RETRIES} reintentos tras esperar tiempo acumulado. Intenta de nuevo más tarde.`);
}

/**
 * TRUCO DRIVE: Sube PDF -> Convierte a GDoc -> Extrae Texto -> Borra.
 */
function _extractTextFromPdf(base64Content, filename) {
    const blob = Utilities.newBlob(Utilities.base64Decode(base64Content), 'application/pdf', filename);

    // 1. Subir a Drive con bandera ocr=true es deprecated en API v3 simple, 
    // pero insert() con convert: true funciona para PDFs si son texto seleccionable o OCR básico.
    // Nota: Para OCR puro de imagen, Drive API REST es mejor, pero Apps Script Drive.Files.insert 
    // lo maneja si se habilita la opción de convertir.

    const resource = {
        title: `TEMP_OCR_${new Date().getTime()}_${filename}`,
        mimeType: MimeType.GOOGLE_DOCS
    };

    // Detectar Versión de API (v2 vs v3)
    let file;
    try {
        // Opción A: Drive API v2 (Legacy standard for this hack)
        if (Drive.Files.insert) {
            file = Drive.Files.insert(resource, blob, { ocr: true, ocrLanguage: 'es' });
        }
        // Opción B: Drive API v3 (Modern default)
        else if (Drive.Files.create) {
            // En v3, la conversión es automática si el mimeType es Google Docs
            file = Drive.Files.create(resource, blob);
        } else {
            throw new Error('Método de creación de archivos Drive no encontrado (¿Api activa?).');
        }
    } catch (e) {
        console.warn('Drive OCR falló, detalle:', e.message);
        throw new Error(`Error Drive API: ${e.message}. (Verifica que el servicio "Drive API" esté habilitado en v2 o v3)`);
    }

    // 2. Leer Texto
    // Nota: file.id funciona en ambas versiones
    const doc = DocumentApp.openById(file.id);
    const text = doc.getBody().getText();

    // 3. Limpieza
    try {
        Drive.Files.remove(file.id);
    } catch (e) { console.warn('No se pudo borrar archivo temp:', e.message); }

    return text;
}
