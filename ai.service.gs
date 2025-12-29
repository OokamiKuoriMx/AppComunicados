/**
 * ============================================================================
 * SERVICIO DE INTELIGENCIA ARTIFICIAL (Gemini 1.5 Flash)
 * Descripción: Procesa archivos PDF usando OCR y LLM para extracción estructurada.
 * ============================================================================
 */

// Configuración
const GEMINI_MODEL = 'gemini-1.5-flash';
const MAX_RETRIES = 3;

/**
 * Procesa un PDF en Base64 y devuelve los datos estructurados.
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
    console.log(`[${contexto}] Iniciando procesamiento IA...`);

    try {
        // 1. Convertir PDF a Texto (OCR via Drive)
        const textContext = _extractTextFromPdf(base64Content, filename);
        if (!textContext || textContext.length < 50) {
            throw new Error('No se pudo extraer texto del PDF o está vacío/ilegible.');
        }
        console.log(`[${contexto}] Texto extraído: ${textContext.length} caracteres.`);

        // 2. Loop de Intentos con Auto-Corrección
        let intentos = 0;
        let lastError = null;
        let resultadoFinal = null;

        while (intentos < MAX_RETRIES) {
            intentos++;
            console.log(`[${contexto}] Intento AI #${intentos}...`);

            try {
                // Llamada API
                const jsonResponse = _callGeminiAPI(textContext, filename, lastError);

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

        // 3. Estructurar para el Importador
        // Asegurar formato compatible con importarUnico
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
 * Realiza la llamada HTTP a la API de Gemini.
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

    const response = UrlFetchApp.fetch(url, options);
    const code = response.getResponseCode();
    const text = response.getContentText();

    if (code !== 200) {
        throw new Error(`Gemini API Error (${code}): ${text}`);
    }

    const respJson = JSON.parse(text);
    const rawContent = respJson.candidates[0].content.parts[0].text;

    // Limpieza de Markdown ```json ... ``` si la IA lo incluye
    let cleanJson = rawContent.replace(/```json/g, '').replace(/```/g, '').trim();
    return JSON.parse(cleanJson);
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

    // Habilitar Drive API en Servicios Avanzados es requisito.
    let file;
    try {
        file = Drive.Files.insert(resource, blob, { ocr: true, ocrLanguage: 'es' });
    } catch (e) {
        console.warn('Drive OCR falló, intentando como texto plano si es PDF nativo...');
        // Fallback: Si falla OCR (ej. límite de tamaño), intentar extraer texto con pdf-parse (no nativo)
        // O simplemente fallar. Para este caso, asumimos Drive API activado.
        throw new Error(`Error Drive API: ${e.message}. Asegúrate de activar el servicio Drive API.`);
    }

    // 2. Leer Texto
    const doc = DocumentApp.openById(file.id);
    const text = doc.getBody().getText();

    // 3. Limpieza
    try {
        Drive.Files.remove(file.id);
    } catch (e) { console.warn('No se pudo borrar archivo temp:', e.message); }

    return text;
}
