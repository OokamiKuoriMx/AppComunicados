/**
 * ============================================================================
 * MÓDULO: IMPORTACIÓN INTELIGENTE (Versión Batch - High Performance)
 * Descripción: Procesa archivos planos (CSV) para generar estructura relacional.
 * Optimizado para leer una vez y escribir en lotes ordenados.
 * ============================================================================
 */

/**
 * API PÚBLICA: Previsualizar Importación (Solo Lectura)
 * Devuelve los datos parseados y validados para que el usuario confirme.
 */
function previsualizarImportacion(fileContent) {
    const contexto = 'previsualizarImportacion';
    console.log(`[${contexto}] Iniciando previsualización...`);

    try {
        // 1. REUTILIZAR PARSER Y CACHE
        const loteAgrupado = parseImportFile(fileContent);
        const cache = _loadCatalogsCache();

        // 2. SIMULAR VALIDACIÓN
        // 2. SIMULAR VALIDACIÓN
        validarLote(loteAgrupado, cache);

        // ORDENAMIENTO INTELIGENTE (Smart Batch Ordering) - Replicado para consistencia visual
        loteAgrupado.sort((a, b) => {
            const refA = String(a.header.refCta || '').trim();
            const refB = String(b.header.refCta || '').trim();
            if (refA !== refB) return refA.localeCompare(refB);
            const vA = _parseVersion(a.header.comunicadoId);
            const vB = _parseVersion(b.header.comunicadoId);
            if (vA.base !== vB.base) return vA.base.localeCompare(vB.base);
            if (vA.index !== vB.index) return vA.index - vB.index;
            const dateA = a.header.fechaDoc ? new Date(a.header.fechaDoc).getTime() : 0;
            const dateB = b.header.fechaDoc ? new Date(b.header.fechaDoc).getTime() : 0;
            return dateA - dateB;
        });

        // 3. ENRIQUECER PARA VISTA PREVIA
        // Pasamos 'loteAgrupado' completo como contexto para resolver padres en el mismo lote
        const previewData = loteAgrupado.map(doc => _analizarDocumento(doc, cache, loteAgrupado));

        const resumen = {
            total: previewData.length,
            validos: previewData.filter(d => d.esValido && d.status !== 'OMITIDO').length,
            omitidos: previewData.filter(d => d.esValido && d.status === 'OMITIDO').length,
            errores: previewData.filter(d => !d.esValido).length
        };

        return {
            success: true,
            data: {
                resumen: resumen,
                filas: previewData
            }
        };

    } catch (error) {
        console.error(`Error en ${contexto}:`, error);
        const msg = (error instanceof Error) ? error.message : String(error);
        return { success: false, message: msg || 'Error desconocido en previsualización' };
    }
}

/**
 * API PÚBLICA: Analiza un payload JSON (salida de IA) para generar el objeto de previsualización.
 */
function analizarExtraccionIA(payload) {
    const contexto = 'analizarExtraccionIA';
    console.log(`[${contexto}] Inicio. Payload recibido:`, JSON.stringify(payload).substring(0, 500));

    try {
        if (!payload || !payload.header) {
            console.error(`[${contexto}] Payload inválido - no tiene header`);
            throw new Error('Payload inválido: falta header');
        }

        console.log(`[${contexto}] Header: refCta=${payload.header.refCta}, comunicado=${payload.header.comunicadoId}`);

        const cache = _loadCatalogsCache();
        console.log(`[${contexto}] Cache cargado. Cuentas: ${cache.cuentas.length}, Comunicados: ${cache.comunicados.length}`);

        // Simular estructura de documento interno
        const doc = {
            header: payload.header,
            lineas: payload.lineas || [],
            validacion: { esValido: true, status: 'OK' }
        };

        const analisisRow = _analizarDocumento(doc, cache);
        console.log(`[${contexto}] Análisis completado:`, JSON.stringify(analisisRow).substring(0, 300));

        return { success: true, data: analisisRow };

    } catch (e) {
        console.error(`[${contexto}] ERROR:`, e.message, e.stack);
        return { success: false, message: e.message || 'Error desconocido en análisis' };
    }
}

/**
 * Lógica central de análisis y comparativa con BD.
 */
function _analizarDocumento(doc, cache, batchDocs = []) {
    const h = doc.header;
    const v = doc.validacion;
    const esValido = v.esValido && v.status !== 'OMITIDO';

    // SANITIZACIÓN CRÍTICA DEL ID
    // Asegurar que trabajamos con "L30A" y no "GL070059-L30A" para que los Match de BD funcionen
    if (h.comunicadoId && h.comunicadoId.includes('-')) {
        const parts = h.comunicadoId.split('-');
        const cleanId = parts[parts.length - 1].trim();
        console.log(`[Import] Sanitizando ID: ${h.comunicadoId} -> ${cleanId}`);
        h.comunicadoId = cleanId;
    }

    // Detección de "Nuevos" Catálogos
    let statusAjustador = _checkStatus(cache.ajustadores, ['nombreAjustador', 'nombre'], (h.ajustadorNombre || h.ajustador));
    if (h.ajustadorAmbiguo) {
        statusAjustador = { status: 'AMBIGUO', valor: h.valorOriginalAjustador || h.ajustadorNombre, advertencia: 'Ambigüedad detectada por IA' };
    }

    let statusAseguradora = _checkStatus(cache.aseguradoras, 'descripción', (h.aseguradoraNombre || h.aseguradora));
    if (h.aseguradoraAmbigua) {
        statusAseguradora = { status: 'AMBIGUO', valor: h.valorOriginalAseguradora || h.aseguradoraNombre, advertencia: 'Ambigüedad detectada por IA' };
    }

    const analisis = {
        cuenta: _checkStatus(cache.cuentas, ['referencia', 'cuenta'], h.refCta),
        siniestro: _checkStatus(cache.siniestros, 'siniestro', h.refSiniestro),
        ajustador: statusAjustador,
        distrito: _checkStatus(cache.distritosRiego, 'distritoRiego', h.distritoRiego),
        aseguradora: statusAseguradora,
        advertencias: h.advertencias || []
    };

    // Enhanced Comunicado Analysis
    let statusCom = 'NUEVO';
    let changes = [];
    let existingCom = null;
    let dgActual = null;
    let resEstadoId = null;

    // 1. Find Account ID
    const refClean = String(h.refCta || '').trim().toUpperCase();
    const cta = cache.cuentas.find(c =>
        String(c.referencia).toUpperCase().trim() === refClean ||
        String(c.cuenta).toUpperCase().trim() === refClean
    );
    if (cta) {

        // =================================================================================
        // NUEVA LÓGICA: CONTROL DE VERSIONES Y AMBIGÜEDAD (STRICT ORIGEN)
        // =================================================================================

        // 1. Detección de Ambigüedad (L30A sin "Actualización")
        // La IA ahora devuelve 'tipoRegistro' = 'ORIGEN' si no vio la palabra, aunque sea L30A.
        // Pero debemos advertir al usuario si parece una versión.
        const vParsed = _parseVersion(h.comunicadoId);
        let esAmbiguo = false;
        let advertencia = null;

        if (vParsed.sufijo && h.tipoRegistro === 'ORIGEN') {
            // Caso: Tiene letra (L30A) pero NO dice "Actualización".
            // CAMBIO (2025-12-30): Si tiene sufijo, intentamos buscar padre de todos modos.
            // Confiamos en que la estructura del ID (L30A) denota versión más que el texto OCR.
            console.log(`[Import] Sufijo '${vParsed.sufijo}' detectado en ORIGEN. Se intentará vincular como versión.`);

            // Ya no es ambiguo bloqueante, permitimos flujo hacia búsqueda de padres
            esAmbiguo = false;
        }

        // A) Validar si es una versión obsoleta (Ej: Subir L30 cuando ya existe L30A)
        // Solo aplica si NO es ambiguo y realmente estbuscando actualizar familia
        if (!esAmbiguo) {
            const checkObsoleto = _validarVersionObsoleta(cache, cta.id, h.comunicadoId);
            if (checkObsoleto.esObsoleto) {
                return {
                    ref: h.refCta,
                    comunicado: h.comunicadoId,
                    tipo: h.tipoRegistro,
                    fecha: h.fechaDoc ? new Date(h.fechaDoc).toISOString().split('T')[0] : '',
                    monto: h.totalPdf,
                    sumaLineas: v.sumaLineas,
                    status: 'ERROR',
                    esValido: false, // Invalido porque es viejo
                    motivo: checkObsoleto.mensaje,
                    analisis: {
                        comunicado: { status: 'OBSOLETO', valor: h.comunicadoId, debug: { msg: checkObsoleto.mensaje } }
                    },
                    rawPayload: { header: h, lineas: releaseEvents.lineas || [] }
                };
            }
        }

        // B) Generar Descripción Histórica Automática
        // Solo si NO es ambiguo (si es ambiguo, es origen nuevo, inicia su propia historia)
        if (!esAmbiguo) {
            const nuevaDescripcion = _construirHistorial(cache, cta, h.comunicadoId);
            if (nuevaDescripcion) {
                h.descripcion = nuevaDescripcion;
            }
        }

        // C) Validación Estricta de Textos para ORIGEN (Requerimiento)
        if (h.tipoRegistro === 'ORIGEN') {
            // Validar Referencia Exacta (Si tenemos forma de saber la esperada... 
            // Buscar si el comunicado ya existe
            // USO DE CLEAN ID
            const _cleanId = (val) => String(val || '').toUpperCase().replace(/\s+/g, '').trim();

            let existingCom = cache.comunicados.find(c =>
                String(c.idReferencia) === String(cta.id) &&
                _cleanId(c.comunicado) === _cleanId(doc.header.comunicadoId)
            );

            if (existingCom) {
                // YA EXISTE EXACTO -> Revisar si hay cambios (Update) o es igual (Omitir)
                // Por defecto a actualizar para que entre a validación de cambios
                docsParaActualizar.push(doc);
                doc._existingId = existingCom.id; // Marcar ID real
            } else {
                // NO EXISTE EXACTO -> Es Nuevo (o Nueva Versión)
                // Siempre CREAR nuevo registro para L30A, L30B, etc. No sobreescribir L30.
                docsParaCrear.push(doc);

                // Opcional: Podríamos marcar '_parentVersionId' si quisieramos enlazar, 
                // pero por ahora el requerimiento es que sea un Nuevo Comunicado con status visual correcto.
            }
        }

        // 2. Check strict existence in DB
        // CRÍTICO: Validamos AMBOS: Que pertenezca a la cuenta correcta (Referencia) Y sea el ID exacto.
        // Uso de _cleanId para comparación robusta - Definido una sola vez arriba o renombrado si colapsa
        // Ya definimos _cleanId dentro del bloque anterior? No, parece que fue un error de copy-paste en la herramienta.
        // Vamos a definirlo con var o let único fuera de ifs.

        const _cleanIdFunc = (val) => String(val || '').toUpperCase().replace(/\s+/g, '').trim();

        existingCom = cache.comunicados.find(c =>
            String(c.idReferencia) === String(cta.id) &&
            _cleanIdFunc(c.comunicado) === _cleanIdFunc(h.comunicadoId)
        );

        // 2.1 LOGICA DE MATCHING / BUSQUEDA DE PADRES (Solo si no es Ambiguo)
        if (!existingCom && !esAmbiguo) {
            const version = _parseVersion(h.comunicadoId);

            // Solo si tiene versión (L30 -> L30A)
            if (version.sufijo) {
                // A) Buscar en DB (Prioridad 1)
                existingCom = cache.comunicados.find(c => {
                    if (String(c.idReferencia) !== String(cta.id)) return false;
                    const v = _parseVersion(c.comunicado);
                    // Misma base (L30) y que sea índice MENOR (L30 < L30A)
                    return v.base === version.base && v.index < version.index;
                });

                if (existingCom) {
                    console.log(`[Import] Detectada Actualización de Versión (DB): ${existingCom.comunicado} -> ${h.comunicadoId}`);
                }
                // B) Buscar en LOTE ACTUAL (Prioridad 2 - Para Vista Previa)
                else if (batchDocs && batchDocs.length > 0) {
                    const parentInBatch = batchDocs.find(d => {
                        const dHeader = d.header;
                        const dRefClean = String(dHeader.refCta || '').trim().toUpperCase();
                        if (dRefClean !== refClean) return false;

                        const vB = _parseVersion(dHeader.comunicadoId);
                        return vB.base === version.base && vB.index < version.index;
                    });

                    if (parentInBatch) {
                        console.log(`[Import] Detectada Actualización en LOTE: ${parentInBatch.header.comunicadoId} -> ${h.comunicadoId}`);
                        statusCom = 'ACTUALIZACION'; // Modificado para badge azul
                        existingCom = {
                            id: 'PENDIENTE',
                            comunicado: parentInBatch.header.comunicadoId,
                            simulado: true
                        };
                    }
                }
            }
        }

        // 3. NUEVO: Verificación de Contenido Duplicado (Líneas)
        // Si no es el mismo ID, pero tiene las mismas líneas/monto -> ALERTA DE DUPLICADO
        let posibleDuplicadoId = null;
        if (!existingCom && statusCom !== 'ACTUALIZACION') {
            // Buscar en DB records con el Mismo Monto y Mismo Numero de Lineas (heurística rápida)
            const totalPayload = parseFloat(h.totalPdf || 0);
            const lineasPayload = doc.lineas ? doc.lineas.length : 0;

            // Iterar comunicados de la cuenta
            const candidatos = cache.comunicados.filter(c => String(c.idReferencia) === String(cta.id));

            for (const c of candidatos) {
                // Necesitamos Datos Generales para el monto? O Lineas?
                // Cache.datosGenerales tiene monto? No, comunicados tiene 'monto' usualmente?
                // En DB, 'comunicados' tiene id, referencia, fecha... 'total'?
                // Vamos a cache.datosGenerales que tiene totales.
                const dg = cache.datosGenerales.find(d => String(d.idComunicado) === String(c.id));
                if (dg) {
                    // Comparar Monto (con tolerancia pequeña)
                    if (Math.abs(dg.total - totalPayload) < 0.1) {
                        // MISMO MONTO. Podría ser duplicado.
                        // Si tuvieramos lineas en cache sería ideal, pero es caro.
                        // Asumimos advertencia por monto idéntico.
                        posibleDuplicadoId = c.comunicado;
                        break;
                    }
                }
            }
        }

        if (existingCom) {
            // 3. Deep Compare for Smart Update
            dgActual = cache.datosGenerales.find(dg => String(dg.idComunicado) === String(existingCom.id));

            if (!dgActual) {
                statusCom = 'REEMPLAZAR'; // Existe Com pero no DG (Raro, pero forzamos update/insert DG)
            } else {
                // Comparacion profunda de campos clave
                let hasChanges = false;

                // Descripcion (Ahora comparamos contra la generada automáticamente)
                if (h.descripcion && normalizarTexto(h.descripcion) !== normalizarTexto(dgActual.descripcion)) {
                    hasChanges = true;
                    changes.push('Descripción (Autogenerada)');
                }

                // Fecha
                if (h.fechaDoc) {
                    const dateCSV = new Date(h.fechaDoc).toISOString().split('T')[0];
                    const dateDB = dgActual.fecha ? new Date(dgActual.fecha).toISOString().split('T')[0] : '';
                    if (dateCSV !== dateDB) {
                        hasChanges = true;
                        changes.push(`Fecha (${dateDB} -> ${dateCSV})`);
                    }
                }

                // Edo
                resEstadoId = _resolveIdFromCache(cache.estados, h.estado, 'estado');
                if (resEstadoId) {
                    if (String(resEstadoId) !== String(dgActual.idEstado)) {
                        hasChanges = true;
                        // Intentar obtener nombre anterior
                        const edoAnt = cache.estados.find(e => String(e.id) === String(dgActual.idEstado));
                        changes.push(`Estado (${edoAnt ? edoAnt.estado : '??'} -> ${h.estado})`);
                    }
                } else if (h.estado) {
                    hasChanges = true;
                    changes.push(`AVISO: Estado '${h.estado}' no encontrado`);
                }


                // Distrito
                const idDRNuevo = _resolveIdFromCache(cache.distritosRiego, h.distritoRiego, 'distritoRiego');
                if (idDRNuevo) {
                    if (String(idDRNuevo) !== String(dgActual.idDR)) {
                        hasChanges = true;
                        changes.push('Distrito');
                    }
                } else if (h.distritoRiego) {
                    hasChanges = true;
                    changes.push(`AVISO: Distrito '${h.distritoRiego}' no encontrado`);
                }

                // Siniestro
                const idSiniestroNuevo = _resolveIdFromCache(cache.siniestros, h.refSiniestro, 'siniestro');
                if (idSiniestroNuevo) {
                    if (String(idSiniestroNuevo) !== String(dgActual.idSiniestro)) {
                        hasChanges = true;
                        changes.push('Siniestro');
                    }
                } else if (h.refSiniestro) {
                    hasChanges = true;
                    changes.push(`AVISO: Siniestro '${h.refSiniestro}' no encontrado`);
                }

                if (existingCom.simulado) {
                    statusCom = 'ACTUALIZACION'; // Force update status for simulated/batch parents
                } else {
                    statusCom = hasChanges ? 'REEMPLAZAR' : 'OMITIDO';
                }
            }
        } else {
            // NO EXISTE EN DB (Ni padre ni exacto) -> Es NUEVO o ACTUALIZACION NUEVA
            if (h.esActualizacionExplicita || (h.tipoRegistro && h.tipoRegistro.length === 1 && h.tipoRegistro !== 'ORIGEN')) {
                // Es una actualización explicita (ej L30A) pero no encontramos L30.
                // Se trata como NUEVO registro, pero con status visual diferenciado.
                statusCom = 'ACTUALIZACION';
            } else if (posibleDuplicadoId) {
                // Nuevo pero con contenido duplicado
                statusCom = 'DUPLICADO_CONTENIDO';
                advertencia = `Posible duplicado de ${posibleDuplicadoId} (Mismo Monto)`;
            }
        }
    }

    // DEBUG: Inject diagnostics
    const debugInfo = {
        csvEstado: h.estado,
        duplicadoDe: posibleDuplicadoId || null,
        resId: resEstadoId,
        dbId: (existingCom && cache.datosGenerales.find(dg => String(dg.idComunicado) === String(existingCom.id))) ?
            cache.datosGenerales.find(dg => String(dg.idComunicado) === String(existingCom.id)).idEstado : '?'
    };

    analisis.comunicado = { status: statusCom, valor: h.comunicadoId, cambios: changes, debug: debugInfo };

    // Determine Global Status based on Analysis
    let finalStatus = v.status;
    let finalMotivo = v.motivo || (v.errores ? v.errores.join(', ') : '');

    if (esValido) {
        const isComOmitido = statusCom === 'OMITIDO';
        const isCtaNueva = analisis.cuenta && analisis.cuenta.status === 'NUEVO';
        const isSinNuevo = analisis.siniestro && analisis.siniestro.status === 'NUEVO';
        const isDrNuevo = analisis.distrito && analisis.distrito.status === 'NUEVO';

        if (isComOmitido && !isCtaNueva && !isSinNuevo && !isDrNuevo) {
            finalStatus = 'OMITIDO';
            finalMotivo = 'Registro idéntico a Base de Datos.';
        }
        if (statusCom === 'DUPLICADO_CONTENIDO') {
            // No bloqueamos, pero advertimos
            // finalStatus = 'ALERT'; 
            finalMotivo = `Posible duplicado de ${posibleDuplicadoId}`;
        }
    }

    return {
        ref: h.refCta,
        comunicado: h.comunicadoId,
        tipo: h.tipoRegistro,
        fecha: h.fechaDoc ? new Date(h.fechaDoc).toISOString().split('T')[0] : '',
        monto: h.totalPdf,
        sumaLineas: v.sumaLineas,
        status: finalStatus,

        esValido: esValido, // Keep valid so it counts as "processable" but omitted
        motivo: finalMotivo,
        analisis: analisis,
        rawPayload: { header: h, lineas: doc.lineas } // Data for single import

    }
}

/**
 * Parsea un ID de comunicado para extraer su base y versión.
 * Ej: "L30" -> { base: "L30", sufijo: "", index: 0 }
 * Ej: "L30A" -> { base: "L30", sufijo: "A", index: 1 }
 * Ej: "L30C" -> { base: "L30", sufijo: "C", index: 3 }
 */
function _parseVersion(comunicadoId) {
    if (!comunicadoId) return { base: '', sufijo: '', index: 0 };

    // Limpieza previa: Si viene con guiones (GL070059-L30A), tomar la última parte.
    let shortId = comunicadoId;
    if (comunicadoId.includes('-')) {
        const parts = comunicadoId.split('-');
        shortId = parts[parts.length - 1]; // Tomar "L30A" de "GL...-L30A"
    }
    shortId = shortId.trim();

    // Regex: (Todo lo que no sea la ultima letra si es mayuscula) + (Letra opcional)
    // Asumiendo formato estricto L(\d+)([A-Z]?)
    const match = shortId.match(/^(L\d+)([A-Z])?$/);

    if (match) {
        const base = match[1]; // L30
        const sufijo = match[2] || ''; // A, B, o nada

        let index = 0;
        if (sufijo) {
            // A=1, B=2, C=3...
            index = sufijo.charCodeAt(0) - 64;
        }

        return { base, sufijo, index };
    }

    // Si no cumple patrón Lxx, retornar como base pura
    return { base: shortId, sufijo: '', index: 0 };
}

/**
 * Valida si el comunicado entrante es una versión anterior a lo que ya existe.
 */
function _validarVersionObsoleta(cache, idReferencia, newComunicadoId) {
    if (!newComunicadoId) return { esObsoleto: false };

    const nueva = _parseVersion(newComunicadoId);

    // Filtrar comunicados de la misma cuenta y misma BASE (L30 vs L30A vs L30B)
    const hermanos = cache.comunicados.filter(c => {
        if (String(c.idReferencia) !== String(idReferencia)) return false;
        const v = _parseVersion(c.comunicado);
        return v.base === nueva.base;
    });

    if (hermanos.length === 0) return { esObsoleto: false };

    // Encontrar la versión máxima existente
    let maxVersion = -1;
    let maxCom = '';

    hermanos.forEach(h => {
        const v = _parseVersion(h.comunicado);
        if (v.index > maxVersion) {
            maxVersion = v.index;
            maxCom = h.comunicado;
        }
    });

    // Comparar
    // Si la nueva (ej: A=1) es MENOR que la máxima (ej: B=2) -> OBSOLETO
    if (nueva.index < maxVersion) {
        return {
            esObsoleto: true,
            mensaje: `Versión OBSOLETA. Ya existe una versión más reciente (${maxCom}) para este comunicado.`
        };
    }

    return { esObsoleto: false };
}

/**
 * Construye la descripción histórica: "Ref - Old1, Old2, New"
 */
function _construirHistorial(cache, cta, newComunicadoId) {
    if (!cta || !newComunicadoId) return null;

    const nueva = _parseVersion(newComunicadoId);
    let historial = new Set();

    // 1. Buscar en BD si ya existe algún hermano o el mismo registro (para sacar su historial previo)
    // Filtramos por cuenta y FAMILIA base (L30)
    const hermanos = cache.comunicados.filter(c => {
        if (String(c.idReferencia) !== String(cta.id)) return false;
        const v = _parseVersion(c.comunicado);
        return v.base === nueva.base;
    });

    // 2. Extraer historial de la descripción actual de esos hermanos
    // (Normalmente solo habrá 1 hermano si aplicamos la lógica de Unique Record)
    hermanos.forEach(h => {
        // Añadir el nombre actual del registro (ej: L30A)
        historial.add(h.comunicado);

        // Buscar su DatosGenerales para leer la descripción histórica (ej: "Ref - L30, L30A")
        const dg = cache.datosGenerales.find(d => String(d.idComunicado) === String(h.id));
        if (dg && dg.descripcion) {
            // Parsear descripción: "GL070059-L30, L30A" -> ["L30", "L30A"]
            // Asumimos formato: TEXTO - v1, v2, v3
            const partes = dg.descripcion.split('-');
            if (partes.length > 1) {
                const versionesStr = partes[1].trim(); // "L30, L30A"
                const versiones = versionesStr.split(',').map(s => s.trim());
                versiones.forEach(v => {
                    // Solo añadir si parece una versión válida de la misma familia
                    const vP = _parseVersion(v);
                    if (vP.base === nueva.base) {
                        historial.add(v);
                    }
                });
            }
        }
    });

    // 3. Añadir el nuevo
    historial.add(newComunicadoId);

    // 4. Convertir a array y ordenar
    const historialArr = Array.from(historial);
    historialArr.sort((a, b) => {
        const vA = _parseVersion(a);
        const vB = _parseVersion(b);
        return vA.index - vB.index;
    });

    const historialStr = historialArr.join(', ');
    return `${cta.referencia}-${historialStr}`;
}



/**
 * Helper para verificar si un valor existe en cache o será nuevo
 */
function _checkStatus(list, fields, value) {
    if (!value) return { status: 'VACIO', valor: '' };
    const cleanVal = String(value).toUpperCase().trim();

    // Check exist
    const exists = list.some(item => {
        if (Array.isArray(fields)) {
            return fields.some(f => String(item[f] || '').toUpperCase().trim() === cleanVal);
        }
        return String(item[fields] || '').toUpperCase().trim() === cleanVal;
    });

    return {
        status: exists ? 'EXISTE' : 'NUEVO',
        valor: value
    };
}

/**
 * PASO 7 (BACKEND): CONTROLADOR PRINCIPAL
 * Orquesta la importación completa usando Persistencia Batch.
 * @param {string} fileContent - Contenido de texto del archivo CSV.
 */
function importarUnico(payload) {
    const context = 'importarUnico';
    try {
        console.log(`[${context}] Raw payload received type: ${typeof payload}`);

        // Robust Parsing: If payload is stringified (to avoid GAS recursive copy issues), parse it.
        if (typeof payload === 'string') {
            try {
                payload = JSON.parse(payload);
            } catch (jsonErr) {
                console.warn(`[${context}] Failed to parse string payload:`, jsonErr);
                // Continue, maybe it was just a string? But likely invalid.
            }
        }

        // Debug
        // console.log(`[${context}] Parsed Payload:`, JSON.stringify(payload)); 

        if (!payload || !payload.header || !payload.lineas) {
            const keys = payload ? JSON.stringify(Object.keys(payload)) : 'null';
            const type = typeof payload;
            console.error(`[${context}] Invalid payload structure. Type: ${type}, Keys: ${keys}`);
            throw new Error(`Payload inválido. Recibido: ${type}, Llaves: ${keys}`);
        }

        const item = { header: payload.header, lineas: payload.lineas, validacion: { esValido: true, status: 'OK' } };
        const loteAgrupado = [item];
        const cache = _loadCatalogsCache();
        const result = _procesarBatchInterno(loteAgrupado, cache);
        console.log(`[${context}] Resultado Batch:`, JSON.stringify(result));
        return result;
    } catch (e) {
        console.error(e);
        return { success: false, message: e.message + (e.stack ? ' | ' + e.stack : '') };
    }
}

function ejecutarImportacion(fileContent) {
    const contexto = 'ejecutarImportacion';
    console.log(`[${contexto}] Iniciando procesamiento Batch...`);

    try {
        const loteAgrupado = parseImportFile(fileContent);
        const cache = _loadCatalogsCache();
        validarLote(loteAgrupado, cache);

        // ORDENAMIENTO INTELIGENTE (Smart Batch Ordering)
        // Asegurar que procesamos L30 -> L30A -> L30B para que la historia se construya correctamente
        loteAgrupado.sort((a, b) => {
            // 1. Por Referencia (Cuenta)
            const refA = String(a.header.refCta || '').trim();
            const refB = String(b.header.refCta || '').trim();
            if (refA !== refB) return refA.localeCompare(refB);

            // 2. Por Familia (Base del Comunicado, ej: L30)
            const vA = _parseVersion(a.header.comunicadoId);
            const vB = _parseVersion(b.header.comunicadoId);
            if (vA.base !== vB.base) return vA.base.localeCompare(vB.base);

            // 3. Por Índice de Versión (Origen=0, A=1, B=2...)
            if (vA.index !== vB.index) return vA.index - vB.index;

            // 4. Por Fecha Documento (Desempate final)
            // Priorizar más antiguos primero
            const dateA = a.header.fechaDoc ? new Date(a.header.fechaDoc).getTime() : 0;
            const dateB = b.header.fechaDoc ? new Date(b.header.fechaDoc).getTime() : 0;
            return dateA - dateB;
        });

        console.log(`[${contexto}] Lote ordenado para consistencia: ${loteAgrupado.map(d => d.header.comunicadoId).join(' -> ')}`);

        return _procesarBatchInterno(loteAgrupado, cache);
    } catch (error) {
        console.error(`Error en ${contexto}:`, error);
        return { success: false, message: `Error fatal: ${error.message}` };
    }
}

function _procesarBatchInterno(loteAgrupado, cache) {
    const debugLogs = [];
    const logBatch = (msg) => {
        console.log(msg);
        debugLogs.push(msg);
    };

    const contexto = '_procesarBatchInterno';
    try {
        logBatch(`[${contexto}] Inicio de proceso batch. Total registros: ${loteAgrupado.length}`);

        // Separar válidos y omitidos
        const validos = loteAgrupado.filter(d => d.validacion.esValido && d.validacion.status !== 'OMITIDO');
        const omitidos = loteAgrupado.filter(d => !d.validacion.esValido || d.validacion.status === 'OMITIDO');

        if (validos.length === 0) {
            return _buildResponse(false, 'No hay registros válidos.', { total: loteAgrupado.length, omitidos: omitidos.length }, omitidos, loteAgrupado);
        }

        // ==========================================================================================
        // FASE 0: CLASIFICACIÓN DE DOCUMENTOS (NUEVOS vs EXISTENTES)
        // ==========================================================================================
        logBatch(`[${contexto}] FASE 0: Clasificando ${validos.length} documentos...`);

        const docsParaCrear = [];     // Comunicados que NO existen en la BD
        const docsParaActualizar = []; // Comunicados que SÍ existen en la BD

        validos.forEach(doc => {
            const idReferencia = _resolveIdFromCache(cache.cuentas, doc.header.refCta, ['referencia', 'cuenta']);

            // Si la cuenta no existe aún, se creará más adelante, así que lo marcamos como nuevo
            if (!idReferencia) {
                doc._isNew = true;
                doc._needsCuentaCreation = true;
                docsParaCrear.push(doc);
                return;
            }

            doc._idReferencia = idReferencia;

            // Check status for routing
            const st = doc.analisis.comunicado.status;

            if (st === 'NUEVO' || st === 'ACTUALIZACION') {
                // ACTUALIZACION here means "New Record that is an version" (because existingCom was null)
                // Or "Update in Batch" (simulated parent)
                // We treat it as CREATE because we need to insert the row.
                // (If it was REEMPLAZAR, it means we found an existing ID to overwrite)

                // However, "ACTUALIZACION" logic in `_analizarDocumento` includes cases where `existingCom` IS found relative to parent?
                // Wait. In `_analizarDocumento`:
                // If `existingCom` (PARENT) was found:
                //    `statusCom` -> 'REEMPLAZAR' or 'ACTUALIZACION' (modified above)
                // Ah, I modified `_analizarDocumento` to set 'ACTUALIZACION' if simulated.
                // But what if Parent is REAL DB Record?
                // Previous logic: existingCom = baseCom. THEN `statusCom = 'REEMPLAZAR'`.
                // So "REEMPLAZAR" implies UPDATE operation.

                // If I want to INSERT L30A (while L30 exists), I CANNOT use REEMPLAZAR if logic uses `existingCom.id` to `update()`.
                // I must ensure that for VERSIONS (L30 -> L30A), we perform INSERT (Create), NOT Update.

                // CRITICAL CHECK: Does Reemplazar imply Overwrite?
                // Yes, `docsParaActualizar` usually implies `updateComunicado(id, data)`.

                // We want L30A to be a NEW ROW even if L30 exists.
                // So we must route it to `docsParaCrear`.

                // If the logic detects "Version Upgrade" (L30 -> L30A), it sets `existingCom`.
                // If we push to `docsParaActualizar`, we overwrite L30 with L30A. BAD.

                // FIX: For Version Upgrades (L30 -> L30A), we want CREATE.
                // In `_analizarDocumento`, if existingCom (Parent) is found, we currently fall into "REEMPLAZAR".
                // But that's wrong for versions.
                // Actually, the original code had:
                // `doc._isVersionUpgrade = true; docsParaActualizar.push(doc);` in Phase 0 Classification.
                // BUT `_analizarDocumento` is Pre-Phase 0 (Preview).

                // Here in `_procesarBatchInterno` (Phase 0), we re-evaluate?
                // No, `_procesarBatchInterno` iterates `validos`.
                // It repeats some logic.

                // I will fix Phase 0 logic in this block.

                const vParsed = _parseVersion(doc.header.comunicadoId);
                // ... logic continues ...
                // If new, push to create.

                // If I set 'ACTUALIZACION' in Preview, I should respect it here.
                docsParaCrear.push(doc);

            } else if (st === 'REEMPLAZAR') {
                docsParaActualizar.push(doc);
            } else if (st === 'EXISTE' || st === 'OMITIDO') {
                // Do nothing explicit, maybe log
            } else {
                // Default fallback
                docsParaCrear.push(doc);
            }

            return; // Skip the old logic block below for cleanliness or integrate?
            // The old logic block below re-did the search. I should replace it completely or let it run?
            // The old block (lines 651+) re-finds existingCom.
            // I should REPLACE the whole iteration logic to rely on `doc.analisis.comunicado.status`.

            /* 
               Refactoring Phase 0 to rely on Pre-Analysis Status is safer and consistent with Wizard.
               But `_analizarDocumento` might not have been run if we just called `ejecutarImportacion` directly?
               `ejecutarImportacion` calls `validarLote`, but `previsualizarImportacion` calls `_analizarDocumento`.
               `ejecutarImportacion` does NOT call `_analizarDocumento`.
               So `doc.analisis` might be missing or stale if we skip strict check here!
               
               However, `_procesarBatchInterno` is called by `importarUnico` (which prepares item with validation/analisis?)
               No, `importarUnico` calls `_procesarBatchInterno` with a constructed item.
               
               `ejecutarImportacion` calls `_procesarBatchInterno` directly.
               AND `ejecutarImportacion` does NOT call `_analizarDocumento`.
               So `doc.analisis` DOES NOT EXIST or is minimal.
               
               So I MUST keep the logic inside `_procesarBatchInterno`.
               
               I will Modify the logic inside the loop (lines 650+) to handle ACTUALIZACION.
            */
        });

        logBatch(`[${contexto}] Clasificación: ${docsParaCrear.length} NUEVOS, ${docsParaActualizar.length} EXISTENTES`);

        // ==========================================================================================
        // FASE 1: CREAR CATÁLOGOS AUXILIARES (Aseguradoras, Distritos, Ajustadores, Siniestros, Cuentas)
        // ==========================================================================================
        logBatch(`[${contexto}] FASE 1: Creando catálogos auxiliares...`);

        const counts = { newAsegs: 0, newSins: 0, newCuentas: 0, newComs: 0, newDG: 0, newActs: 0, newLines: 0, updatedDG: 0 };

        // Aseguradoras
        const newAseguradoras = _extractUnique(validos, (d => d.header.aseguradoraNombre || d.header.aseguradora), cache.aseguradoras, 'descripción');
        if (newAseguradoras.length > 0) {
            const res = createBatch('aseguradoras', newAseguradoras.map(desc => ({ descripción: desc })));
            counts.newAsegs += res.count;
            _updateCache(cache, 'aseguradoras', res.ids, newAseguradoras, 'descripción');
        }

        // Distritos
        const newDistritos = _extractUnique(validos, 'distritoRiego', cache.distritosRiego, 'distritoRiego');
        if (newDistritos.length > 0) {
            const res = createBatch('distritosRiego', newDistritos.map(d => ({ distritoRiego: d })));
            _updateCache(cache, 'distritosRiego', res.ids, newDistritos, 'distritoRiego');
        }

        // Ajustadores
        const newAjustadores = _extractUnique(validos, (d => d.header.ajustadorNombre || d.header.ajustador), cache.ajustadores, 'nombreAjustador');
        if (newAjustadores.length > 0) {
            const validNewAjustadores = newAjustadores.filter(a => a && a.length > 2);
            if (validNewAjustadores.length > 0) {
                const res = createBatch('ajustadores', validNewAjustadores.map(a => ({ nombreAjustador: a, nombre: a })));
                _updateCache(cache, 'ajustadores', res.ids, validNewAjustadores, 'nombreAjustador');
            }
        }

        // Siniestros
        const siniestrosMap = _prepareSiniestrosBatch(validos, cache);
        if (siniestrosMap.inserts.length > 0) {
            const res = createBatch('siniestros', siniestrosMap.inserts);
            counts.newSins += res.count;
            _updateCache(cache, 'siniestros', res.ids, siniestrosMap.keys, 'siniestro');
        }

        // Cuentas
        const idAjustadorDefault = _findIdAjustadorDefault(cache.ajustadores);
        const cuentasMap = _prepareCuentasBatch(validos, cache, idAjustadorDefault);
        if (cuentasMap.inserts.length > 0) {
            const res = createBatch('cuentas', cuentasMap.inserts);
            counts.newCuentas += res.count;
            _updateCache(cache, 'cuentas', res.ids, cuentasMap.keys, ['referencia', 'cuenta']);
        }

        SpreadsheetApp.flush();
        logBatch(`[${contexto}] FASE 1 completada. Catálogos creados.`);

        // ==========================================================================================
        // FASE 2: CREAR COMUNICADOS NUEVOS (y sus DatosGenerales + Actualizaciones)
        // ==========================================================================================
        const batchComunicados = [];
        const batchDatosGenerales = [];
        const batchActualizaciones = [];
        const batchPresupuestos = [];

        if (docsParaCrear.length > 0) {
            logBatch(`[${contexto}] FASE 2: Procesando ${docsParaCrear.length} comunicados NUEVOS...`);

            // 2A: Preparar datos de comunicados únicos para insertar
            const comUnicosPorKey = new Map(); // Evitar duplicados

            docsParaCrear.forEach(doc => {
                // Resolver idReferencia (ahora que las cuentas ya se crearon)
                const idReferencia = _resolveIdFromCache(cache.cuentas, doc.header.refCta, ['referencia', 'cuenta']);
                doc._idReferencia = idReferencia;

                if (!idReferencia) {
                    logBatch(`[${contexto}] WARN: No se pudo resolver cuenta para ${doc.header.refCta}`);
                    _markError(doc, omitidos, `Cuenta ${doc.header.refCta} no encontrada`);
                    return;
                }

                const key = `${idReferencia}|${doc.header.comunicadoId}`;

                if (!comUnicosPorKey.has(key)) {
                    comUnicosPorKey.set(key, {
                        data: { idReferencia, comunicado: doc.header.comunicadoId, status: 1 },
                        docs: [doc]
                    });
                } else {
                    comUnicosPorKey.get(key).docs.push(doc);
                }
            });

            // 2B: Insertar comunicados nuevos
            const comUnicosArray = Array.from(comUnicosPorKey.values());
            if (comUnicosArray.length > 0) {
                const comsBatchData = comUnicosArray.map(item => item.data);
                logBatch(`[${contexto}] Insertando ${comsBatchData.length} comunicados nuevos...`);

                const resComs = createBatch('comunicados', comsBatchData);
                counts.newComs += resComs.count;

                // Asignar IDs reales a los documentos
                resComs.ids.forEach((realId, i) => {
                    const item = comUnicosArray[i];
                    item.docs.forEach(d => d._newComId = realId);
                    cache.comunicados.push({ id: realId, ...item.data });
                });

                SpreadsheetApp.flush();
            }

            // 2C: Crear DatosGenerales y Actualizaciones para cada documento nuevo
            docsParaCrear.forEach(doc => {
                const idComunicado = doc._newComId;
                if (!idComunicado) {
                    logBatch(`[${contexto}] SKIP: Doc ${doc.header.refCta}-${doc.header.comunicadoId} sin ID de comunicado`);
                    return;
                }

                const isOrigen = doc.header.tipoRegistro === 'ORIGEN';

                // Crear DatosGenerales (solo una vez por comunicado)
                const dgExiste = batchDatosGenerales.some(dg => String(dg.idComunicado) === String(idComunicado));
                if (!dgExiste) {
                    const idEstado = _resolveIdFromCache(cache.estados, doc.header.estado, 'estado');
                    const idSiniestro = _resolveIdFromCache(cache.siniestros, doc.header.refSiniestro, 'siniestro');
                    const idDR = _resolveIdFromCache(cache.distritosRiego, doc.header.distritoRiego, 'distritoRiego');
                    let idAjustador = _resolveIdFromCache(cache.ajustadores, doc.header.ajustador, ['nombreAjustador', 'nombre']) || idAjustadorDefault;

                    logBatch(`[${contexto}] FASE 2: Creando DG para nuevo ComID ${idComunicado} (Estado: ${doc.header.estado} -> ${idEstado})`);

                    batchDatosGenerales.push({
                        idComunicado: idComunicado,
                        descripcion: doc.header.descripcion || `${doc.header.refCta}-${doc.header.comunicadoId}`,
                        fecha: doc.header.fechaDoc,
                        idEstado: idEstado,
                        idSiniestro: idSiniestro,
                        idDR: idDR,
                        idAjustador: idAjustador
                    });
                }

                // Crear Actualizacion
                const actsPrevias = batchActualizaciones.filter(a => String(a.idComunicado) === String(idComunicado));
                const consecutivo = actsPrevias.length + 1;

                logBatch(`[${contexto}] FASE 2: Creando Actualizacion #${consecutivo} para ComID ${idComunicado}`);

                batchActualizaciones.push({
                    idComunicado: idComunicado,
                    consecutivo: consecutivo,
                    esOrigen: isOrigen && consecutivo === 1 ? 1 : 0,
                    revision: isOrigen ? 'Origen' : doc.header.tipoRegistro,
                    monto: doc.header.totalPdf,
                    montoCapturado: null,
                    montoSupervisión: (doc.header.totalPdf || 0) * 0.05,
                    fecha: new Date(),
                    _docLineas: doc.lineas
                });
            });

            logBatch(`[${contexto}] FASE 2 completada. DG preparados: ${batchDatosGenerales.length}, Acts preparadas: ${batchActualizaciones.length}`);
        }

        // ==========================================================================================
        // FASE 3: ACTUALIZAR COMUNICADOS EXISTENTES
        // ==========================================================================================
        if (docsParaActualizar.length > 0) {
            logBatch(`[${contexto}] FASE 3: Procesando ${docsParaActualizar.length} comunicados EXISTENTES...`);

            // DEBUG: Mostrar qué documentos van a actualizarse
            docsParaActualizar.forEach((doc, idx) => {
                logBatch(`[${contexto}] FASE 3 - Doc #${idx + 1}: ${doc.header.refCta}-${doc.header.comunicadoId} | Tipo: ${doc.header.tipoRegistro} | Desc CSV: "${doc.header.descripcion}"`);
            });

            docsParaActualizar.forEach(doc => {
                const idComunicado = doc._existingComId;
                const isOrigen = doc.header.tipoRegistro === 'ORIGEN';

                // =========================================================
                // 3.0 VERSION UPGRADE (Actualizar nombre en tabla comunicados)
                // =========================================================
                if (doc._isVersionUpgrade) {
                    logBatch(`[${contexto}] FASE 3: Upgrade de Versión detectado para ID ${idComunicado}: -> ${doc.header.comunicadoId}`);
                    try {
                        const resUpdName = updateRow('comunicados', idComunicado, { comunicado: doc.header.comunicadoId });
                        if (!resUpdName.success) {
                            logBatch(`[${contexto}] ERROR al actualizar nombre de comunicado: ${resUpdName.message}`);
                        } else {
                            // Actualizar cache local por si acaso
                            const comCache = cache.comunicados.find(c => String(c.id) === String(idComunicado));
                            if (comCache) comCache.comunicado = doc.header.comunicadoId;
                        }
                    } catch (e) {
                        logBatch(`[${contexto}] EXCEPCION updating comunicado name: ${e.message}`);
                    }
                }

                // Buscar DatosGenerales existente
                const existingDG = cache.datosGenerales.find(dg => String(dg.idComunicado) === String(idComunicado));

                if (existingDG) {
                    logBatch(`[${contexto}] FASE 3: Verificando actualización para ComID ${idComunicado} | DG.id: ${existingDG.id} | DG.desc DB: "${existingDG.descripcion}"`);

                    // Comparar campos y preparar updates
                    const updates = {};
                    let doUpdate = false;

                    // 1. Estado
                    const idEstado = _resolveIdFromCache(cache.estados, doc.header.estado, 'estado');
                    if (idEstado && String(idEstado) !== String(existingDG.idEstado)) {
                        logBatch(`[${contexto}] -> Estado CAMBIO: ${existingDG.idEstado} -> ${idEstado}`);
                        updates.idEstado = idEstado;
                        doUpdate = true;
                    }

                    // 2. Distrito
                    const idDR = _resolveIdFromCache(cache.distritosRiego, doc.header.distritoRiego, 'distritoRiego');
                    if (idDR && String(idDR) !== String(existingDG.idDR)) {
                        logBatch(`[${contexto}] -> Distrito CAMBIO: ${existingDG.idDR} -> ${idDR}`);
                        updates.idDR = idDR;
                        doUpdate = true;
                    }

                    // 3. Siniestro
                    const idSiniestro = _resolveIdFromCache(cache.siniestros, doc.header.refSiniestro, 'siniestro');
                    if (idSiniestro && String(idSiniestro) !== String(existingDG.idSiniestro)) {
                        updates.idSiniestro = idSiniestro;
                        doUpdate = true;
                    }

                    // 4. Fecha
                    if (doc.header.fechaDoc) {
                        const dateCSV = new Date(doc.header.fechaDoc).toISOString().split('T')[0];
                        const dateDB = existingDG.fecha ? new Date(existingDG.fecha).toISOString().split('T')[0] : '';
                        if (dateCSV !== dateDB) {
                            updates.fecha = dateCSV;
                            doUpdate = true;
                        }
                    }

                    // 5. Descripción: Recalcular SIEMPRE para asegurar historial acumulativo en batch (L30A, L30B, L30C)
                    // Resolver objeto cuenta para el helper
                    const ctaObj = cache.cuentas.find(c => c.id === existingDG.idReferencia) || cache.cuentas.find(c => c.referencia === doc.header.refCta);

                    // RECALCULAR Historial en tiempo real usando el cache (que se irá actualizando en cada ciclo del loop)
                    const descCalculada = _construirHistorial(cache, ctaObj, doc.header.comunicadoId);
                    const descFinal = descCalculada || doc.header.descripcion || '';

                    if (descFinal) {
                        const descNueva = String(descFinal).trim();
                        const descExistente = String(existingDG.descripcion || '').trim();

                        logBatch(`[${contexto}] DEBUG Descripción - Calc: "${descNueva}" | DB: "${descExistente}"`);

                        // Solo actualizar si es diferente
                        if (normalizarTexto(descNueva) !== normalizarTexto(descExistente)) {
                            updates.descripcion = descNueva;
                            doUpdate = true;
                            logBatch(`[${contexto}] -> Descripción ACTUALIZADA: "${descExistente}" -> "${descNueva}"`);
                        } else {
                            logBatch(`[${contexto}] -> Descripción SIN CAMBIOS (idéntica normalizada)`);
                        }
                    } else {
                        logBatch(`[${contexto}] -> SIN descripción calculada ni en header`);
                    }

                    if (doUpdate) {
                        logBatch(`[${contexto}] -> Ejecutando UPDATE para DG ID ${existingDG.id}: ${JSON.stringify(updates)}`);
                        try {
                            const resUpd = updateRow('datosGenerales', existingDG.id, updates);
                            if (resUpd.success) {
                                counts.updatedDG++;
                                logBatch(`[${contexto}] -> UPDATE exitoso para DG ID ${existingDG.id}`);

                                // CRITICO: Actualizar el CACHE en memoria para que la siguiente iteración (ej: L30B -> L30C)
                                // vea la descripción actualizada (L30, L30B) y pueda adjuntar la suya.
                                Object.assign(existingDG, updates);

                            } else {
                                logBatch(`[${contexto}] -> UPDATE falló para DG ID ${existingDG.id}: ${resUpd.message}`);
                            }
                        } catch (e) {
                            logBatch(`[${contexto}] -> UPDATE error: ${e.message}`);
                            _markError(doc, omitidos, `Error al actualizar: ${e.message}`);
                        }
                    } else {
                        logBatch(`[${contexto}] -> Sin cambios detectados para ComID ${idComunicado}`);
                    }
                } else {
                    logBatch(`[${contexto}] WARN: No se encontró DG para ComID existente ${idComunicado}`);
                }

                // Crear nueva Actualizacion (siempre, para registrar la nueva revisión)
                // IDEMPOTENCIA: Verificar si ya existe esta revisión para evitar duplicados (ej: "ORIGEN" repetido)
                const actsPrevias = cache.actualizaciones.filter(a => String(a.idComunicado) === String(idComunicado));
                const actsEnBatch = batchActualizaciones.filter(a => String(a.idComunicado) === String(idComunicado));

                const tipoRevision = doc.header.tipoRegistro || 'Actualización';

                // Normalizar para comparar (ej: "ORIGEN" vs "Origen")
                const yaExiste = [...actsPrevias, ...actsEnBatch].some(a =>
                    String(a.revision).toUpperCase() === String(tipoRevision).toUpperCase()
                );

                if (yaExiste) {
                    logBatch(`[${contexto}] FASE 3: SKIP Actualizacion. Ya existe revisión '${tipoRevision}' para ComID ${idComunicado}`);
                } else {
                    const consecutivo = actsPrevias.length + actsEnBatch.length + 1;
                    logBatch(`[${contexto}] FASE 3: Creando Actualizacion #${consecutivo} para ComID existente ${idComunicado}`);

                    batchActualizaciones.push({
                        idComunicado: idComunicado,
                        consecutivo: consecutivo,
                        esOrigen: 0, // Ya existe, no puede ser origen
                        revision: tipoRevision,
                        monto: doc.header.totalPdf,
                        montoCapturado: null,
                        montoSupervisión: (doc.header.totalPdf || 0) * 0.05,
                        fecha: new Date(),
                        _docLineas: doc.lineas
                    });
                }
            });

            SpreadsheetApp.flush();
            logBatch(`[${contexto}] FASE 3 completada. DG actualizados: ${counts.updatedDG}`);
        }

        // ==========================================================================================
        // FASE 4: INSERCIÓN BATCH FINAL (DatosGenerales, Actualizaciones, Presupuestos)
        // ==========================================================================================
        logBatch(`[${contexto}] FASE 4: Inserción batch final...`);

        // Insertar DatosGenerales
        if (batchDatosGenerales.length > 0) {
            logBatch(`[${contexto}] Insertando ${batchDatosGenerales.length} DatosGenerales...`);
            createBatch('datosGenerales', batchDatosGenerales);
            counts.newDG = batchDatosGenerales.length;
            SpreadsheetApp.flush();
        }

        // Insertar Actualizaciones
        if (batchActualizaciones.length > 0) {
            logBatch(`[${contexto}] Insertando ${batchActualizaciones.length} Actualizaciones...`);
            const resActs = createBatch('actualizaciones', batchActualizaciones);
            counts.newActs = resActs.count;

            // Preparar PresupuestoLineas
            resActs.ids.forEach((idActReal, i) => {
                const updateObj = batchActualizaciones[i];
                const lines = updateObj._docLineas || [];
                lines.forEach(l => {
                    batchPresupuestos.push({
                        idActualizacion: idActReal,
                        descripcion: l.concepto,
                        categoria: l.categoria,
                        importe: l.importe,
                        fechaCreacion: new Date()
                    });
                });
            });
            SpreadsheetApp.flush();
        }

        // Insertar Presupuestos
        if (batchPresupuestos.length > 0) {
            logBatch(`[${contexto}] Insertando ${batchPresupuestos.length} líneas de presupuesto...`);
            const resLines = createBatch('presupuestoLineas', batchPresupuestos);
            counts.newLines = resLines.count;
            SpreadsheetApp.flush();
        }

        // FIN DEL PROCESO
        logBatch(`[${contexto}] Batch Completado. Resumen: ${JSON.stringify(counts)}`);

        // Generar CSV Errores
        let csvErrorContent = null;
        if (omitidos.length > 0) {
            csvErrorContent = _generarCsvErrores(omitidos);
        }

        return _buildResponse(true, 'Importación Batch Completada.', counts, omitidos, loteAgrupado, csvErrorContent, debugLogs);

    } catch (error) {
        console.error(`Error en ${contexto}:`, error);
        return { success: false, message: `Error fatal: ${error.message}` };
    }
}

// ============================================================================
// HELPERS DE BATCH & CACHE
// ============================================================================

function _loadCatalogsCache() {
    // Cargar todas las tablas necesarias en memoria
    // Para datasets gigantes, esto podria optimizarse con filtros, pero para <10k filas funciona bien.
    return {
        aseguradoras: readAllRows('aseguradoras').data || [],
        siniestros: readAllRows('siniestros').data || [],
        cuentas: readAllRows('cuentas').data || [],
        comunicados: readAllRows('comunicados').data || [],
        datosGenerales: readAllRows('datosGenerales').data || [],
        estados: readAllRows('estados').data || [],
        ajustadores: readAllRows('ajustadores').data || [],
        distritosRiego: readAllRows('distritosRiego').data || [], // Cargar Distritos
        actualizaciones: readAllRows('actualizaciones').data || []
    };
}

function _updateCache(cache, tableKey, newIds, originalKeys, keyField) {
    // Actualiza el cache local con los nuevos registros insertados
    // originalKeys es array de strings (nombres) o array de keys si es compuesto
    newIds.forEach((id, i) => {
        const item = { id: id };
        const val = originalKeys[i];

        if (typeof val === 'object' && val !== null) {
            // Si el valor es un objeto completo (ej. Cuentas), lo mezclamos
            Object.assign(item, val);
        } else {
            // Si es un valor simple
            if (Array.isArray(keyField)) {
                keyField.forEach(k => item[k] = val);
            } else {
                item[keyField] = val;
            }
        }

        cache[tableKey].push(item);
    });
}




function _extractUnique(docs, docField, existingList, dbField) {
    const unique = new Set();
    docs.forEach(d => {
        const val = d.header[docField];
        if (!val) return;
        // Check if exists in DB
        const exists = existingList.some(item => String(item[dbField] || '').toUpperCase() === String(val).toUpperCase());
        if (!exists) unique.add(val);
    });
    return Array.from(unique);
}

function _prepareSiniestrosBatch(validos, cache) {
    const inserts = [];
    const keys = []; // Para update cache
    const seen = new Set();

    // Primero, indexar existentes para búsqueda rápida
    const existMap = new Set(cache.siniestros.map(s => String(s.siniestro || '').toUpperCase()));

    validos.forEach(d => {
        const h = d.header;
        if (!h.refSiniestro) return;
        const key = String(h.refSiniestro).toUpperCase();

        if (!existMap.has(key) && !seen.has(key)) {
            // Buscar ID Aseguradora (ya deben estar cacheadas tras paso 1)
            const idAseg = _resolveIdFromCache(cache.aseguradoras, h.aseguradora, ['aseguradora', 'nombre']);

            const newSin = {
                siniestro: h.refSiniestro,
                fenomeno: h.fenomeno,
                fi: h.fechaSiniestroFi,
                fondo: h.fondo,
                idAseguradora: idAseg
            };
            inserts.push(newSin);
            keys.push(newSin);
            seen.add(key);
        }
    });
    return { inserts, keys };
}

function _prepareCuentasBatch(validos, cache, idAjustadorDefault) {
    const inserts = [];
    const keys = [];
    const seen = new Set();

    // Indexar existentes. Cuentas busca por referencia O cuenta
    // Simplificación: Asumimos refCta es la clave
    const existMap = new Set(cache.cuentas.map(c => String(c.referencia || '').toUpperCase()));

    validos.forEach(d => {
        const ref = d.header.refCta;
        if (!ref) return;
        const key = String(ref).toUpperCase();

        if (!existMap.has(key) && !seen.has(key)) {
            // Resolver Ajustador especifico de esta fila
            let idAj = _resolveIdFromCache(cache.ajustadores, d.header.ajustador, ['nombreAjustador', 'nombre']);
            if (!idAj) idAj = idAjustadorDefault;

            const newCta = {
                referencia: ref,
                cuenta: ref, // Duplicamos valor por diseño original
                idAjustador: idAj,
                fechaAlta: new Date()
            };
            inserts.push(newCta);
            keys.push(newCta);
            seen.add(key);
        }
    });
    return { inserts, keys };
}

function _resolveIdFromCache(list, value, fieldName) {
    if (!value) return null;
    const clean = String(value).toUpperCase().trim();
    // Arrays handles compuesto ? No, simple
    // Si fieldName es array, checkeamos cualquiera
    const found = list.find(item => {
        if (Array.isArray(fieldName)) {
            return fieldName.some(f => String(item[f] || '').toUpperCase().trim() === clean);
        }
        return String(item[fieldName] || '').toUpperCase().trim() === clean;
    });
    return found ? found.id : null;
}

function _findIdAjustadorDefault(ajustadores) {
    const ct = ajustadores.find(a => String(a.nombre || a.nombreAjustador).toUpperCase().includes('CHARLES'));
    return ct ? ct.id : null;
}

function _markError(doc, omitidos, msg) {
    doc.validacion.esValido = false;
    doc.validacion.status = 'OMITIDO';
    doc.validacion.motivo = msg;
    omitidos.push(doc);
}

function _buildResponse(success, msg, counts, omitidos, allDocs, csvContent, debugLogs) {
    const responseData = {
        success: success,
        message: msg,
        resumen: {
            totalDocumentos: allDocs.length,
            procesados: counts ? (counts.newComs + counts.newLines) : 0,
            detallesTecnicos: counts,
            omitidos: omitidos ? omitidos.length : 0
        },
        csvErrorContent: csvContent,
        debugLogs: debugLogs,
        detalles: allDocs ? allDocs.map(d => ({
            ref: d.header.refCta,
            comunicado: d.header.comunicadoId,
            tipo: d.header.tipoRegistro, // Add this to fix UNK in frontend
            valido: d.validacion.esValido && d.validacion.status !== 'OMITIDO',
            errores: d.validacion.motivo ? [d.validacion.motivo] : d.validacion.errores
        })) : []
    };

    return {
        success: success,
        message: msg,
        data: responseData
    };
}


// ============================================================================
// LOGICA DE PARSING Y VALIDACION (ORIGINAL - MANTENIDA)
// ============================================================================

/**
 * Convierte un archivo Excel (base64) a formato CSV
 * VERSIÓN ROBUSTA - Soporta Drive API v2 y v3
 */
function convertirExcelACsv(base64Data) {
    const contexto = 'convertirExcelACsv';
    console.log(`[${contexto}] Iniciando conversión de Excel a CSV...`);
    let fileId = null;
    try {
        const decodedData = Utilities.base64Decode(base64Data);
        const blob = Utilities.newBlob(decodedData, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'temp_' + new Date().getTime() + '.xlsx');

        let file;
        if (Drive.Files.insert) {
            file = Drive.Files.insert({ title: blob.getName(), mimeType: MimeType.GOOGLE_SHEETS }, blob, { convert: true });
        } else if (Drive.Files.create) {
            file = Drive.Files.create({ name: blob.getName(), mimeType: MimeType.GOOGLE_SHEETS }, blob);
        } else {
            throw new Error('Drive API no disponible.');
        }

        fileId = file.id;
        const ss = SpreadsheetApp.openById(fileId);
        const sheet = ss.getSheets()[0];
        const data = sheet.getDataRange().getValues();

        if (!data || data.length === 0) throw new Error('Excel vacío');

        let csvContent = '';
        const timeZone = Session.getScriptTimeZone();
        data.forEach(row => {
            const csvRow = row.map(cell => {
                let s = '';
                if (cell instanceof Date) {
                    // Formatear fechas a ISO simple para evitar "Fri May 12..."
                    s = Utilities.formatDate(cell, timeZone, 'yyyy-MM-dd');
                } else {
                    s = String(cell || '');
                }

                if (s.includes(',') || s.includes('"') || s.includes('\n')) s = '"' + s.replace(/"/g, '""') + '"';
                return s;
            }).join(',');
            csvContent += csvRow + '\n';
        });

        return {
            success: true,
            data: {
                success: true,
                csvContent: csvContent,
                message: 'Archivo Excel convertido exitosamente'
            }
        };

    } catch (error) {
        console.error(`[${contexto}] Error:`, error);
        return { success: false, message: error.message };
    } finally {
        if (fileId) {
            try {
                if (Drive.Files.remove) Drive.Files.remove(fileId);
                else if (Drive.Files.delete) Drive.Files.delete(fileId);
            } catch (e) { }
        }
    }
}

function parseImportFile(csvInfo) {
    let cleanCsv = csvInfo;
    if (cleanCsv.charCodeAt(0) === 0xFEFF) cleanCsv = cleanCsv.slice(1);
    const rows = Utilities.parseCsv(cleanCsv);
    if (!rows || rows.length < 2) return [];

    const headers = rows[0].map(h => String(h).trim().toUpperCase());
    const dataRows = rows.slice(1);

    const idxRef = headers.indexOf('REF_CTA');
    const idxCom = headers.findIndex(h => h === 'COMUNICADO_ID' || h === 'COMUNICADO'); // Alias
    const idxTipo = headers.indexOf('TIPO_REGISTRO');
    const idxFecha = headers.indexOf('FECHA_DOC');

    // Robust Column Lookup with Aliases
    const idxEstado = headers.findIndex(h => ['ESTADO', 'ENTIDAD', 'EDO', 'NOMBRE'].includes(h));
    const idxSinRef = headers.findIndex(h => ['REF_SINIESTRO', 'SINIESTRO_REF', 'SINI_REF'].includes(h));
    if (idxSinRef === -1 && headers.indexOf('REF_SINIESTRO') > -1) idxSinRef = headers.indexOf('REF_SINIESTRO'); // Fallback

    const idxAseg = headers.findIndex(h => ['ASEGURADORA', 'ASEG'].includes(h));
    const idxFen = headers.indexOf('FENOMENO');
    const idxFi = headers.map(h => h.replace(/_/g, '')).indexOf('FECHASINIESTROFI'); // Loose match attempt? No, keep it safe
    // const idxFi = headers.indexOf('FECHA_SINIESTRO_FI'); 

    const idxFondo = headers.indexOf('FONDO');
    const idxDistrito = headers.findIndex(h => ['DISTRITO_RIEGO', 'DISTRITO', 'DR', 'NOMBRE_DISTRITO'].includes(h));
    const idxAjustador = headers.findIndex(h => ['AJUSTADOR', 'NOMBRE_AJUSTADOR', 'AJUST'].includes(h));
    const idxDesc = headers.indexOf('DESCRIPCION'); // Nuevo: Opción explicita usuario
    const idxTotal = headers.findIndex(h => h === 'TOTAL_DOC_PDF' || h === 'TOTAL_DOC_P'); // Alias por si viene cortado
    const idxConcepto = headers.indexOf('CONCEPTO_OBRA');
    const idxCat = headers.indexOf('CATEGORIA');
    const idxImporte = headers.indexOf('IMPORTE_RENGLON');
    const idxSup = headers.findIndex(h => h === 'MONTO_SUPERVISION' || h === 'MONTO_SUPERV'); // Alias

    if (idxRef === -1 || idxCom === -1) throw new Error('Faltan columnas REF_CTA o COMUNICADO_ID');

    const agrupado = {};

    dataRows.forEach(row => {
        const refCta = String(row[idxRef] || '').trim().toUpperCase();
        const comId = String(row[idxCom] || '').split(',')[0].trim().toUpperCase();
        const tipoRegistro = idxTipo > -1 ? String(row[idxTipo] || 'ACTUALIZACION').trim().toUpperCase() : 'ACTUALIZACION';

        if (!refCta || !comId) return;

        // Clave única compuesta: Ref + ID + TIPO
        // Esto permite que un ORIGEN y una ACTUALIZACION compartan el mismo ID (L30) pero sean objetos distintos.
        const key = `${refCta}|${comId}|${tipoRegistro}`;

        if (!agrupado[key]) {
            agrupado[key] = {
                header: {
                    refCta: refCta,
                    comunicadoId: comId,
                    descripcion: idxDesc > -1 ? String(row[idxDesc] || '').trim() : null, // Capturar descripcion
                    tipoRegistro: tipoRegistro,
                    fechaDoc: idxFecha > -1 ? row[idxFecha] : null,
                    estado: idxEstado > -1 ? String(row[idxEstado] || '').toUpperCase() : '',
                    refSiniestro: idxSinRef > -1 ? String(row[idxSinRef] || '').toUpperCase() : '',
                    aseguradora: idxAseg > -1 ? String(row[idxAseg] || '').toUpperCase() : '',
                    fenomeno: idxFen > -1 ? String(row[idxFen] || '').toUpperCase() : '',
                    fechaSiniestroFi: idxFi > -1 ? row[idxFi] : null,
                    fondo: idxFondo > -1 ? String(row[idxFondo] || '').toUpperCase() : '',
                    distritoRiego: idxDistrito > -1 ? String(row[idxDistrito] || '').toUpperCase() : '',
                    ajustador: idxAjustador > -1 ? String(row[idxAjustador] || '').toUpperCase() : '',
                    totalPdf: idxTotal > -1 ? (parseNumeric(row[idxTotal]) || 0) : 0,
                    montoSupervision: idxSup > -1 ? (parseNumeric(row[idxSup]) || 0) : 0
                },
                lineas: [],
                validacion: { sumaLineas: 0, esValido: true, errores: [] }
            };
        }
        const importe = idxImporte > -1 ? (parseNumeric(row[idxImporte]) || 0) : 0;
        agrupado[key].lineas.push({
            concepto: idxConcepto > -1 ? String(row[idxConcepto] || '') : '',
            categoria: idxCat > -1 ? String(row[idxCat] || '') : '',
            importe: importe
        });
        agrupado[key].validacion.sumaLineas += importe;
    });
    return Object.values(agrupado);
}

function validarLote(loteAgrupado, cache) {
    // Usamos el CACHE pasado por parametro en lugar de leer DB
    const cuentasExistentes = cache.cuentas;
    const comunicadosExistentes = cache.comunicados;

    loteAgrupado.forEach(doc => {
        doc.validacion = { esValido: true, status: 'OK', motivo: null, errores: [], sumaLineas: doc.validacion.sumaLineas };

        if (!doc.header.refCta || !doc.header.comunicadoId) {
            doc.validacion.esValido = false; doc.validacion.status = 'OMITIDO'; doc.validacion.motivo = 'Datos clave faltantes'; return;
        }
        if (!doc.header.totalPdf || doc.header.totalPdf <= 0) {
            doc.validacion.esValido = false; doc.validacion.status = 'OMITIDO'; doc.validacion.motivo = 'Monto Financiaro invalido'; return;
        }
        const diff = Math.abs(doc.header.totalPdf - doc.validacion.sumaLineas);

        // AUTO-CORRECCIÓN: Si hay líneas, la verdad está en la suma de líneas.
        if (doc.validacion.sumaLineas > 0 && diff > 1) {
            // Si el usuario puso un Header Total diferente a la suma, asumimos error de captura en el Header
            // y priorizamos la suma de las líneas de desglose.
            const oldTotal = doc.header.totalPdf;
            doc.header.totalPdf = doc.validacion.sumaLineas;
            doc.validacion.motivo = `Corregido: Total Header (${oldTotal}) ajustado a Suma Líneas (${doc.validacion.sumaLineas})`;
            // No marcamos omitido, dejamos pasar.
        } else if (doc.validacion.sumaLineas === 0 && doc.header.totalPdf > 0) {
            // Caso: Actualización de Monto sin desglose (posible en ajustes directos)
            // Se mantiene el totalPdf del header valido.
        }

        const cuentaObj = cuentasExistentes.find(c => c.referencia === doc.header.refCta || c.cuenta === doc.header.refCta);
        const idCuenta = cuentaObj ? cuentaObj.id : null;

        if (doc.header.tipoRegistro === 'ACTUALIZACION') {
            // 1. Validar REFERENCIA (Cuenta)
            // Existe en DB o existe UN ORIGEN para esta cuenta en el lote?
            const origenCuentaEnLote = loteAgrupado.find(d => d.header.refCta === doc.header.refCta && d.header.tipoRegistro === 'ORIGEN' && d.validacion.esValido);

            if (!idCuenta && !origenCuentaEnLote) {
                doc.validacion.esValido = false;
                doc.validacion.status = 'OMITIDO';
                doc.validacion.motivo = 'No existe Referencia (Cuenta) ni Origen en lote';
                return;
            }

            // 2. Validar COMUNICADO (Padre)
            // Existe en DB estricto?
            const existeComunicado = comunicadosExistentes.some(c => String(c.idReferencia) === String(idCuenta) && String(c.comunicado) === String(doc.header.comunicadoId));

            // Existe en Lote estricto? Buscamos ESPECIFICAMENTE el comunicado ID, no solo la cuenta.
            const origenComunicadoEnLote = loteAgrupado.find(d =>
                d.header.refCta === doc.header.refCta &&
                d.header.tipoRegistro === 'ORIGEN' &&
                d.header.comunicadoId === doc.header.comunicadoId &&
                d.validacion.esValido
            );

            if (!existeComunicado && !origenComunicadoEnLote) {
                // Caso especifico: Tenemos la cuenta (via lote o DB) pero el ID Comunicado no existe.
                if (origenCuentaEnLote) {
                    // Si hay otros origenes para esta cuenta, avisar cual se encontró para dar contexto, 
                    // pero el error real es que FALTA el origen especifico.
                    doc.validacion.esValido = false;
                    doc.validacion.status = 'OMITIDO';
                    doc.validacion.motivo = `No se encontró el Origen '${doc.header.comunicadoId}' en el lote (Se encontró otro: '${origenCuentaEnLote.header.comunicadoId}').`;
                    return;
                }

                doc.validacion.esValido = false;
                doc.validacion.status = 'OMITIDO';
                doc.validacion.motivo = 'No existe Comunicado Origen ni en DB ni en Lote';
                return;
            }



            if (origenComunicadoEnLote) {
                doc.validacion.motivo = 'Validado por dependencia en lote (Nuevo Origen)';
            }
        }
        else if (doc.header.tipoRegistro === 'ORIGEN') {
            if (!idCuenta) doc.validacion.esAltaExpress = true;
        }
    });
}

function parseNumeric(value) {
    if (value === null || value === undefined || value === '') return 0;
    const clean = String(value).replace(/[$,]/g, '');
    const num = parseFloat(clean);
    return isNaN(num) ? 0 : num;
}

function _generarCsvErrores(listaOmitidos) {
    const headers = ['REF_CTA', 'COMUNICADO_ID', 'TIPO', 'MOTIVO_ERROR'];
    let csvString = headers.join(',') + '\n';
    listaOmitidos.forEach(item => {
        const row = [
            `"${item.header.refCta || ''}"`,
            `"${item.header.comunicadoId || ''}"`,
            `"${item.header.tipoRegistro || ''}"`,
            `"${item.validacion.motivo || item.validacion.errores.join('; ') || 'Error'}"`
        ];
        csvString += row.join(',') + '\n';
    });
    return Utilities.base64Encode(csvString);
}

// ============================================================================
// MÓDULO: IMPORTACIÓN DE FACTURAS
// ============================================================================

/**
 * API PÚBLICA: Previsualizar Facturas (Solo Lectura)
 */
function previsualizarImportacionFacturas(fileContent) {
    const contexto = 'previsualizarImportacionFacturas';
    console.log(`[${contexto}] Iniciando...`);

    try {
        const rows = parseFacturaFile(fileContent);

        // Cargar caches necesarios
        const facturasExistentes = readAllRows('facturas').data || [];
        const comunicadosExistentes = readAllRows('comunicados').data || [];
        const cuentasExistentes = readAllRows('cuentas').data || []; // Por si referencia es por cuenta?? No, requerimos ID Comunicado o Clave.

        // Mapeo UUIDs existentes
        const uuidsMap = new Set(facturasExistentes.map(f => String(f.uuid || '').toUpperCase()));

        // Mapeo Comunicados (Clave -> ID)
        // La columna REF_COMUNICADO puede ser la CLAVE del comunicado (e.g. C-001)
        const comunicadoMap = new Map();
        comunicadosExistentes.forEach(c => {
            comunicadoMap.set(String(c.comunicado).trim().toUpperCase(), c);
        });

        const previewData = rows.map(row => {
            const h = row;
            const analisis = {
                esValido: true,
                motivo: null,
                uuidDuplicado: false,
                comunicadoEncontrado: false
            };

            // Validaciones
            if (!h.uuid || !h.folio || !h.monto || !h.refComunicado) {
                analisis.esValido = false;
                analisis.motivo = 'Datos obligatorios faltantes (UUID, Folio, Monto, Ref)';
            }

            // Validar UUID Único
            if (h.uuid && uuidsMap.has(h.uuid.toUpperCase())) {
                analisis.esValido = false;
                analisis.motivo = 'UUID ya registrado en el sistema';
                analisis.uuidDuplicado = true;
            }

            // Validar Referencia Comunicado
            let comRef = null;
            if (h.refComunicado) {
                const key = String(h.refComunicado).trim().toUpperCase();
                const found = comunicadoMap.get(key);
                if (found) {
                    analisis.comunicadoEncontrado = true;
                    comRef = found.comunicado; // Mostrar clave real
                } else {
                    analisis.esValido = false;
                    analisis.motivo = `Comunicado '${h.refComunicado}' no encontrado`;
                }
            }

            return {
                folio: h.folio,
                uuid: h.uuid,
                fecha: h.fecha ? new Date(h.fecha).toISOString().split('T')[0] : '',
                monto: h.monto,
                comunicadoRef: h.refComunicado,
                proveedor: h.proveedor,
                estatus: h.estatus,
                esValido: analisis.esValido,
                motivo: analisis.motivo
            };
        });

        const resumen = {
            total: previewData.length,
            validos: previewData.filter(d => d.esValido).length,
            omitidos: previewData.filter(d => !d.esValido).length
        };

        return {
            success: true,
            data: {
                resumen: resumen,
                filas: previewData
            }
        };

    } catch (error) {
        console.error(`Error en ${contexto}:`, error);
        return { success: false, message: error.message };
    }
}

/**
 * Controladora para Ejecutar la Importación de Facturas
 */
function ejecutarImportacionFacturas(fileContent) {
    const contexto = 'ejecutarImportacionFacturas';
    console.log(`[${contexto}] Iniciando persistencia...`);

    try {
        const rows = parseFacturaFile(fileContent);

        // Recargar cache para asegurar consistencia
        const facturasExistentes = readAllRows('facturas').data || [];
        const comunicadosExistentes = readAllRows('comunicados').data || [];

        const uuidsMap = new Set(facturasExistentes.map(f => String(f.uuid || '').toUpperCase()));
        const comunicadoMap = new Map();
        comunicadosExistentes.forEach(c => {
            comunicadoMap.set(String(c.comunicado).trim().toUpperCase(), c.id); // Map Clave -> ID Real
        });

        // Filtrar y preparar para batch
        const batchFacturas = [];
        const omitidos = [];

        rows.forEach(row => {
            // Re-validación rápida
            const uuid = String(row.uuid || '').trim().toUpperCase();
            if (!uuid || !row.folio || !row.monto || !row.refComunicado) {
                omitidos.push({ ...row, error: 'Datos incompletos' });
                return;
            }
            if (uuidsMap.has(uuid)) {
                omitidos.push({ ...row, error: 'UUID duplicado' });
                return;
            }

            const comKey = String(row.refComunicado).trim().toUpperCase();
            const idComunicado = comunicadoMap.get(comKey);

            if (!idComunicado) {
                omitidos.push({ ...row, error: 'Comunicado no existe' });
                return;
            }

            batchFacturas.push({
                idComunicado: idComunicado,
                folio: row.folio,
                fecha: row.fecha, // Deberia ser obj Date o string ISO? createBatch usa raw, Sheets parsea. Mejor Date.
                monto: row.monto,
                uuid: row.uuid,
                estatus: row.estatus || 'VIGENTE',
                proveedor: row.proveedor
            });
        });

        let insertedCount = 0;
        if (batchFacturas.length > 0) {
            const res = createBatch('facturas', batchFacturas);
            insertedCount = res.count;
        }

        return {
            success: true,
            data: {
                resumen: {
                    procesados: insertedCount,
                    omitidos: omitidos.length
                },
                omitidos: omitidos // Opcional devolver detalle
            },
            message: 'Importación de facturas completada'
        };

    } catch (e) {
        console.error(`Error en ${contexto}:`, e);
        return { success: false, message: e.message };
    }
}

/**
 * Parser específico para Facturas
 * Columas esperadas: FOLIO, FECHA, MONTO, UUID, PROVEEDOR, REF_COMUNICADO
 */
function parseFacturaFile(csvText) {
    let cleanCsv = csvText;
    if (cleanCsv.charCodeAt(0) === 0xFEFF) cleanCsv = cleanCsv.slice(1);

    const rows = Utilities.parseCsv(cleanCsv);
    if (!rows || rows.length < 2) return [];

    const headers = rows[0].map(h => String(h).trim().toUpperCase());
    const dataRows = rows.slice(1);

    // Mapeo Indices
    const idxFolio = headers.findIndex(h => h.includes('FOLIO') && !h.includes('FISCAL'));
    const idxFecha = headers.findIndex(h => h.includes('FECHA'));
    const idxMonto = headers.findIndex(h => h === 'MONTO' || h === 'TOTAL' || h === 'IMPORTE');
    const idxUuid = headers.findIndex(h => h === 'UUID' || h === 'FOLIO FISCAL' || h === 'FOLIO_FISCAL');
    const idxProv = headers.findIndex(h => h === 'PROVEEDOR' || h === 'EMISOR' || h === 'RAZON SOCIAL');
    const idxRef = headers.findIndex(h => h === 'REF_COMUNICADO' || h === 'COMUNICADO' || h === 'REFERENCIA');
    const idxEstatus = headers.findIndex(h => h === 'ESTATUS' || h === 'ESTADO');

    if (idxUuid === -1 || idxMonto === -1) {
        throw new Error('Formato inválido: Se requieren columnas UUID y MONTO/TOTAL.');
    }

    return dataRows.map(r => {
        // Parsear fecha flexible
        let fecha = null;
        if (idxFecha > -1 && r[idxFecha]) {
            // Intentar parseo básico, o dejar string
            const val = r[idxFecha];
            // Si es string 'DD/MM/YYYY', convertir. Si es ISO, dejar.
            fecha = val;
        }

        return {
            folio: idxFolio > -1 ? String(r[idxFolio]).trim() : 'S/N',
            fecha: fecha,
            monto: idxMonto > -1 ? (parseNumeric(r[idxMonto]) || 0) : 0,
            uuid: idxUuid > -1 ? String(r[idxUuid]).trim() : null,
            proveedor: idxProv > -1 ? String(r[idxProv]).trim() : '',
            refComunicado: idxRef > -1 ? String(r[idxRef]).trim() : null,
            estatus: idxEstatus > -1 ? String(r[idxEstatus]).trim() : 'Por Validar'
        };
    }).filter(obj => obj.uuid); // Filtrar filas vacias
}
