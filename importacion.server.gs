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

        // 2. SIMULAR VALIDACIÓN (Incluye Auto-Corrección)
        validarLote(loteAgrupado, cache);

        // 3. ENRIQUECER PARA VISTA PREVIA
        const previewData = loteAgrupado.map(doc => {
            const h = doc.header;
            const v = doc.validacion;
            const esValido = v.esValido && v.status !== 'OMITIDO';

            // Detección de "Nuevos" Catálogos
            const analisis = {
                cuenta: _checkStatus(cache.cuentas, ['referencia', 'cuenta'], h.refCta),
                siniestro: _checkStatus(cache.siniestros, 'siniestro', h.refSiniestro),
                ajustador: _checkStatus(cache.ajustadores, ['nombreAjustador', 'nombre'], h.ajustador),
                distrito: _checkStatus(cache.distritosRiego, 'distritoRiego', h.distritoRiego),
                aseguradora: _checkStatus(cache.aseguradoras, 'descripción', h.aseguradora)
            };

            // Enhanced Comunicado Analysis
            let statusCom = 'NUEVO';
            let changes = [];
            let existingCom = null;
            let dgActual = null;
            let resEstadoId = null;

            // 1. Find Account ID
            const cta = cache.cuentas.find(c => c.referencia === h.refCta || c.cuenta === h.refCta);
            if (cta) {
                // 2. Check strict existence in DB
                existingCom = cache.comunicados.find(c =>
                    String(c.idReferencia) === String(cta.id) &&
                    String(c.comunicado) === String(h.comunicadoId)
                );

                if (existingCom) {
                    // 3. Deep Compare for Smart Update
                    dgActual = cache.datosGenerales.find(dg => String(dg.idComunicado) === String(existingCom.id));

                    if (!dgActual) {
                        statusCom = 'REEMPLAZAR'; // Existe Com pero no DG (Raro, pero forzamos update/insert DG)
                    } else {
                        // Comparacion profunda de campos clave
                        let hasChanges = false;

                        // Descripcion
                        if (h.descripcion && normalizarTexto(h.descripcion) !== normalizarTexto(dgActual.descripcion)) {
                            hasChanges = true;
                            changes.push('Descripción');
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

                        statusCom = hasChanges ? 'REEMPLAZAR' : 'OMITIDO';
                    }
                }
            }

            // DEBUG: Inject diagnostics
            const debugInfo = {
                csvEstado: h.estado,
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
            }

            return {
                ref: h.refCta,
                comunicado: h.comunicadoId,
                tipo: h.tipoRegistro,
                fecha: h.fechaDoc ? new Date(h.fechaDoc).toISOString().split('T')[0] : '',
                monto: h.totalPdf,
                sumaLineas: v.sumaLineas,
                status: finalStatus, // OK, OMITIDO
                esValido: esValido, // Keep valid so it counts as "processable" but omitted
                motivo: finalMotivo,
                analisis: analisis,
                rawPayload: { header: h, lineas: doc.lineas } // Data for single import
            };
        });

        const resumen = {
            total: previewData.length,
            // Validos para importar (Válidos y NO Omitidos)
            validos: previewData.filter(d => d.esValido && d.status !== 'OMITIDO').length,
            // Omitidos (Válidos pero sin cambios)
            omitidos: previewData.filter(d => d.esValido && d.status === 'OMITIDO').length,
            // Errores (No válidos)
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

            // Buscar si el comunicado ya existe
            const existingCom = cache.comunicados.find(c =>
                String(c.idReferencia) === String(idReferencia) &&
                String(c.comunicado) === String(doc.header.comunicadoId)
            );

            if (existingCom) {
                doc._isNew = false;
                doc._existingComId = existingCom.id;
                docsParaActualizar.push(doc);
            } else {
                doc._isNew = true;
                docsParaCrear.push(doc);
            }
        });

        logBatch(`[${contexto}] Clasificación: ${docsParaCrear.length} NUEVOS, ${docsParaActualizar.length} EXISTENTES`);

        // ==========================================================================================
        // FASE 1: CREAR CATÁLOGOS AUXILIARES (Aseguradoras, Distritos, Ajustadores, Siniestros, Cuentas)
        // ==========================================================================================
        logBatch(`[${contexto}] FASE 1: Creando catálogos auxiliares...`);

        const counts = { newAsegs: 0, newSins: 0, newCuentas: 0, newComs: 0, newDG: 0, newActs: 0, newLines: 0, updatedDG: 0 };

        // Aseguradoras
        const newAseguradoras = _extractUnique(validos, 'aseguradora', cache.aseguradoras, 'descripción');
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
        const newAjustadores = _extractUnique(validos, 'ajustador', cache.ajustadores, 'nombreAjustador');
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

                    // 5. Descripción: SIEMPRE actualizar con la nueva (el archivo ya trae la descripción correcta)
                    if (doc.header.descripcion) {
                        const descNueva = String(doc.header.descripcion).trim();
                        const descExistente = String(existingDG.descripcion || '').trim();

                        logBatch(`[${contexto}] DEBUG Descripción - CSV: "${descNueva}" | DB: "${descExistente}"`);
                        logBatch(`[${contexto}] DEBUG Normalizado - CSV: "${normalizarTexto(descNueva)}" | DB: "${normalizarTexto(descExistente)}"`);

                        // Solo actualizar si es diferente
                        if (normalizarTexto(descNueva) !== normalizarTexto(descExistente)) {
                            updates.descripcion = descNueva;
                            doUpdate = true;
                            logBatch(`[${contexto}] -> Descripción ACTUALIZADA: "${descExistente}" -> "${descNueva}"`);
                        } else {
                            logBatch(`[${contexto}] -> Descripción SIN CAMBIOS (idéntica normalizada)`);
                        }
                    } else {
                        logBatch(`[${contexto}] -> SIN descripción en header del documento`);
                    }

                    if (doUpdate) {
                        logBatch(`[${contexto}] -> Ejecutando UPDATE para DG ID ${existingDG.id}: ${JSON.stringify(updates)}`);
                        try {
                            const resUpd = updateRow('datosGenerales', existingDG.id, updates);
                            if (resUpd.success) {
                                counts.updatedDG++;
                                logBatch(`[${contexto}] -> UPDATE exitoso para DG ID ${existingDG.id}`);
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
                const actsPrevias = cache.actualizaciones.filter(a => String(a.idComunicado) === String(idComunicado));
                const actsEnBatch = batchActualizaciones.filter(a => String(a.idComunicado) === String(idComunicado));
                const consecutivo = actsPrevias.length + actsEnBatch.length + 1;

                logBatch(`[${contexto}] FASE 3: Creando Actualizacion #${consecutivo} para ComID existente ${idComunicado}`);

                batchActualizaciones.push({
                    idComunicado: idComunicado,
                    consecutivo: consecutivo,
                    esOrigen: 0, // Ya existe, no puede ser origen
                    revision: doc.header.tipoRegistro || 'Actualización',
                    monto: doc.header.totalPdf,
                    montoCapturado: null,
                    montoSupervisión: (doc.header.totalPdf || 0) * 0.05,
                    fecha: new Date(),
                    _docLineas: doc.lineas
                });
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
