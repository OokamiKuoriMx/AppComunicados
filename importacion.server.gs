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

            return {
                ref: h.refCta,
                comunicado: h.comunicadoId,
                tipo: h.tipoRegistro,
                fecha: h.fechaDoc ? new Date(h.fechaDoc).toISOString().split('T')[0] : '',
                monto: h.totalPdf,
                sumaLineas: v.sumaLineas,
                status: v.status, // OK, OMITIDO
                esValido: esValido,
                motivo: v.motivo || (v.errores ? v.errores.join(', ') : ''),
                analisis: analisis
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
function ejecutarImportacion(fileContent) {
    const contexto = 'ejecutarImportacion';
    console.log(`[${contexto}] Iniciando procesamiento Batch...`);

    try {
        // PASO 1: PARSER Y AGRUPADOR
        const loteAgrupado = parseImportFile(fileContent);

        // PASO 2: VALIDACIÓN DE NEGOCIO (In-Memory)
        // Nota: validarLote lee catalogos para validar.
        // Optimizacion: Leer catalogos UNA SOLA VEZ aqui y pasarlos.
        const cache = _loadCatalogsCache();
        validarLote(loteAgrupado, cache);

        // Separar validos y omitidos
        const validos = loteAgrupado.filter(d => d.validacion.esValido && d.validacion.status !== 'OMITIDO');
        const omitidos = loteAgrupado.filter(d => !d.validacion.esValido || d.validacion.status === 'OMITIDO');

        // Ordenar Validos para consistencia (Origen primero)
        validos.sort((a, b) => {
            const tipoA = a.header.tipoRegistro;
            const tipoB = b.header.tipoRegistro;
            if (tipoA === 'ORIGEN' && tipoB !== 'ORIGEN') return -1;
            if (tipoA !== 'ORIGEN' && tipoB === 'ORIGEN') return 1;
            return 0;
        });

        if (validos.length === 0) {
            return _buildResponse(false, 'No hay registros válidos.', { total: loteAgrupado.length, omitidos: omitidos.length }, omitidos, loteAgrupado);
        }

        // ==========================================================================================
        // FASE DE PERSISTENCIA BATCH (ORDEN ESTRICTO)
        // 1. Ajustadoras/Aseguradoras -> 2. Siniestros/Cuentas -> 3. Comunicados -> 
        // 4. Datos Generales -> 5. Actualizaciones -> 6. Presupuesto Lineas
        // ==========================================================================================

        const counts = { newAsegs: 0, newSins: 0, newCuentas: 0, newComs: 0, newLines: 0 };

        // --- 1. AJUSTADORAS (Default) & ASEGURADORAS ---
        // Identificar Aseguradoras únicas nuevas
        const newAseguradoras = _extractUnique(validos, 'aseguradora', cache.aseguradoras, 'descripción');
        if (newAseguradoras.length > 0) {
            const res = createBatch('aseguradoras', newAseguradoras.map(desc => ({ descripción: desc })));
            counts.newAsegs += res.count;
            _updateCache(cache, 'aseguradoras', res.ids, newAseguradoras, 'descripción');
        }

        // --- 1.5. DISTRITOS RIEGO & AJUSTADORES ---
        // Identificar Distritos únicos nuevos
        const newDistritos = _extractUnique(validos, 'distritoRiego', cache.distritosRiego, 'distritoRiego');
        if (newDistritos.length > 0) {
            const res = createBatch('distritosRiego', newDistritos.map(d => ({ distritoRiego: d })));
            _updateCache(cache, 'distritosRiego', res.ids, newDistritos, 'distritoRiego');
        }

        // Identificar Ajustadores únicos nuevos (si vienen en archivo)
        const newAjustadores = _extractUnique(validos, 'ajustador', cache.ajustadores, 'nombreAjustador');
        if (newAjustadores.length > 0) {
            // Validar que no esten vacios
            const validNewAjustadores = newAjustadores.filter(a => a && a.length > 2); // Simple validation
            if (validNewAjustadores.length > 0) {
                const res = createBatch('ajustadores', validNewAjustadores.map(a => ({ nombreAjustador: a, nombre: a })));
                _updateCache(cache, 'ajustadores', res.ids, validNewAjustadores, 'nombreAjustador');
            }
        }


        // --- 2. SINIESTROS & CUENTAS ---
        // 2a. Siniestros (requiere idAseguradora del paso 1)
        const siniestrosMap = _prepareSiniestrosBatch(validos, cache);
        if (siniestrosMap.inserts.length > 0) {
            const res = createBatch('siniestros', siniestrosMap.inserts);
            counts.newSins += res.count;
            _updateCache(cache, 'siniestros', res.ids, siniestrosMap.keys, 'siniestro');
        }

        // 2b. Cuentas (Referencias)
        // Ajustador Default (Charles Taylor) - Se usa de Fallback
        const idAjustadorDefault = _findIdAjustadorDefault(cache.ajustadores);

        const cuentasMap = _prepareCuentasBatch(validos, cache, idAjustadorDefault);
        if (cuentasMap.inserts.length > 0) {
            const res = createBatch('cuentas', cuentasMap.inserts);
            counts.newCuentas += res.count;
            _updateCache(cache, 'cuentas', res.ids, cuentasMap.keys, ['referencia', 'cuenta']);
        }

        // --- 3. COMUNICADOS (Main Parent) ---
        // Aqui empieza la construccion relacional.
        // Necesitamos arrays para cada tabla
        const batchComunicados = [];
        const batchDatosGenerales = [];
        const batchActualizaciones = [];
        const batchPresupuestos = [];

        // Mapeo temporal para saber qué doc corresponde a qué índice de batchComunicados
        // para luego asignar ID real.
        const comsToInsertMap = []; // { docIndex, data }

        // Recorrido para construir objetos en memoria
        validos.forEach((doc, idx) => {
            const isOrigen = doc.header.tipoRegistro === 'ORIGEN';

            // Resolver IDs usando cache actualizado
            const idReferencia = _resolveIdFromCache(cache.cuentas, doc.header.refCta, ['referencia', 'cuenta']);
            const idSiniestro = _resolveIdFromCache(cache.siniestros, doc.header.refSiniestro, 'siniestro');
            const idEstado = _resolveIdFromCache(cache.estados, doc.header.estado, 'estado'); // Asumiendo estados estáticos

            // Lógica Comunicado: Buscar si existe o es nuevo
            let idComunicado = null;
            // Buscar en cache (incluyendo los que acabamos de "decidir" crear en este loop? No, porque aun no tienen ID)
            // Problema: Si en el lote vienen 2 docs para el mismo NUEVO comunicado.
            // Solucion: Cache local de "Comunicados creados en este lote"

            const existingCom = cache.comunicados.find(c =>
                String(c.idReferencia) === String(idReferencia) &&
                String(c.comunicado) === String(doc.header.comunicadoId)
            );

            if (isOrigen && !existingCom) {
                // ES NUEVO COMUNICADO
                // Check if we already queued it in this very batch
                let queuedIdx = comsToInsertMap.findIndex(q =>
                    String(q.data.idReferencia) === String(idReferencia) &&
                    String(q.data.comunicado) === String(doc.header.comunicadoId)
                );

                if (queuedIdx === -1) {
                    // Queue for creation
                    const newComData = {
                        idReferencia: idReferencia,
                        comunicado: doc.header.comunicadoId,
                        status: 1
                    };
                    batchComunicados.push(newComData);
                    comsToInsertMap.push({
                        docRefs: [doc], // Guardamos ref al doc para luego inyectar ID
                        data: newComData,
                        isNew: true
                    });
                } else {
                    // Ya está en cola (caso raro: 2 origenes iguales?), agregamos ref
                    comsToInsertMap[queuedIdx].docRefs.push(doc);
                }
            } else {
                // YA EXISTE O ES ACTUALIZACION
                let tempId = null;

                if (existingCom) {
                    tempId = existingCom.id;
                } else {
                    // Si no existe en BD, verificar si lo estamos creando en ESTE batch (Dependencia Lote)
                    const queuedItem = comsToInsertMap.find(q =>
                        String(q.data.idReferencia) === String(idReferencia) &&
                        String(q.data.comunicado) === String(doc.header.comunicadoId)
                    );

                    if (queuedItem) {
                        // VINCULACIÓN EN LOTE:
                        // Aun no tenemos ID real, pero lo agregamos a la lista de docs que esperan ese ID.
                        queuedItem.docRefs.push(doc);
                        // No asignamos _tempIdComunicado aun, se asignará cuando se cree el batch.
                        return; // OJO: Salimos, la inyección de ID ocurrirá mas abajo (Line 266)
                    } else {
                        // Error real: No existe en BD y no se está creando.
                        _markError(doc, omitidos, "Error Lógico: Comunicado padre no encontrado para Actualización");
                        return;
                    }
                }

                // Si encontramos existente directo:
                doc._tempIdComunicado = tempId;
            }
        });

        // INSERTAR COMUNICADOS BATCH
        if (batchComunicados.length > 0) {
            const resComs = createBatch('comunicados', batchComunicados);
            counts.newComs += resComs.count;

            // Asignar IDs reales a los docs
            resComs.ids.forEach((realId, i) => {
                const queuedItem = comsToInsertMap[i]; // batchComunicados[i]
                queuedItem.docRefs.forEach(d => d._tempIdComunicado = realId); // Inyectar ID

                // Actualizar cache local por si acaso
                cache.comunicados.push({ id: realId, ...queuedItem.data });
            });
        }

        // --- 4, 5, 6. HIJOS (Datos Generales, Actualizaciones, Presupuestos) ---
        // Ahora que todos los validos tienen `_tempIdComunicado`, construimos el resto

        validos.forEach(doc => {
            if (!doc._tempIdComunicado) return; // Si falló algo arriba

            const idComunicado = doc._tempIdComunicado;
            const isOrigen = doc.header.tipoRegistro === 'ORIGEN';

            // 4. Datos Generales (Solo Origen)
            // Verificar si YA existe DG para este comunicado? 
            // Si es origen nuevo, seguro no. Si es origen existente (reproceso), quiza duplicamos?
            // createBatch es ciego. Asumiremos que si es ORIGEN en el CSV, queremos insertar DG.
            // Ojo: Si el comunicado ya existía, quizas NO queremos duplicar DG.
            // Regla: Insertar DG solo si acabamos de crear el comunicado (newComs).
            // O si explicitamente permitimos Multiples DG (no es usual).
            // Verificamos en cache.datosGenerales si ya existe para este comunicado.

            const existeDG = cache.datosGenerales.some(dg => String(dg.idComunicado) === String(idComunicado));

            if (isOrigen && !existeDG) {
                const idEstado = _resolveIdFromCache(cache.estados, doc.header.estado, 'estado');
                const idSiniestro = _resolveIdFromCache(cache.siniestros, doc.header.refSiniestro, 'siniestro');
                const idDR = _resolveIdFromCache(cache.distritosRiego, doc.header.distritoRiego, 'distritoRiego');

                // Resolver Ajustador para DG: Prioridad File -> Default
                // Nota: Cuentas ya tiene el Link, pero DG tambien lo pide segun esquema.
                // Es redundante pero lo llenamos si podemos.
                let idAjustador = _resolveIdFromCache(cache.ajustadores, doc.header.ajustador, ['nombreAjustador', 'nombre']);
                if (!idAjustador) idAjustador = _findIdAjustadorDefault(cache.ajustadores);

                batchDatosGenerales.push({
                    idComunicado: idComunicado,
                    descripcion: doc.header.descripcion || `${doc.header.refCta}-${doc.header.comunicadoId}`, // Prioridad a columna Descripcion
                    fecha: doc.header.fechaDoc,
                    idEstado: idEstado,
                    idSiniestro: idSiniestro,
                    idDR: idDR,
                    idAjustador: idAjustador
                });

                // Hack: update cache local to prevent double insert if batch has dupes
                cache.datosGenerales.push({ idComunicado: idComunicado });
            }

            // REGLA: Si viene una descripcion EXPLICITA en cualquier renglón (incluso actualización),
            // actualizamos el registro de Datos Generales que acabamos de preparar (si existe).
            // Esto permite que el registro L30A "substituya" la descripcion del L30.
            if (doc.header.descripcion) {
                const pendingDG = batchDatosGenerales.find(dg => String(dg.idComunicado) === String(idComunicado));
                if (pendingDG) {
                    pendingDG.descripcion = doc.header.descripcion;
                }
            }

            // 5. Actualizaciones
            // Calcular consecutivo. Leemos todas las actualizaciones de este comunicado
            const actsPrevias = cache.actualizaciones.filter(a => String(a.idComunicado) === String(idComunicado));
            // Cuantas estamos agregando en ESTE lote para el mismo comunicado?
            const updatesInBatch = batchActualizaciones.filter(a => String(a.idComunicado) === String(idComunicado));

            const consecutivo = actsPrevias.length + updatesInBatch.length + 1;

            // Necesitamos guardar referencia al objeto update para luego asignarle ID a sus lineas
            const newUpdateObj = {
                idComunicado: idComunicado,
                consecutivo: consecutivo,
                esOrigen: isOrigen && consecutivo === 1 ? 1 : 0,
                revision: isOrigen ? 'Origen' : (doc.header.comunicadoId + ' (Imp)'),
                monto: doc.header.totalPdf,
                montoCapturado: null,
                montoSupervisión: (doc.header.totalPdf || 0) * 0.05, // Regla: 5% del Monto (Total PDF)
                fecha: new Date(),
                // Meta-data para vincular lineas despues
                _docLineas: doc.lineas
            };
            batchActualizaciones.push(newUpdateObj);
        });

        // INSERTAR DATOS GENERALES
        if (batchDatosGenerales.length > 0) {
            createBatch('datosGenerales', batchDatosGenerales);
        }

        // INSERTAR ACTUALIZACIONES
        if (batchActualizaciones.length > 0) {
            const resActs = createBatch('actualizaciones', batchActualizaciones);
            // counts.newActs += resActs.count;

            // --- 6. PRESUPUESTO LINEAS ---
            // Usamos los IDs generados de actualizaciones para crear las lineas
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
        }

        // INSERTAR PRESUPUESTOS
        if (batchPresupuestos.length > 0) {
            const resLines = createBatch('presupuestoLineas', batchPresupuestos);
            counts.newLines = resLines.count;
        }

        // FIN DEL PROCESO
        console.log(`[${contexto}] Batch Completed.`, counts);

        // Generar CSV Errores
        let csvErrorContent = null;
        if (omitidos.length > 0) {
            csvErrorContent = _generarCsvErrores(omitidos);
        }

        return _buildResponse(true, 'Importación Batch Completada.', counts, omitidos, loteAgrupado, csvErrorContent);

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
        if (Array.isArray(keyField)) {
            // Caso compuesto (Cuentas: referencia, cuenta)
            keyField.forEach(k => item[k] = val); // val deberia ser obj? No, simplifiquemos
            // Para cuentas, _prepareCuentasBatch devuelve keys como los objetos a insertar (menos id).
            // Ajustamos lógica abajo:
        } else {
            item[keyField] = val;
        }

        // Si originalKeys son los objetos completos, mejor:
        if (typeof val === 'object') {
            Object.assign(item, val);
        } else {
            item[keyField] = val;
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

function _buildResponse(success, msg, counts, omitidos, allDocs, csvContent) {
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
    const idxEstado = headers.indexOf('ESTADO');
    const idxSinRef = headers.indexOf('REF_SINIESTRO');
    const idxAseg = headers.indexOf('ASEGURADORA');
    const idxFen = headers.indexOf('FENOMENO');
    const idxFi = headers.indexOf('FECHA_SINIESTRO_FI');
    const idxFondo = headers.indexOf('FONDO');
    const idxDistrito = headers.indexOf('DISTRITO_RIEGO'); // Nuevo
    const idxAjustador = headers.indexOf('AJUSTADOR'); // Nuevo
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
        const comId = String(row[idxCom] || '').trim().toUpperCase();
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
