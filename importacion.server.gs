/**
 * ============================================================================
 * MÓDULO: IMPORTACIÓN INTELIGENTE (Versión Batch - High Performance)
 * Descripción: Procesa archivos planos (CSV) para generar estructura relacional.
 * Optimizado para leer una vez y escribir en lotes ordenados.
 * ============================================================================
 */

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

        // --- 2. SINIESTROS & CUENTAS ---
        // 2a. Siniestros (requiere idAseguradora del paso 1)
        const siniestrosMap = _prepareSiniestrosBatch(validos, cache);
        if (siniestrosMap.inserts.length > 0) {
            const res = createBatch('siniestros', siniestrosMap.inserts);
            counts.newSins += res.count;
            _updateCache(cache, 'siniestros', res.ids, siniestrosMap.keys, 'siniestro');
        }

        // 2b. Cuentas (Referencias)
        // Ajustador Default (Charles Taylor)
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
                if (!existingCom) {
                    // Error en Actualizacion sin padre. Debería haber fallado en validación, pero por seguridad:
                    _markError(doc, omitidos, "Error Lógico: Comunicado padre no encontrado para Actualización");
                    return;
                }
                // Usamos el existente
                // Pero necesitamos agregarlo a un mapa para procesar sus hijos (DG, Act) 
                // simulando que ya tenemos su ID
                doc._tempIdComunicado = existingCom.id;
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

                batchDatosGenerales.push({
                    idComunicado: idComunicado,
                    descripcion: 'Importación Auto - ' + doc.header.tipoRegistro,
                    fecha: doc.header.fechaDoc,
                    idEstado: idEstado,
                    idSiniestro: idSiniestro
                });

                // Hack: update cache local to prevent double insert if batch has dupes
                cache.datosGenerales.push({ idComunicado: idComunicado });
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
                montoSupervisión: doc.header.montoSupervision,
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
            const idAseg = _resolveIdFromCache(cache.aseguradoras, h.aseguradora, 'descripción');

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
            const newCta = {
                referencia: ref,
                cuenta: ref, // Duplicamos valor por diseño original
                idAjustador: idAjustadorDefault,
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
    return {
        success: success,
        message: msg,
        resumen: {
            totalDocumentos: allDocs.length,
            procesados: counts.newComs + counts.newLines, // Proxies
            detallesTecnicos: counts,
            omitidos: omitidos.length
        },
        csvErrorContent: csvContent,
        detalles: allDocs.map(d => ({
            ref: d.header.refCta,
            comunicado: d.header.comunicadoId,
            valido: d.validacion.esValido && d.validacion.status !== 'OMITIDO',
            errores: d.validacion.motivo ? [d.validacion.motivo] : d.validacion.errores
        }))
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
        data.forEach(row => {
            const csvRow = row.map(cell => {
                let s = String(cell || '');
                if (s.includes(',') || s.includes('"') || s.includes('\n')) s = '"' + s.replace(/"/g, '""') + '"';
                return s;
            }).join(',');
            csvContent += csvRow + '\n';
        });
        return { success: true, csvContent: csvContent };

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
    const idxCom = headers.indexOf('COMUNICADO_ID');
    const idxTipo = headers.indexOf('TIPO_REGISTRO');
    const idxFecha = headers.indexOf('FECHA_DOC');
    const idxEstado = headers.indexOf('ESTADO');
    const idxSinRef = headers.indexOf('REF_SINIESTRO');
    const idxAseg = headers.indexOf('ASEGURADORA');
    const idxFen = headers.indexOf('FENOMENO');
    const idxFi = headers.indexOf('FECHA_SINIESTRO_FI');
    const idxFondo = headers.indexOf('FONDO');
    const idxTotal = headers.indexOf('TOTAL_DOC_PDF');
    const idxConcepto = headers.indexOf('CONCEPTO_OBRA');
    const idxCat = headers.indexOf('CATEGORIA');
    const idxImporte = headers.indexOf('IMPORTE_RENGLON');
    const idxSup = headers.indexOf('MONTO_SUPERVISION');

    if (idxRef === -1 || idxCom === -1) throw new Error('Faltan columnas REF_CTA o COMUNICADO_ID');

    const agrupado = {};

    dataRows.forEach(row => {
        const refCta = String(row[idxRef] || '').trim().toUpperCase();
        const comId = String(row[idxCom] || '').trim().toUpperCase();
        if (!refCta || !comId) return;

        const key = `${refCta}|${comId}`;
        if (!agrupado[key]) {
            agrupado[key] = {
                header: {
                    refCta: refCta,
                    comunicadoId: comId,
                    tipoRegistro: idxTipo > -1 ? String(row[idxTipo] || 'ACTUALIZACION').trim().toUpperCase() : 'ACTUALIZACION',
                    fechaDoc: idxFecha > -1 ? row[idxFecha] : null,
                    estado: idxEstado > -1 ? String(row[idxEstado] || '').toUpperCase() : '',
                    refSiniestro: idxSinRef > -1 ? String(row[idxSinRef] || '').toUpperCase() : '',
                    aseguradora: idxAseg > -1 ? String(row[idxAseg] || '').toUpperCase() : '',
                    fenomeno: idxFen > -1 ? String(row[idxFen] || '').toUpperCase() : '',
                    fechaSiniestroFi: idxFi > -1 ? row[idxFi] : null,
                    fondo: idxFondo > -1 ? String(row[idxFondo] || '').toUpperCase() : '',
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
        if (diff > 1) {
            doc.validacion.esValido = false; doc.validacion.status = 'OMITIDO'; doc.validacion.motivo = `Descuadre: ${doc.header.totalPdf} vs ${doc.validacion.sumaLineas}`; return;
        }

        const cuentaObj = cuentasExistentes.find(c => c.referencia === doc.header.refCta || c.cuenta === doc.header.refCta);
        const idCuenta = cuentaObj ? cuentaObj.id : null;

        if (doc.header.tipoRegistro === 'ACTUALIZACION') {
            if (!idCuenta) {
                doc.validacion.esValido = false; doc.validacion.status = 'OMITIDO'; doc.validacion.motivo = 'No existe Referencia (Cuenta)'; return;
            }
            const existeComunicado = comunicadosExistentes.some(c => String(c.idReferencia) === String(idCuenta) && String(c.comunicado) === String(doc.header.comunicadoId));
            const origenEnLote = loteAgrupado.find(d => d.header.refCta === doc.header.refCta && d.header.comunicadoId === doc.header.comunicadoId && d.header.tipoRegistro === 'ORIGEN' && d.validacion.esValido);

            if (!existeComunicado && !origenEnLote) {
                doc.validacion.esValido = false; doc.validacion.status = 'OMITIDO'; doc.validacion.motivo = 'No existe Comunicado Origen'; return;
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
