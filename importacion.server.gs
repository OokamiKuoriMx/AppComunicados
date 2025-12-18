/**
 * ============================================================================
 * MÓDULO: IMPORTACIÓN INTELIGENTE (Las Octavas)
 * Descripción: Procesa archivos planos (CSV) para generar estructura relacional.
 * ============================================================================
 */

/**
 * PASO 7 (BACKEND): CONTROLADOR PRINCIPAL
 * Orquesta la importación completa.
 * @param {string} fileContent - Contenido de texto del archivo CSV.
 */
function ejecutarImportacion(fileContent) {
    const contexto = 'ejecutarImportacion';
    console.log(`[${contexto}] Iniciando procesamiento de archivo...`);

    try {
        // PASO 2: PARSER Y AGRUPADOR
        const loteAgrupado = parseImportFile(fileContent);
        console.log(`[${contexto}] Archivo parseado. Documentos identificados: ${loteAgrupado.length}`);

        // PASO 3: VALIDACIÓN DE NEGOCIO
        validarLote(loteAgrupado); // Ahora modifica doc.validacion in-place

        // Separar validos y omitidos
        // Validos: esValido=true AND status!='OMITIDO'
        const validos = loteAgrupado.filter(d => d.validacion.esValido && d.validacion.status !== 'OMITIDO');

        // Omitidos: esValido=false OR status=='OMITIDO'
        // Inicializamos lista con los que ya fallaron en validación
        const omitidos = loteAgrupado.filter(d => !d.validacion.esValido || d.validacion.status === 'OMITIDO');

        // Ordenar Validos: ORIGEN primero
        validos.sort((a, b) => {
            const tipoA = a.header.tipoRegistro;
            const tipoB = b.header.tipoRegistro;
            if (tipoA === 'ORIGEN' && tipoB !== 'ORIGEN') return -1;
            if (tipoA !== 'ORIGEN' && tipoB === 'ORIGEN') return 1;
            return 0;
        });

        // Filtrar solo los válidos para procesar (ya lo hicimos arriba en const validos)

        if (loteAgrupado.length === 0) {
            return {
                success: false,
                message: 'El archivo no contiene registros válidos o está vacío.'
            };
        }

        // Procesar persistencia
        let procesados = 0;
        let nuevosSiniestros = 0;

        validos.forEach(doc => {
            try {
                // PASO 4: MOTOR ALTA EXPRESS
                let ids = {};
                if (doc.header.tipoRegistro === 'ORIGEN') {
                    const entidades = _findOrCreateEntities(doc.header);
                    ids = entidades.ids;
                    if (entidades.nuevoSiniestro) nuevosSiniestros++;
                }

                // PASO 5: PERSISTENCIA COMUNICADOS Y ACTUALIZACIONES
                const resultadoPersistencia = _persistirDocumento(doc, ids);

                // PASO 6: PERSISTENCIA PRESUPUESTO
                if (resultadoPersistencia && resultadoPersistencia.idActualizacion) {
                    _guardarLineasPresupuesto(resultadoPersistencia.idActualizacion, doc.lineas);
                    procesados++;
                }
            } catch (err) {
                // Si falla persistencia individual, mover a omitidos
                console.error(`Error persistiendo ${doc.header.refCta}:`, err);
                omitidos.push({
                    header: doc.header,
                    validacion: { esValido: false, status: 'ERROR_PERSISTENCIA', motivo: err.message, errores: [err.message] }
                });
                // No incrementamos procesados
            }
        });

        // Generar CSV de errores si hay omitidos
        let csvErrorContent = null;
        if (omitidos.length > 0) {
            csvErrorContent = _generarCsvErrores(omitidos);
        }

        return {
            success: true,
            message: `Proceso completado.`,
            resumen: {
                totalDocumentos: loteAgrupado.length,
                procesados: procesados,
                nuevosSiniestros: nuevosSiniestros,
                omitidos: omitidos.length
            },
            csvErrorContent: csvErrorContent,
            detalles: loteAgrupado.map(d => ({
                ref: d.header.refCta,
                comunicado: d.header.comunicadoId,
                valido: d.validacion.esValido && d.validacion.status !== 'OMITIDO',
                errores: d.validacion.motivo ? [d.validacion.motivo] : d.validacion.errores
            }))
        };

    } catch (error) {
        console.error(`Error en ${contexto}:`, error);
        return { success: false, message: `Error fatal: ${error.message}` };
    }
}

/**
 * PASO 2: PARSER Y AGRUPADOR
 */
function parseImportFile(csvInfo) {
    // Asumimos que csvInfo es string CSV
    const rows = Utilities.parseCsv(csvInfo);
    if (!rows || rows.length < 2) return [];

    // Headers esperados: REF_CTA, COMUNICADO_ID, TIPO_REGISTRO, FECHA_DOC, ESTADO, REF_SINIESTRO, ASEGURADORA, FENOMENO, FECHA_SINIESTRO_FI, FONDO, TOTAL_DOC_PDF, CONCEPTO_OBRA, CATEGORIA, IMPORTE_RENGLON, MONTO_SUPERVISION
    // Mapeo de indices basado en la primera fila si es header, o fijo segun requerimiento.
    // Asumiremos la primera fila es header.
    const headers = rows[0].map(h => String(h).trim().toUpperCase());
    const dataRows = rows.slice(1);

    const getIdx = (name) => headers.indexOf(name);

    const idxRef = getIdx('REF_CTA');
    const idxCom = getIdx('COMUNICADO_ID');
    const idxTipo = getIdx('TIPO_REGISTRO');
    const idxFecha = getIdx('FECHA_DOC');
    const idxEstado = getIdx('ESTADO');
    const idxSinRef = getIdx('REF_SINIESTRO');
    const idxAseg = getIdx('ASEGURADORA');
    const idxFen = getIdx('FENOMENO');
    const idxFi = getIdx('FECHA_SINIESTRO_FI');
    const idxFondo = getIdx('FONDO');
    const idxTotal = getIdx('TOTAL_DOC_PDF');
    const idxConcepto = getIdx('CONCEPTO_OBRA');
    const idxCat = getIdx('CATEGORIA');
    const idxImporte = getIdx('IMPORTE_RENGLON');
    const idxSup = getIdx('MONTO_SUPERVISION');

    const agrupado = {};

    dataRows.forEach(row => {
        const refCta = String(row[idxRef] || '').trim().toUpperCase();
        const comId = String(row[idxCom] || '').trim().toUpperCase();

        if (!refCta || !comId) return; // Skip empty rows

        const key = `${refCta}|${comId}`;

        if (!agrupado[key]) {
            // Inicializar cabecera
            agrupado[key] = {
                header: {
                    refCta: refCta,
                    comunicadoId: comId,
                    tipoRegistro: String(row[idxTipo] || 'ACTUALIZACION').trim().toUpperCase(),
                    fechaDoc: row[idxFecha], // Debería ser YYYY-MM-DD o parseable
                    estado: String(row[idxEstado] || '').toUpperCase(),
                    refSiniestro: String(row[idxSinRef] || '').toUpperCase(),
                    aseguradora: String(row[idxAseg] || '').toUpperCase(),
                    fenomeno: String(row[idxFen] || '').toUpperCase(),
                    fechaSiniestroFi: row[idxFi],
                    fondo: String(row[idxFondo] || '').toUpperCase(),
                    totalPdf: parseNumeric(row[idxTotal]) || 0,
                    // Supervision puede venir repetido, tomamos el del parent o sumamos?
                    // Asumiremos que es un valor global del documento, tomamos el primero.
                    montoSupervision: parseNumeric(row[idxSup]) || 0
                },
                lineas: [],
                validacion: { sumaLineas: 0, esValido: true, errores: [] }
            };
        }

        // Agregar línea
        const importe = parseNumeric(row[idxImporte]) || 0;
        agrupado[key].lineas.push({
            concepto: String(row[idxConcepto] || ''),
            categoria: String(row[idxCat] || ''),
            importe: importe
        });

        agrupado[key].validacion.sumaLineas += importe;
    });

    return Object.values(agrupado);
}

/**
 * PASO 3: VALIDACIÓN DE NEGOCIO Y OMISIÓN
 */
function validarLote(loteAgrupado) {
    const cuentasResp = readAllRows('cuentas');
    // Para validar relaciones padre-hijo correctamente, necesitamos saber si el padre (comunicado) existe
    // no solo la cuenta.
    const comunicadosResp = readAllRows('comunicados');

    const cuentasExistentes = cuentasResp.success ? cuentasResp.data : [];
    const comunicadosExistentes = comunicadosResp.success ? comunicadosResp.data : [];

    loteAgrupado.forEach(doc => {

        // Reset status
        doc.validacion = {
            esValido: true,
            status: 'OK',
            motivo: null,
            errores: [],
            sumaLineas: doc.validacion.sumaLineas // Mantener la suma calculada en parser
        };

        // 1. Datos Incompletos
        if (!doc.header.refCta || !doc.header.comunicadoId) {
            doc.validacion.esValido = false;
            doc.validacion.status = 'OMITIDO';
            doc.validacion.motivo = 'Datos clave faltantes (Ref o Comunicado)';
            return;
        }

        if (!doc.header.totalPdf || doc.header.totalPdf <= 0) {
            doc.validacion.esValido = false;
            doc.validacion.status = 'OMITIDO';
            doc.validacion.motivo = 'Monto financiero inválido o faltante';
            return;
        }

        // Check Financiero
        const diff = Math.abs(doc.header.totalPdf - doc.validacion.sumaLineas);
        if (diff > 1) {
            doc.validacion.esValido = false;
            doc.validacion.status = 'OMITIDO';
            doc.validacion.motivo = `Descuadre: Header(${doc.header.totalPdf}) vs Lineas(${doc.validacion.sumaLineas})`;
            return;
        }

        // 2. Lógica de Relaciones
        // Buscar Cuenta
        const cuentaObj = cuentasExistentes.find(c => c.referencia === doc.header.refCta || c.cuenta === doc.header.refCta);
        const idCuenta = cuentaObj ? cuentaObj.id : null;

        if (doc.header.tipoRegistro === 'ACTUALIZACION') {
            // Regla: Debe existir el comunicado padre
            // Buscamos en memoria si existe el comunicado para esa cuenta y ese ID
            // Nota: idReferencia se necesita para buscar en comunicadosExistentes

            if (!idCuenta) {
                doc.validacion.esValido = false;
                doc.validacion.status = 'OMITIDO';
                doc.validacion.motivo = 'No existe la Referencia (Cuenta) para esta Actualización';
                return;
            }

            const existeComunicado = comunicadosExistentes.some(c =>
                String(c.idReferencia) === String(idCuenta) &&
                String(c.comunicado) === String(doc.header.comunicadoId)
            );

            // Tambien verificar si viene en el mismo lote como ORIGEN previo (el sort ya puso origenes antes)
            // Pero aqui estamos validando todo el lote.
            // Si el ORIGEN del mismo comunicado está en este lote y es válido, entonces esta actualizacion es válida.
            // Buscamos en el lote un doc con mismo ref/comunicado y tipo ORIGEN
            const origenEnLote = loteAgrupado.find(d =>
                d.header.refCta === doc.header.refCta &&
                d.header.comunicadoId === doc.header.comunicadoId &&
                d.header.tipoRegistro === 'ORIGEN' &&
                d.validacion.esValido // Asume validación secuencial o iterativa? forEach es secuencial.
                // Si el origen está mas abajo en la lista, aun no ha sido validado, pero 'esValido' init true.
            );

            if (!existeComunicado && !origenEnLote) {
                doc.validacion.esValido = false;
                doc.validacion.status = 'OMITIDO';
                doc.validacion.motivo = 'No existe el Comunicado Origen (Padre)';
                return;
            }
        }
        else if (doc.header.tipoRegistro === 'ORIGEN') {
            // Alta Express Flag
            // Si no existe cuenta, se marcará para crear, eso es valido.
            if (!idCuenta) {
                doc.validacion.esAltaExpress = true;
            }
        }
        else {
            doc.validacion.esValido = false;
            doc.validacion.status = 'OMITIDO';
            doc.validacion.motivo = `Tipo registro desconocido: ${doc.header.tipoRegistro}`;
        }
    });
}

/**
 * PASO 4: MOTOR ALTA EXPRESS
 */
function _findOrCreateEntities(header) {
    const ids = { idSiniestro: null, idReferencia: null, idAjustador: null };
    let nuevoSiniestro = false;

    // A) Siniestro
    if (header.refSiniestro) {
        const sinResult = ensureCatalogRecord('siniestros', {
            siniestro: header.refSiniestro,
            fenomeno: header.fenomeno,
            fi: header.fechaSiniestroFi,
            fondo: header.fondo,
            // idAseguradora: Se resolverá dentro si pasamos nombre
            // ensureCatalogRecord no hace búsqueda compleja de FK, asi que mejor resolvemos Aseguradora primero
        });

        // Mejor logica: Resolver Aseguradora primero
        let idAseguradora = null;
        if (header.aseguradora) {
            const asegRes = ensureCatalogRecord('aseguradoras', { descripción: header.aseguradora });
            if (asegRes.success) idAseguradora = asegRes.data.id;
        }

        // Ahora create/update siniestro
        // ensureCatalogRecord busca por el campo primario de nombre (siniestro)
        // Si existe devuelve ID, si no crea.
        // Si queremos actualizar campos complementarios (fenomeno, etc) si ya existe, ensureCatalog no siempre lo hace.
        // Asumiremos comportamiento estándar: Find or Create.
        const headerSiniestro = {
            siniestro: header.refSiniestro,
            fenomeno: header.fenomeno,
            fi: header.fechaSiniestroFi,
            fondo: header.fondo,
            idAseguradora: idAseguradora
        };
        const sinRes = ensureCatalogRecord('siniestros', headerSiniestro);
        if (sinRes.success) {
            ids.idSiniestro = sinRes.data.id;
            if (sinRes.created) nuevoSiniestro = true;
        }
    }

    // B) Referencia
    // Buscamos Referencia por nombre
    const searchRef = readAllRows('cuentas');
    // Nota: readAllRows es ineficiente en loop, pero validarLote ya leyó.
    // Optimizacion: Si esto fuera masivo, cachearía. Como es Apps Script, asumimos volumen moderado.

    // Default Ajustador ID ?
    // "Referencia: Inserta... vinculando al idAjustador Default Charles Taylor"
    // Buscamos Charles Taylor
    let idAjustadorDefault = null;
    const ajRes = readAllRows('ajustadores');
    if (ajRes.success) {
        const ct = ajRes.data.find(a => String(a.nombre || a.nombreAjustador).toUpperCase().includes('CHARLES'));
        idAjustadorDefault = ct ? ct.id : null;
    }

    let idCuenta = null;
    if (searchRef.success) {
        const found = searchRef.data.find(c => c.referencia === header.refCta || c.cuenta === header.refCta);
        if (found) {
            idCuenta = found.id;
        } else {
            // Crear Referencia
            const newRef = {
                referencia: header.refCta,
                cuenta: header.refCta,
                idAjustador: idAjustadorDefault,
                fechaAlta: new Date()
            };
            const createRef = createRow('cuentas', newRef);
            if (createRef.success) idCuenta = createRef.data.id;
        }
    }
    ids.idReferencia = idCuenta;

    return { ids, nuevoSiniestro };
}

/**
 * PASO 5: PERSISTENCIA COMUNICADOS Y ACTUALIZACIONES
 */
function _persistirDocumento(doc, ids) {
    const isOrigen = doc.header.tipoRegistro === 'ORIGEN';
    let idComunicado = null;

    // Buscar si el comunicado ya existe
    const allComs = readAllRows('comunicados');
    const existingCom = (allComs.success && allComs.data)
        ? allComs.data.find(c => String(c.idReferencia) === String(ids.idReferencia || _getIdReferencia(doc.header.refCta)) && c.comunicado === doc.header.comunicadoId)
        : null;

    if (isOrigen) {
        if (!existingCom) {
            // Crear Comunicado
            const newCom = {
                idReferencia: ids.idReferencia,
                comunicado: doc.header.comunicadoId,
                status: 1
            };
            const resCom = createRow('comunicados', newCom);
            if (!resCom.success) throw new Error('Error al crear comunicado ' + newCom.comunicado);
            idComunicado = resCom.data.id;

            // Crear Datos Generales
            // Necesitamos resolver IDs de Estado y Distrito
            const idEstado = _resolveId('estados', 'estado', doc.header.estado);
            //const idDistrito = ... (No viene distrito en CSV input? Supuesto: No viene o es implicito)

            const newDG = {
                idComunicado: idComunicado,
                descripcion: 'Importación Automática - ' + doc.header.tipoRegistro,
                fecha: doc.header.fechaDoc,
                idEstado: idEstado,
                idSiniestro: ids.idSiniestro,
                // idAjustador se hereda de la cuenta
            };
            createRow('datosGenerales', newDG);

        } else {
            idComunicado = existingCom.id;
        }
    } else {
        // ACTUALIZACION
        // Debe existir
        if (!existingCom) throw new Error('Comunicado no encontrado para Actualización: ' + doc.header.comunicadoId);
        idComunicado = existingCom.id;
    }

    // Insertar ACTUALIZACION (Header financiero)
    // Tabla 'actualizaciones': idComunicado, consecutivo, esOrigen, revision, monto...

    // Calcular consecutivo
    const allActs = readAllRows('actualizaciones');
    const actsPropias = (allActs.success && allActs.data)
        ? allActs.data.filter(a => String(a.idComunicado) === String(idComunicado))
        : [];

    const consecutivo = actsPropias.length + 1;

    const newAct = {
        idComunicado: idComunicado,
        consecutivo: consecutivo,
        esOrigen: isOrigen && consecutivo === 1 ? 1 : 0,
        revision: isOrigen ? 'Origen' : (doc.header.comunicadoId + ' (Imp)'),
        monto: doc.header.totalPdf, // Monto calculado
        montoCapturado: null,
        montoSupervisión: doc.header.montoSupervision,
        fecha: new Date()
    };

    const resAct = createRow('actualizaciones', newAct);
    if (!resAct.success) throw new Error('Error creando actualizacion');

    return { idActualizacion: resAct.data.id, idComunicado };
}

/**
 * PASO 6: PERSISTENCIA LINEAS PRESUPUESTO
 */
function _guardarLineasPresupuesto(idActualizacion, lineas) {
    // 1. Limpieza idempotente
    _syncChildTable('presupuestoLineas', 'idActualizacion', idActualizacion, []);

    // 2. Inserción
    const lineasMapped = lineas.map(l => ({
        idActualizacion: idActualizacion,
        descripcion: l.concepto,
        categoria: l.categoria,
        importe: l.importe,
        fechaCreacion: new Date()
    }));

    // Usamos createRow en loop o batch? _syncChildTable ya hace batch insert en su lógica interna (o loop)
    // Reutilizamos _syncChildTable para insertar
    _syncChildTable('presupuestoLineas', 'idActualizacion', idActualizacion, lineasMapped);
}

// Helpers
function _getIdReferencia(refName) {
    // Helper sucio si no tenemos el ID a mano en el flujo de Actualizacion
    const res = readAllRows('cuentas');
    if (res.success) {
        const c = res.data.find(x => x.referencia === refName || x.cuenta === refName);
        return c ? c.id : null;
    }
    return null;
}

function _resolveId(table, fieldName, value) {
    if (!value) return null;
    const res = readAllRows(table);
    if (!res.success) return null;
    const item = res.data.find(x => String(x[fieldName] || '').toUpperCase() === String(value).toUpperCase());
    return item ? item.id : null;
}

function parseNumeric(value) {
    if (value === null || value === undefined || value === '') return 0;
    // Eliminar $ y comas
    const clean = String(value).replace(/[$,]/g, '');
    const num = parseFloat(clean);
    return isNaN(num) ? 0 : num;
}

/**
 * Genera CSV de Errores
 */
function _generarCsvErrores(listaOmitidos) {
    // Encabezados
    const headers = ['REF_CTA', 'COMUNICADO_ID', 'TIPO', 'MOTIVO_ERROR'];
    let csvString = headers.join(',') + '\n';

    listaOmitidos.forEach(item => {
        const row = [
            `"${item.header.refCta || ''}"`,
            `"${item.header.comunicadoId || ''}"`,
            `"${item.header.tipoRegistro || ''}"`,
            `"${item.validacion.motivo || item.validacion.errores.join('; ') || 'Error Desconocido'}"`
        ];
        csvString += row.join(',') + '\n';
    });

    // Retornar base64 para facilitar descarga en frontend sin blobs raros
    return Utilities.base64Encode(csvString);
}
