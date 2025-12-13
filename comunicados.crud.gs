/**
 * ============================================================================
 * ARCHIVO: comunicados.crud.gs
 * Descripción: Operaciones CRUD para Comunicados
 * Versión: 2.5 (FINAL - Duplicates Removed & Sync Forced)
 * ============================================================================
 */

/**
 * === CATALOGO DE COMUNICADOS ===
 * Obtiene catálogos necesarios para crear comunicados
 * @return {Object} {success, data: {estados, distritosRiego, siniestros}}
 */
function fetchComunicadoCatalogs() {
    const debugLog = [];
    const log = (msg) => {
        console.log(msg);
        debugLog.push(String(msg));
    };

    try {
        log('Iniciando fetchComunicadoCatalogs...');

        // 1. Verificar hojas disponibles en el libro
        const ss = SpreadsheetApp.getActive();
        const sheets = ss.getSheets();
        const sheetNames = sheets.map(s => s.getName());
        log(`Hojas disponibles en el libro (${sheetNames.length}): ${sheetNames.join(', ')}`);

        // 2. Función auxiliar para leer y diagnosticar
        const leerCatalogo = (key) => {
            const def = TABLE_DEFINITIONS[key];
            if (!def) {
                log(`Error: No hay definición para la tabla '${key}'`);
                return [];
            }
            const nombreHoja = def.sheetName;
            log(`Leyendo catálogo '${key}' desde hoja '${nombreHoja}'...`);

            if (!sheetNames.includes(nombreHoja)) {
                log(`CRÍTICO: La hoja '${nombreHoja}' NO existe exactamente. Buscando coincidencias...`);
                const match = sheetNames.find(n => n.toLowerCase() === nombreHoja.toLowerCase());
                if (match) log(`-> Encontrada hoja similar: '${match}' (diferencia de mayúsculas/minúsculas)`);
                else log(`-> No se encontró ninguna hoja similar a '${nombreHoja}'`);
            }

            const response = readAllRows(key);
            if (!response.success) {
                log(`Error en readAllRows('${key}'): ${response.message}`);
                return [];
            }

            const data = response.data || [];
            log(`Éxito leyendo '${key}': ${data.length} registros encontrados.`);

            if (data.length > 0) {
                log(`Ejemplo primer registro '${key}': ${JSON.stringify(data[0])}`);
            } else {
                // Diagnóstico profundo si está vacío
                const rawSheet = ss.getSheetByName(nombreHoja);
                if (rawSheet) {
                    const lastRow = rawSheet.getLastRow();
                    log(`Diagnóstico '${nombreHoja}': LastRow=${lastRow}`);
                    if (lastRow > 0) {
                        const headers = rawSheet.getRange(1, 1, 1, rawSheet.getLastColumn()).getValues()[0];
                        log(`Headers en '${nombreHoja}': ${JSON.stringify(headers)}`);
                    }
                }
            }
            return data;
        };

        // 3. Leer los catálogos
        const estados = leerCatalogo('estados');
        const distritos = leerCatalogo('distritosRiego');
        const siniestros = leerCatalogo('siniestros');
        const ajustadores = leerCatalogo('ajustadores');

        // 4. Procesar datos (ordenar)
        const processResponse = (data) => {
            if (Array.isArray(data)) {
                return data.sort((a, b) => {
                    const nombreA = String(a.nombre || a.nombreAjustador || a.estado || a.distritoRiego || a.siniestro || '');
                    const nombreB = String(b.nombre || b.nombreAjustador || b.estado || b.distritoRiego || b.siniestro || '');
                    return nombreA.localeCompare(nombreB, 'es', { sensitivity: 'base' });
                });
            }
            return [];
        };

        return {
            success: true,
            data: {
                estados: processResponse(estados),
                distritosRiego: processResponse(distritos),
                siniestros: processResponse(siniestros),
                ajustadores: processResponse(ajustadores),
                debugLogs: debugLog // Devolver logs dentro de data para sobrevivir al unwrap
            }
        };

    } catch (error) {
        log(`Excepción en fetchComunicadoCatalogs: ${error.message}`);
        console.error(error);
        return {
            success: false,
            message: `Error al obtener catálogos: ${error.message}`,
            debugLogs: debugLog
        };
    }
}

/**
 * Alias para compatibilidad con versiones cacheadas del cliente
 */
function getComunicadoCatalogs() {
    return fetchComunicadoCatalogs();
}

/**
 * === CREAR COMUNICADO ===
 * Realiza validaciones y genera registros relacionados para un nuevo comunicado
 */
function createComunicado(data) {
    const contexto = 'createComunicado';
    try {
        const comunicadoNombre = String(data?.comunicado || data?.label || '').trim();
        if (!comunicadoNombre) {
            return crearRespuestaError('El nombre del comunicado es obligatorio', { source: contexto });
        }
        if (comunicadoNombre.length > 15) {
            return crearRespuestaError('El comunicado no puede exceder 15 caracteres', { source: contexto });
        }

        const cuentaId = String(data?.idCuenta || '').trim();
        if (!cuentaId) {
            return crearRespuestaError('Se requiere la cuenta asociada', { source: contexto });
        }

        const distritoNombre = String(data?.distrito || '').trim();
        if (!distritoNombre) {
            return crearRespuestaError('El distrito de riego es obligatorio', { source: contexto });
        }

        const siniestroNombre = String(data?.siniestro || '').trim();
        if (!siniestroNombre) {
            return crearRespuestaError('El siniestro es obligatorio', { source: contexto });
        }

        const fecha = String(data?.fecha || '').trim();
        if (!fecha) {
            return crearRespuestaError('La fecha del comunicado es obligatoria', { source: contexto });
        }

        const estadoId = String(data?.estadoId || data?.estado || '').trim();
        if (!estadoId) {
            return crearRespuestaError('Selecciona un estado válido', { source: contexto });
        }

        const cuentaResult = buscarPorId('cuentas', cuentaId);
        if (!cuentaResult.success) {
            return propagarRespuestaError(contexto, cuentaResult);
        }
        const cuenta = cuentaResult.data;

        const comunicadosResponse = readAllRows('comunicados');
        if (!comunicadosResponse.success) {
            return propagarRespuestaError(contexto, comunicadosResponse, { message: `No fue posible validar comunicados existentes: ${comunicadosResponse.message}` });
        }
        const comunicadosExistentes = comunicadosResponse.data || [];
        const duplicado = comunicadosExistentes.find(c =>
            normalizarClave(c.comunicado) === normalizarClave(comunicadoNombre) &&
            String(c.idCuenta) === cuentaId
        );

        if (duplicado) {
            return crearRespuestaError(`Ya existe un comunicado "${comunicadoNombre}" para esta cuenta`, { source: contexto });
        }

        const distritoResult = ensureCatalogRecord('distritosRiego', { distritoRiego: distritoNombre });
        if (!distritoResult.success) {
            return propagarRespuestaError(contexto, distritoResult);
        }

        const siniestroResult = ensureCatalogRecord('siniestros', {
            siniestro: siniestroNombre,
            fenomeno: data?.fenomeno || '',
            fondo: data?.fondo || '',
            fi: data?.fi || ''
        });
        if (!siniestroResult.success) {
            return propagarRespuestaError(contexto, siniestroResult);
        }

        const comunicadosDef = TABLE_DEFINITIONS.comunicados;
        const comunicadosTabla = obtenerDatosTabla(comunicadosDef.sheetName);
        if (!comunicadosTabla.sheet) {
            return crearRespuestaError('No se encontró la hoja de Comunicados.', { source: contexto });
        }
        const idxComunicadoId = buscarIndiceColumna(
            comunicadosTabla.headers,
            comunicadosDef.headers?.[comunicadosDef.primaryField] || comunicadosDef.primaryField
        );
        if (idxComunicadoId === -1) {
            return crearRespuestaError('No se identificó la columna de ID para Comunicados.', { source: contexto });
        }

        const datosGeneralesDef = TABLE_DEFINITIONS.datosGenerales;
        const datosGeneralesTabla = obtenerDatosTabla(datosGeneralesDef.sheetName);
        if (!datosGeneralesTabla.sheet) {
            return crearRespuestaError('No se encontró la hoja de DatosGenerales.', { source: contexto });
        }
        const idxDatosGeneralesId = buscarIndiceColumna(
            datosGeneralesTabla.headers,
            datosGeneralesDef.headers?.[datosGeneralesDef.primaryField] || datosGeneralesDef.primaryField
        );
        if (idxDatosGeneralesId === -1) {
            return crearRespuestaError('No se identificó la columna de ID para DatosGenerales.', { source: contexto });
        }

        const comunicadoId = obtenerSiguienteId(comunicadosTabla.rows, idxComunicadoId);
        const datosGeneralesId = obtenerSiguienteId(datosGeneralesTabla.rows, idxDatosGeneralesId);

        const descripcion = `${cuenta.cuenta}-${comunicadoNombre}`;
        if (descripcion.length > 15) {
            return crearRespuestaError(
                `La descripción "${descripcion}" excede 15 caracteres. Usa un comunicado más corto.`,
                { source: contexto }
            );
        }

        const datosGeneralesRecord = {
            id: datosGeneralesId,
            idComunicado: comunicadoId,
            descripcion: descripcion,
            fecha: fecha,
            idEstado: estadoId,
            idDR: distritoResult.data?.id || '',
            idSiniestro: siniestroResult.data?.id || '',
            fechaAsignacion: null,
            idActualizacion: null
        };

        const datosGeneralesResponse = insertarRegistro('datosGenerales', datosGeneralesRecord);
        if (!datosGeneralesResponse.success) {
            return propagarRespuestaError(contexto, datosGeneralesResponse, { message: `Error al crear datos generales: ${datosGeneralesResponse.message}` });
        }

        const comunicadoRecord = {
            id: comunicadoId,
            idCuenta: cuentaId,
            comunicado: comunicadoNombre,
            status: 1,
            idSustituido: null
        };

        const comunicadoResponse = insertarRegistro('comunicados', comunicadoRecord);
        if (!comunicadoResponse.success) {
            eliminarRegistro('datosGenerales', datosGeneralesId);
            return propagarRespuestaError(contexto, comunicadoResponse, { message: `Error al crear comunicado: ${comunicadoResponse.message}` });
        }

        return {
            success: true,
            message: `Comunicado "${comunicadoNombre}" creado correctamente`,
            data: {
                comunicado: comunicadoRecord,
                datosGenerales: datosGeneralesRecord,
                distrito: distritoResult.data,
                siniestro: siniestroResult.data
            }
        };

    } catch (error) {
        console.error('Error en createComunicado:', error);
        return crearRespuestaError(`Error al crear comunicado: ${error.message}`, { source: contexto, error });
    }
}

/**
 * === ELIMINAR CUENTA ===
 * Verifica dependencias y elimina la cuenta solicitada
 */
function deleteCuenta(id) {
    const contexto = 'deleteCuenta';
    try {
        const cuentaId = String(id || '').trim();
        if (!cuentaId) {
            return crearRespuestaError('Se requiere el ID de la cuenta', { source: contexto });
        }

        const cuentaResult = buscarPorId('cuentas', cuentaId);
        if (!cuentaResult.success) {
            return propagarRespuestaError(contexto, cuentaResult);
        }
        const cuenta = cuentaResult.data;

        const comunicados = readComunicadosPorCuenta(cuentaId);
        if (!comunicados.success) {
            return propagarRespuestaError(contexto, comunicados, { message: 'No se pudo validar si la cuenta tiene comunicados asociados' });
        }

        if (comunicados.data && comunicados.data.length > 0) {
            return crearRespuestaError(
                `La cuenta "${cuenta.cuenta}" tiene ${comunicados.data.length} comunicado(s) asociado(s) y no puede ser eliminada`,
                { source: contexto }
            );
        }

        const deleteResponse = eliminarRegistro('cuentas', cuentaId);
        if (!deleteResponse.success) {
            return propagarRespuestaError(contexto, deleteResponse);
        }

        return {
            success: true,
            message: `Cuenta "${cuenta.cuenta}" eliminada correctamente`
        };

    } catch (error) {
        console.error('Error en deleteCuenta:', error);
        return crearRespuestaError(`Error al eliminar cuenta: ${error.message}`, { source: contexto, error, details: { id } });
    }
}

/**
 * === LISTAR COMUNICADOS POR CUENTA ===
 * Devuelve comunicados enriquecidos asociados a una cuenta.
 */
function readComunicadosPorCuenta(idCuenta) {
    const contexto = 'readComunicadosPorCuenta';
    let cuentaIdNumerico;

    try {
        if (idCuenta === null || idCuenta === undefined || String(idCuenta).trim() === '') {
            return crearRespuestaError('El ID de la cuenta no puede estar vacío', { source: contexto });
        }
        if (Array.isArray(idCuenta)) {
            if (idCuenta.length === 0) {
                return crearRespuestaError('El ID de cuenta proporcionado (array vacío) es inválido', { source: contexto });
            }
            cuentaIdNumerico = parseInt(idCuenta[0], 10);
        } else {
            cuentaIdNumerico = parseInt(idCuenta, 10);
        }

        if (isNaN(cuentaIdNumerico)) {
            return crearRespuestaError('El ID de la cuenta debe ser un número válido', { source: contexto, details: { idCuentaOriginal: idCuenta } });
        }

    } catch (error) {
        return crearRespuestaError(`Error al procesar ID de cuenta: ${error.message}`, { source: contexto, error });
    }

    try {
        const tablasAEvaluar = {
            comunicados: { response: readAllRows('comunicados'), essential: true },
            cuentas: { response: readAllRows('cuentas'), essential: true },
            datosGenerales: { response: readAllRows('datosGenerales'), essential: true },
            estados: { response: readAllRows('estados'), essential: false },
            distritosRiego: { response: readAllRows('distritosRiego'), essential: false },
            siniestros: { response: readAllRows('siniestros'), essential: false },
            empresas: { response: readAllRows('empresas'), essential: false }
        };

        const datosListas = {};
        for (const [nombreTabla, resultado] of Object.entries(tablasAEvaluar)) {
            if (!resultado.response || !resultado.response.success) {
                if (resultado.essential) {
                    return propagarRespuestaError(contexto, resultado.response, {
                        message: `Error crítico al leer la tabla esencial '${nombreTabla}': ${resultado.response?.message || 'respuesta inválida'}`
                    });
                } else {
                    console.warn(`readComunicadosPorCuenta: No se pudo leer la tabla opcional '${nombreTabla}'. Se continuará sin estos datos.`);
                    datosListas[nombreTabla] = [];
                }
            } else {
                datosListas[nombreTabla] = resultado.response.data || [];
            }
        }

        const parseNumeric = (value) => {
            const numeric = Number(value);
            return Number.isFinite(numeric) ? numeric : null;
        };
        const timeZone = (typeof Session !== 'undefined' && Session.getScriptTimeZone) ? Session.getScriptTimeZone() : 'UTC';
        const formatDateValue = (value) => {
            if (value instanceof Date && !Number.isNaN(value.getTime?.())) {
                try { return Utilities.formatDate(value, timeZone, 'yyyy-MM-dd'); } catch (dateError) { return value.toISOString ? value.toISOString() : String(value); }
            }
            return value === null || value === undefined ? '' : String(value);
        };

        const cuentaIdString = String(cuentaIdNumerico);
        const comunicadosFiltradosPorCuenta = (datosListas.comunicados || []).filter(com =>
            String(com.idCuenta).trim() === cuentaIdString
        );

        if (comunicadosFiltradosPorCuenta.length === 0) {
            return { success: true, data: [], message: 'La cuenta no tiene comunicados registrados' };
        }

        const comunicadosPorId = mapeoPorCampo(comunicadosFiltradosPorCuenta, 'id');
        const cuentasPorId = mapeoPorCampo(datosListas.cuentas || [], 'id');
        const datosGeneralesPorComunicadoId = mapeoPorCampo(datosListas.datosGenerales || [], 'idComunicado');
        const estadosPorId = mapeoPorCampo(datosListas.estados || [], 'id');
        const distritosPorId = mapeoPorCampo(datosListas.distritosRiego || [], 'id');
        const siniestrosPorId = mapeoPorCampo(datosListas.siniestros || [], 'id');

        const cuentaActual = obtenerDesdeMapa(cuentasPorId, cuentaIdNumerico);
        if (!cuentaActual) {
            return crearRespuestaError(`Inconsistencia: Cuenta con ID ${cuentaIdNumerico} no encontrada después de leer la tabla cuentas.`, { source: contexto });
        }

        const datosIntegrados = comunicadosFiltradosPorCuenta.map(comunicado => {
            const datoGeneral = obtenerDesdeMapa(datosGeneralesPorComunicadoId, comunicado.id) || {};
            const estado = obtenerDesdeMapa(estadosPorId, datoGeneral.idEstado) || {};
            const distrito = obtenerDesdeMapa(distritosPorId, datoGeneral.idDR) || {};
            const siniestro = obtenerDesdeMapa(siniestrosPorId, datoGeneral.idSiniestro) || {};

            return {
                id: parseNumeric(comunicado.id) ?? String(comunicado.id || '').trim(),
                idComunicado: parseNumeric(comunicado.id) ?? String(comunicado.id || '').trim(),
                idSustituido: parseNumeric(comunicado.idSustituido),
                idCuenta: parseNumeric(cuentaActual.id) ?? cuentaIdNumerico,
                cuenta: String(cuentaActual.cuenta || ''),
                comunicado: String(comunicado.comunicado || ''),
                status: comunicado.status ?? '',
                idDatosGenerales: parseNumeric(datoGeneral.id),
                descripcion: String(datoGeneral.descripcion || ''),
                fecha: formatDateValue(datoGeneral.fecha),
                idEstado: parseNumeric(datoGeneral.idEstado),
                estado: String(estado.estado || estado.nombre || ''),
                idDistritoRiego: parseNumeric(datoGeneral.idDR),
                distrito: String(distrito.distritoRiego || distrito.nombre || ''),
                fechaAsignacion: formatDateValue(datoGeneral.fechaAsignacion) || null,
                idSiniestro: parseNumeric(datoGeneral.idSiniestro),
                siniestro: String(siniestro.siniestro || siniestro.nombre || ''),
                fenomeno: String(siniestro.fenomeno || ''),
                fondo: String(siniestro.fondo || ''),
                fi: String(siniestro.fi || '')
            };
        }).sort((a, b) => {
            const nombreA = String(a.comunicado || '').toLowerCase();
            const nombreB = String(b.comunicado || '').toLowerCase();
            return nombreA.localeCompare(nombreB, 'es', { sensitivity: 'base' });
        });

        const datosSanitizados = JSON.parse(JSON.stringify(datosIntegrados));
        return { success: true, data: datosSanitizados, message: 'Comunicados encontrados' };

    } catch (error) {
        console.error('Error catastrófico en readComunicadosPorCuenta:', error);
        return crearRespuestaError(`Error inesperado al leer comunicados: ${error.message}`, { source: contexto, error, details: { idCuenta: idCuenta } });
    }
}

/**
 * === LISTAR TODOS LOS COMUNICADOS ===
 * Devuelve todos los comunicados enriquecidos del sistema.
 */
function readAllComunicados() {
    const contexto = 'readAllComunicados';
    try {
        const tablasAEvaluar = {
            comunicados: { response: readAllRows('comunicados'), essential: true },
            cuentas: { response: readAllRows('cuentas'), essential: true },
            datosGenerales: { response: readAllRows('datosGenerales'), essential: true },
            estados: { response: readAllRows('estados'), essential: false },
            distritosRiego: { response: readAllRows('distritosRiego'), essential: false },
            siniestros: { response: readAllRows('siniestros'), essential: false },
            empresas: { response: readAllRows('empresas'), essential: false }
        };

        const datosListas = {};
        for (const [nombreTabla, resultado] of Object.entries(tablasAEvaluar)) {
            if (!resultado.response || !resultado.response.success) {
                if (resultado.essential) {
                    return propagarRespuestaError(contexto, resultado.response, { message: `Error crítico al leer la tabla esencial '${nombreTabla}': ${resultado.response?.message}` });
                } else {
                    console.warn(`readAllComunicados: No se pudo leer tabla opcional '${nombreTabla}'.`);
                    datosListas[nombreTabla] = [];
                }
            } else {
                datosListas[nombreTabla] = resultado.response.data || [];
            }
        }

        const parseNumeric = (value) => {
            const numeric = Number(value);
            return Number.isFinite(numeric) ? numeric : null;
        };
        const timeZone = (typeof Session !== 'undefined' && Session.getScriptTimeZone) ? Session.getScriptTimeZone() : 'UTC';
        const formatDateValue = (value) => {
            if (value instanceof Date && !Number.isNaN(value.getTime?.())) {
                try { return Utilities.formatDate(value, timeZone, 'yyyy-MM-dd'); } catch (e) { return String(value); }
            }
            return value === null || value === undefined ? '' : String(value);
        };

        const cuentasPorId = mapeoPorCampo(datosListas.cuentas || [], 'id');
        const datosGeneralesPorComunicadoId = mapeoPorCampo(datosListas.datosGenerales || [], 'idComunicado');
        const estadosPorId = mapeoPorCampo(datosListas.estados || [], 'id');
        const distritosPorId = mapeoPorCampo(datosListas.distritosRiego || [], 'id');
        const siniestrosPorId = mapeoPorCampo(datosListas.siniestros || [], 'id');

        const todosComunicados = (datosListas.comunicados || []).map(comunicado => {
            const datoGeneral = obtenerDesdeMapa(datosGeneralesPorComunicadoId, comunicado.id) || {};
            const cuenta = obtenerDesdeMapa(cuentasPorId, comunicado.idCuenta) || {};
            const estado = obtenerDesdeMapa(estadosPorId, datoGeneral.idEstado) || {};
            const distrito = obtenerDesdeMapa(distritosPorId, datoGeneral.idDR) || {};
            const siniestro = obtenerDesdeMapa(siniestrosPorId, datoGeneral.idSiniestro) || {};

            return {
                id: parseNumeric(comunicado.id) ?? String(comunicado.id || '').trim(),
                idComunicado: parseNumeric(comunicado.id) ?? String(comunicado.id || '').trim(),
                idCuenta: parseNumeric(comunicado.idCuenta),
                cuenta: String(cuenta.cuenta || 'Cuenta Desconocida'),
                comunicado: String(comunicado.comunicado || ''),
                status: comunicado.status ?? '',
                descripcion: String(datoGeneral.descripcion || ''),
                fecha: formatDateValue(datoGeneral.fecha),
                estado: String(estado.estado || estado.nombre || ''),
                distrito: String(distrito.distritoRiego || distrito.nombre || ''),
                siniestro: String(siniestro.siniestro || siniestro.nombre || ''),
                fenomeno: String(siniestro.fenomeno || ''),
                fondo: String(siniestro.fondo || ''),
                fi: String(siniestro.fi || '')
            };
        }).sort((a, b) => {
            const fechaA = a.fecha || '';
            const fechaB = b.fecha || '';
            return fechaB.localeCompare(fechaA) || a.cuenta.localeCompare(b.cuenta);
        });

        return { success: true, data: todosComunicados, message: 'Todos los comunicados recuperados exitosamente' };

    } catch (error) {
        console.error('Error en readAllComunicados:', error);
        return crearRespuestaError(`Error al leer todos los comunicados: ${error.message}`, { source: contexto, error });
    }
}

/**
 * === ENRIQUECER COMUNICADO ===
 * Adjunta catálogos, empresa, aseguradora, presupuesto y datos extendidos a un comunicado
 */
function enriquecerComunicado(comunicado) {
    try {
        // Leer datos generales
        const datosGeneralesResult = buscarPorCampo('datosGenerales', 'idComunicado', comunicado.id);
        if (!datosGeneralesResult.success) {
            console.warn('enriquecerComunicado: error al leer datosGenerales', { comunicadoId: comunicado.id, message: datosGeneralesResult.message });
            return { ...comunicado, datosGenerales: null, error: datosGeneralesResult.message };
        }
        const datosGenerales = datosGeneralesResult.data;
        if (!datosGenerales) return { ...comunicado, datosGenerales: null };

        // Leer catálogos
        const estadoResult = buscarPorId('estados', datosGenerales.idEstado);
        const estado = estadoResult.success ? estadoResult.data : null;

        const distritoResult = buscarPorId('distritosRiego', datosGenerales.idDR);
        const distrito = distritoResult.success ? distritoResult.data : null;

        const siniestroResult = buscarPorId('siniestros', datosGenerales.idSiniestro);
        const siniestro = siniestroResult.success ? siniestroResult.data : null;

        // NUEVO: Leer ajustador
        const ajustadorResult = buscarPorId('ajustadores', datosGenerales.idAjustador);
        const ajustador = ajustadorResult.success ? ajustadorResult.data : null;

        // Buscar cuenta asociada
        const cuentaResult = buscarPorId('cuentas', comunicado.idCuenta);
        const cuentaObj = cuentaResult.success ? cuentaResult.data : null;

        // Leer actualización vigente
        let actualizacionVigente = null;
        let empresaActual = null;
        let aseguradoraActual = null;
        let presupuestoVigente = null;

        if (datosGenerales.idActualizacion) {
            const actualizacionResult = buscarPorId('actualizaciones', datosGenerales.idActualizacion);
            actualizacionVigente = actualizacionResult.success ? actualizacionResult.data : null;

            if (actualizacionVigente) {
                if (actualizacionVigente.idEmpresa) {
                    const empresaResult = buscarPorId('empresas', actualizacionVigente.idEmpresa);
                    empresaActual = empresaResult.success ? empresaResult.data : null;
                }
                if (actualizacionVigente.idAseguradora) {
                    const aseguradoraResult = buscarPorId('aseguradoras', actualizacionVigente.idAseguradora);
                    aseguradoraActual = aseguradoraResult.success ? aseguradoraResult.data : null;
                }

                // Buscar presupuesto vigente
                const presupuestosResponse = readAllRows('presupuestos');
                if (presupuestosResponse.success) {
                    presupuestoVigente = presupuestosResponse.data.find(p =>
                        String(p.idActualizacion) === String(actualizacionVigente.id) &&
                        Number(p.vigente) === 1
                    );

                    // Si hay presupuesto, agregar detalles
                    if (presupuestoVigente) {
                        const detallesResponse = readAllRows('detallePresupuesto');
                        if (detallesResponse.success) {
                            presupuestoVigente.detalles = detallesResponse.data.filter(d =>
                                String(d.idPresupuesto) === String(presupuestoVigente.id)
                            );
                        } else {
                            presupuestoVigente.detalles = [];
                        }
                    }
                }
            }
        }

        // --- NUEVOS DATOS ---
        // Equipo
        const equipoResponse = readAllRows('equipo');
        const equipo = equipoResponse.success ? equipoResponse.data.filter(e => String(e.idComunicado) === String(comunicado.id)) : [];

        // Financiero
        const financieroResponse = readAllRows('financiero');
        const financieroItems = financieroResponse.success ? financieroResponse.data.filter(f => String(f.idComunicado) === String(comunicado.id)) : [];
        const estimaciones = financieroItems.filter(f => f.tipo === 'estimacion');
        const facturas = financieroItems.filter(f => f.tipo === 'factura');

        // Tickets
        const ticketsResponse = readAllRows('tickets');
        const tickets = ticketsResponse.success ? ticketsResponse.data.filter(t => String(t.idComunicado) === String(comunicado.id)) : [];

        // --- ACTUALIZACIONES DE PRESUPUESTO ---
        // Cargar desde la tabla Actualizaciones (Origen, A, B, etc.)
        const actualizacionesPresResponse = readAllRows('actualizaciones');
        let actualizacionesPresupuesto = [];
        if (actualizacionesPresResponse.success && actualizacionesPresResponse.data) {
            actualizacionesPresupuesto = actualizacionesPresResponse.data
                .filter(a => String(a.idComunicado) === String(comunicado.id))
                .sort((a, b) => Number(a.consecutivo) - Number(b.consecutivo))
                .map(a => ({
                    id: a.id,
                    revision: a.esOrigen == 1 ? 'Origen' : (a.revision || ''),
                    fecha: a.fecha,
                    monto: a.monto || 0,
                    montoCapturado: a.montoCapturado || 0,
                    esOrigen: a.esOrigen == 1,
                    idPresupuesto: a.idPresupuesto || null
                }));
        }

        // Construir objeto enriquecido
        return {
            // Datos del comunicado
            id: comunicado.id,
            idCuenta: comunicado.idCuenta,
            cuenta: cuentaObj ? cuentaObj.cuenta : comunicado.idCuenta,
            cuentaNombre: cuentaObj ? cuentaObj.nombre : '',
            comunicado: comunicado.comunicado,
            status: comunicado.status,
            idSustituido: comunicado.idSustituido || null,

            // Datos generales
            idDatosGenerales: datosGenerales.id,
            descripcion: datosGenerales.descripcion || '',
            fecha: datosGenerales.fecha || '',
            fechaAsignacion: datosGenerales.fechaAsignacion || null,

            // Catálogos
            idEstado: datosGenerales.idEstado,
            estado: estado,
            estadoNombre: estado ? estado.estado : '',
            idDistritoRiego: datosGenerales.idDR,
            distrito: distrito,
            distritoNombre: distrito ? distrito.distritoRiego : '',
            idSiniestro: datosGenerales.idSiniestro,
            siniestro: siniestro,
            siniestroNombre: siniestro ? siniestro.siniestro : '',
            siniestroDetalle: siniestro ? {
                codigo: siniestro.siniestro,
                fenomeno: siniestro.fenomeno || '',
                fondo: siniestro.fondo || '',
                fi: siniestro.fi || ''
            } : null,

            // NUEVO: Ajustador
            idAjustador: datosGenerales.idAjustador,
            ajustador: ajustador,
            ajustadorNombre: ajustador ? ajustador.nombreAjustador : '',

            // Actualización y empresa
            idActualizacion: datosGenerales.idActualizacion,
            actualizacionVigente: actualizacionVigente,
            idEmpresa: actualizacionVigente ? actualizacionVigente.idEmpresa : null,
            empresa: empresaActual,
            empresaNombre: empresaActual ? empresaActual.razonSocial : '',
            idAseguradora: actualizacionVigente ? actualizacionVigente.idAseguradora : null,
            aseguradora: aseguradoraActual,
            aseguradoraNombre: aseguradoraActual ? aseguradoraActual.descripcion : '',

            // Presupuesto (Actualizaciones: Origen, A, B, etc.)
            presupuestoVigente: presupuestoVigente,
            presupuestoTotal: presupuestoVigente ? presupuestoVigente.total : null,
            presupuesto: actualizacionesPresupuesto, // Array de actualizaciones (Origen, A, B...)

            // Nuevos Tabs
            equipo: equipo,
            financiero: { estimaciones: estimaciones, facturas: facturas },
            tickets: tickets,

            // Objeto completo de datos generales
            datosGenerales: datosGenerales
        };

    } catch (error) {
        console.error('Error en enriquecerComunicado:', error);
        return { ...comunicado, error: error.message, datosGenerales: null };
    }
}

/**
 * === ACTUALIZAR COMUNICADO ===
 * Actualiza los datos de un comunicado existente
 */
function updateComunicado(id, updates) {
    const contexto = 'updateComunicado';
    try {
        const comunicadoId = String(id || '').trim();
        if (!comunicadoId) {
            return crearRespuestaError('Se requiere el ID del comunicado', { source: contexto });
        }

        // 1. Validar existencia
        const comunicadoResult = buscarPorId('comunicados', comunicadoId);
        if (!comunicadoResult.success) {
            return propagarRespuestaError(contexto, comunicadoResult);
        }
        const datosGeneralesResult = buscarPorCampo('datosGenerales', 'idComunicado', comunicadoId);
        if (!datosGeneralesResult.success) {
            return propagarRespuestaError(contexto, datosGeneralesResult);
        }
        const datosGenerales = datosGeneralesResult.data;

        // 2. Procesar actualizaciones
        // Actualizar tabla 'comunicados'
        if (updates.comunicado) {
            const updateComResult = actualizarRegistro('comunicados', comunicadoId, { comunicado: updates.comunicado });
            if (!updateComResult.success) {
                return propagarRespuestaError(contexto, updateComResult);
            }
        }

        // Actualizar tabla 'datosGenerales'
        const updatesDatosGenerales = {};
        if (updates.descripcion) updatesDatosGenerales.descripcion = updates.descripcion;
        if (updates.fecha) updatesDatosGenerales.fecha = updates.fecha;
        if (updates.idEstado) updatesDatosGenerales.idEstado = updates.idEstado;
        if (updates.idAjustador) updatesDatosGenerales.idAjustador = updates.idAjustador; // NUEVO

        // Manejo de catálogos (Distrito y Siniestro)
        if (updates.distrito) {
            const distritoResult = ensureCatalogRecord('distritosRiego', { distritoRiego: updates.distrito });
            if (distritoResult.success && distritoResult.data && distritoResult.data.id) {
                updatesDatosGenerales.idDR = distritoResult.data.id;
            }
        }
        if (updates.siniestro) {
            const siniestroResult = ensureCatalogRecord('siniestros', {
                siniestro: updates.siniestro
            });
            if (siniestroResult.success && siniestroResult.data && siniestroResult.data.id) {
                updatesDatosGenerales.idSiniestro = siniestroResult.data.id;
            }
        }

        if (Object.keys(updatesDatosGenerales).length > 0) {
            const updateDGResult = actualizarRegistro('datosGenerales', datosGenerales.id, updatesDatosGenerales);
            if (!updateDGResult.success) {
                return propagarRespuestaError(contexto, updateDGResult);
            }
        }

        // 3. Actualizar Equipo
        if (updates.equipo) {
            _syncChildTable('equipo', 'idComunicado', comunicadoId, updates.equipo);
        }

        // 4. Actualizar Financiero
        if (updates.financiero) {
            const estimaciones = (updates.financiero.estimaciones || []).map(e => ({ ...e, tipo: 'estimacion' }));
            const facturas = (updates.financiero.facturas || []).map(f => ({ ...f, tipo: 'factura' }));
            const allFinanciero = [...estimaciones, ...facturas];
            _syncChildTable('financiero', 'idComunicado', comunicadoId, allFinanciero);
        }

        // 5. Actualizar Tickets
        if (updates.tickets) {
            _syncChildTable('tickets', 'idComunicado', comunicadoId, updates.tickets);
        }

        // 6. Actualizar Presupuesto
        if (updates.presupuesto) {
            _handlePresupuestoUpdate(comunicadoId, datosGenerales, updates.presupuesto);
        }

        return { success: true, message: 'Comunicado actualizado correctamente' };

    } catch (error) {
        console.error('Error en updateComunicado:', error);
        return crearRespuestaError(`Error al actualizar comunicado: ${error.message}`, { source: contexto, error, details: { id } });
    }
}

function getComunicadoCompleto(id) {
    const contexto = 'getComunicadoCompleto';
    console.log(`[${contexto}] Iniciando solicitud para ID: ${id}`);
    try {
        const comunicadoId = String(id || '').trim();
        if (!comunicadoId) {
            console.warn(`[${contexto}] ID no proporcionado.`);
            return crearRespuestaError('Se requiere el ID del comunicado', { source: contexto });
        }

        const comunicadoResult = buscarPorId('comunicados', comunicadoId);
        if (!comunicadoResult.success) {
            console.warn(`[${contexto}] No se encontró el comunicado con ID: ${comunicadoId}`);
            return propagarRespuestaError(contexto, comunicadoResult);
        }

        const comunicadoEnriquecido = enriquecerComunicado(comunicadoResult.data);
        const response = { success: true, data: comunicadoEnriquecido };
        const sanitizedResponse = JSON.parse(JSON.stringify(response));

        console.log(`[${contexto}] Respuesta generada exitosamente para ID: ${comunicadoId}`);
        return sanitizedResponse;

    } catch (error) {
        console.error(`Error en ${contexto}:`, error);
        return crearRespuestaError(`Error al obtener comunicado completo: ${error.message}`, { source: contexto, error, details: { id } });
    }
}

/**
 * Sincroniza una tabla hija (borra anteriores e inserta nuevos)
 */
function _syncChildTable(tableName, foreignKeyField, foreignKeyValue, dataArray) {
    try {
        // 1. Leer todos
        const response = readAllRows(tableName);
        if (response.success && response.data) {
            // 2. Filtrar los que pertenecen a este padre
            const toDelete = response.data.filter(row => String(row[foreignKeyField]) === String(foreignKeyValue));

            // 3. Borrar
            toDelete.forEach(row => {
                eliminarRegistro(tableName, row.id);
            });
        }

        // 4. Insertar nuevos
        dataArray.forEach(item => {
            const newItem = { ...item };
            newItem[foreignKeyField] = foreignKeyValue;
            delete newItem.id; // Asegurar que se genere nuevo ID
            insertarRegistro(tableName, newItem);
        });
    } catch (e) {
        console.error(`Error syncing table ${tableName}:`, e);
        throw e;
    }
}

/**
 * Maneja la sincronización de actualizaciones de presupuesto.
 * Cada actualización (Origen, A, B, etc.) se guarda como un registro en la tabla Actualizaciones.
 *
 * @param {number} comunicadoId - ID del comunicado
 * @param {Object} datosGenerales - Datos generales del comunicado
 * @param {Array} presupuestoItems - Array de actualizaciones de presupuesto desde el frontend
 */
function _handlePresupuestoUpdate(comunicadoId, datosGenerales, presupuestoItems) {
    if (!presupuestoItems || !Array.isArray(presupuestoItems)) {
        return;
    }

    try {
        // 1. Obtener actualizaciones existentes para este comunicado
        const allActualizaciones = readAllRows('actualizaciones');
        const existentes = (allActualizaciones.success && allActualizaciones.data) ?
            allActualizaciones.data.filter(a => String(a.idComunicado) === String(comunicadoId)) : [];

        // 2. Mapear por consecutivo para identificar actualizaciones vs inserciones
        const existentesMap = new Map();
        existentes.forEach(e => {
            existentesMap.set(Number(e.consecutivo), e);
        });

        // 3. Procesar cada item del frontend
        presupuestoItems.forEach((item, index) => {
            const consecutivo = index + 1; // 1-based
            const esOrigen = index === 0;
            const revision = esOrigen ? 'Origen' : (item.revision || '');

            const registro = {
                idComunicado: comunicadoId,
                consecutivo: consecutivo,
                esOrigen: esOrigen ? 1 : 0,
                revision: revision,
                monto: parseFloat(item.monto) || 0,
                montoCapturado: parseFloat(item.montoCapturado) || 0,
                idPresupuesto: item.idPresupuesto || null,
                fecha: item.fecha || new Date().toISOString()
            };

            const existente = existentesMap.get(consecutivo);
            if (existente) {
                // Actualizar registro existente
                actualizarRegistro('actualizaciones', existente.id, registro);
                existentesMap.delete(consecutivo); // Marcar como procesado
            } else {
                // Insertar nuevo registro
                insertarRegistro('actualizaciones', registro);
            }
        });

        // 4. Eliminar actualizaciones que ya no existen en el frontend
        existentesMap.forEach((antiguo, consecutivo) => {
            eliminarRegistro('actualizaciones', antiguo.id);
        });

    } catch (error) {
        console.error('Error en _handlePresupuestoUpdate:', error);
        throw error;
    }
}

/**
 * Crea una referencia y un comunicado asociado en una sola transacción lógica.
 * Utilizado por el modal de "Alta Express".
 * @param {object} datosReferencia - { referencia: '...', idAjustador: ... }
 * @param {object} datosComunicado - { comunicado: '...', fecha: ..., ... }
 */
function crearReferenciaConComunicado(datosReferencia, datosComunicado) {
    const contexto = 'crearReferenciaConComunicado';

    // 1. Crear Referencia
    const refResp = createRow('cuentas', datosReferencia);
    if (!refResp.success) {
        return propagarRespuestaError(contexto, refResp, { message: 'Error al crear la referencia.' });
    }

    const nuevaCuentaId = refResp.data.id;

    // 2. Preparar datos del comunicado con el ID de la nueva cuenta
    datosComunicado.idCuenta = nuevaCuentaId;

    // 3. Crear Comunicado
    const comResp = createComunicado(datosComunicado);

    if (!comResp.success) {
        // Rollback: intentar borrar la referencia creada para mantener consistencia
        console.warn(`[${contexto}] Falló creación de comunicado. Revirtiendo referencia ${nuevaCuentaId}...`);
        try {
            deleteRow('cuentas', nuevaCuentaId);
        } catch (e) {
            console.error(`[${contexto}] Error en rollback de referencia:`, e);
        }

        return propagarRespuestaError(contexto, comResp, {
            message: `Error al crear comunicado: ${comResp.message}. La referencia no se guardó.`
        });
    }

    return {
        success: true,
        data: {
            cuenta: refResp.data,
            comunicado: comResp.data
        },
        message: 'Referencia y comunicado creados correctamente.'
    };
}