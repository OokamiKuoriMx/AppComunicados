/**
 * ============================================================================
 * ARCHIVO: comunicados.crud.gs
 * Descripción: Operaciones CRUD para Comunicados
 * Versión: 2.0 (Refactorizada)
 * ============================================================================
 */

/**
 * === CATALOGO DE COMUNICADOS ===
 * Obtiene catálogos necesarios para crear comunicados
 * @return {Object} {success, data: {estados, distritosRiego, siniestros}}
 */
function getComunicadoCatalogs() {
    try {
        // Leer catálogos usando función genérica
        const estadosResponse = readAllRows('estados');
        const distritosResponse = readAllRows('distritosRiego');
        const siniestrosResponse = readAllRows('siniestros');

        // Procesar respuestas
        const processResponse = (response) => {
            if (response && response.success && Array.isArray(response.data)) {
                return response.data.sort((a, b) => {
                    const nombreA = String(a.nombre || a.estado || a.distritoRiego || a.siniestro || '');
                    const nombreB = String(b.nombre || b.estado || b.distritoRiego || b.siniestro || '');
                    return nombreA.localeCompare(nombreB, 'es', { sensitivity: 'base' });
                });
            }
            return [];
        };

        return {
            success: true,
            data: {
                estados: processResponse(estadosResponse),
                distritosRiego: processResponse(distritosResponse),
                siniestros: processResponse(siniestrosResponse)
            }
        };

    } catch (error) {
        console.error('Error en getComunicadoCatalogs:', error);
        return {
            success: false,
            message: `Error al obtener catálogos: ${error.message}`
        };
    }
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
            return propagarRespuestaError(contexto, comunicadosResponse, {
                message: `No fue posible validar comunicados existentes: ${comunicadosResponse.message}`
            });
        }

        const comunicadosExistentes = comunicadosResponse.data || [];
        const duplicado = comunicadosExistentes.find(c =>
            normalizarClave(c.comunicado) === normalizarClave(comunicadoNombre) &&
            String(c.idCuenta) === cuentaId
        );
        if (duplicado) {
            return crearRespuestaError(`Ya existe un comunicado "${comunicadoNombre}" para esta cuenta`, { source: contexto });
        }

        const distritoResult = ensureCatalogRecord('distritosRiego', {
            distritoRiego: distritoNombre
        });
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

        const descripcion = `${cuenta.referencia}-${comunicadoNombre}`;
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
            idDR: distritoResult.data.id,
            idSiniestro: siniestroResult.data.id,
            fechaAsignacion: null,
            idActualizacion: null
        };

        const datosGeneralesResponse = insertarRegistro('datosGenerales', datosGeneralesRecord);
        if (!datosGeneralesResponse.success) {
            return propagarRespuestaError(contexto, datosGeneralesResponse, {
                message: `Error al crear datos generales: ${datosGeneralesResponse.message}`
            });
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
            return propagarRespuestaError(contexto, comunicadoResponse, {
                message: `Error al crear comunicado: ${comunicadoResponse.message}`
            });
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
        return crearRespuestaError(`Error al crear comunicado: ${error.message}`, {
            source: contexto,
            error
        });
    }
}

/**
 * Elimina una cuenta (valida que no tenga comunicados)
 * @param {string|number} id - ID de la cuenta
 * @return {Object} {success, message}
 */
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
            return propagarRespuestaError(contexto, comunicados, {
                message: 'No se pudo validar si la cuenta tiene comunicados asociados'
            });
        }

        if (comunicados.data && comunicados.data.length > 0) {
            return crearRespuestaError(
                `La cuenta "${cuenta.referencia}" tiene ${comunicados.data.length} comunicado(s) asociado(s) y no puede ser eliminada`,
                { source: contexto }
            );
        }

        const deleteResponse = eliminarRegistro('cuentas', cuentaId);
        if (!deleteResponse.success) {
            return propagarRespuestaError(contexto, deleteResponse);
        }

        return {
            success: true,
            message: `Cuenta "${cuenta.referencia}" eliminada correctamente`
        };

    } catch (error) {
        console.error('Error en deleteCuenta:', error);
        return crearRespuestaError(`Error al eliminar cuenta: ${error.message}`, {
            source: contexto,
            error,
            details: { id }
        });
    }
}

/**
 * === LISTAR COMUNICADOS POR CUENTA ===
 * Devuelve comunicados enriquecidos asociados a una cuenta. Solo falla si las tablas
 * 'comunicados', 'cuentas' o 'datosGenerales' no se pueden leer.
 * @param {string|number} idCuenta - ID de la cuenta.
 * @return {{ success: boolean, data: Array<Object>|null, message: string }}
 */
function readComunicadosPorCuenta(idCuenta) {
    const contexto = 'readComunicadosPorCuenta';
    let cuentaIdNumerico;

    // --- 1. Validar y Normalizar idCuenta ---
    // (Misma validación que antes...)
    try {
        if (idCuenta === null || idCuenta === undefined || String(idCuenta).trim() === '') {
            return crearRespuestaError('El ID de la cuenta no puede estar vacío', { source: contexto });
        }
        // ... (resto de la validación y conversión a número) ...
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
        console.log(`readComunicadosPorCuenta: Iniciando para cuenta ID ${cuentaIdNumerico}`);
    } catch (error) {
        return crearRespuestaError(`Error al procesar ID de cuenta: ${error.message}`, { source: contexto, error });
    }


    try {
        // --- 2. Leer TODAS las tablas necesarias UNA SOLA VEZ ---
        const tablasAEvaluar = {
            // Esenciales (fallo si no se leen)
            comunicados: { response: readAllRows('comunicados'), essential: true },
            cuentas: { response: readAllRows('cuentas'), essential: true },
            datosGenerales: { response: readAllRows('datosGenerales'), essential: true },
            // Opcionales (continúa si no se leen, datos quedarán vacíos)
            estados: { response: readAllRows('estados'), essential: false },
            distritosRiego: { response: readAllRows('distritosRiego'), essential: false },
            siniestros: { response: readAllRows('siniestros'), essential: false },
            empresas: { response: readAllRows('empresas'), essential: false }
            // Añade aquí más lecturas opcionales
        };

        // --- 2.1 Verificar Lecturas ---
        const datosListas = {}; // Guardará los arrays de datos (ej. datosListas.comunicados = [...])
        for (const [nombreTabla, resultado] of Object.entries(tablasAEvaluar)) {
            if (!resultado.response || !resultado.response.success) {
                // Si es esencial y falló, retornar error
                if (resultado.essential) {
                    return propagarRespuestaError(contexto, resultado.response, {
                        message: `Error crítico al leer la tabla esencial '${nombreTabla}': ${resultado.response?.message || 'respuesta inválida'}`
                    });
                } else {
                    // Si no es esencial, loguear advertencia y usar array vacío
                    console.warn(`readComunicadosPorCuenta: No se pudo leer la tabla opcional '${nombreTabla}'. Se continuará sin estos datos. Error: ${resultado.response?.message || 'respuesta inválida'}`);
                    datosListas[nombreTabla] = []; // Usar array vacío
                }
            } else {
                // Si la lectura fue exitosa, guardar los datos
                datosListas[nombreTabla] = resultado.response.data || [];
            }
        }

        // --- 3. Utilidades de normalización ---
        const parseNumeric = (value) => {
            const numeric = Number(value);
            return Number.isFinite(numeric) ? numeric : null;
        };

        const timeZone = (typeof Session !== 'undefined' && Session.getScriptTimeZone)
            ? Session.getScriptTimeZone()
            : 'UTC';

        const formatDateValue = (value) => {
            if (value instanceof Date && !Number.isNaN(value.getTime?.())) {
                try {
                    return Utilities.formatDate(value, timeZone, 'yyyy-MM-dd');
                } catch (dateError) {
                    console.warn('readComunicadosPorCuenta: No se pudo formatear fecha, se regresará ISO.', dateError);
                    return value.toISOString ? value.toISOString() : String(value);
                }
            }
            return value === null || value === undefined ? '' : String(value);
        };

        // --- 4. Filtrar Comunicados iniciales ---
        const cuentaIdString = String(cuentaIdNumerico);
        const comunicadosFiltradosPorCuenta = (datosListas.comunicados || []).filter(com =>
            String(com.idCuenta).trim() === cuentaIdString
        );

        console.log('readComunicadosPorCuenta: Comunicados filtrados por cuenta', { cuentaId: cuentaIdNumerico, cantidad: comunicadosFiltradosPorCuenta.length });

        if (comunicadosFiltradosPorCuenta.length === 0) {
            return { success: true, data: [], message: 'La cuenta no tiene comunicados registrados' };
        }

        // --- 5. Crear Mapas para búsqueda rápida ---
        // Se crean mapas incluso si los datos están vacíos (serán mapas vacíos)
        const comunicadosPorId = mapeoPorCampo(comunicadosFiltradosPorCuenta, 'id');
        const cuentasPorId = mapeoPorCampo(datosListas.cuentas || [], 'id');
        const datosGeneralesPorComunicadoId = mapeoPorCampo(datosListas.datosGenerales || [], 'idComunicado');
        const estadosPorId = mapeoPorCampo(datosListas.estados || [], 'id');
        const distritosPorId = mapeoPorCampo(datosListas.distritosRiego || [], 'id');
        const siniestrosPorId = mapeoPorCampo(datosListas.siniestros || [], 'id');
        const empresasPorId = mapeoPorCampo(datosListas.empresas || [], 'id');
        // ... crea más mapas si leíste más tablas ...

        // Obtener la cuenta específica (ya leída y mapeada)
        const cuentaActual = obtenerDesdeMapa(cuentasPorId, cuentaIdNumerico);
        // Verificación crucial: la cuenta DEBE existir si llegó hasta aquí porque es esencial
        if (!cuentaActual) {
            return crearRespuestaError(`Inconsistencia: Cuenta con ID ${cuentaIdNumerico} no encontrada después de leer la tabla cuentas.`, { source: contexto });
        }

        // --- 6. Iterar y Combinar (Enriquecer) ---
        const datosIntegrados = comunicadosFiltradosPorCuenta.map(comunicado => {
            // Busca el datoGeneral correspondiente
            const datoGeneral = obtenerDesdeMapa(datosGeneralesPorComunicadoId, comunicado.id) || {}; // Objeto vacío si no se encuentra

            // Busca las entidades relacionadas (estas pueden faltar si la tabla opcional no se leyó)
            const estado = obtenerDesdeMapa(estadosPorId, datoGeneral.idEstado) || {};
            const distrito = obtenerDesdeMapa(distritosPorId, datoGeneral.idDR) || {};
            const siniestro = obtenerDesdeMapa(siniestrosPorId, datoGeneral.idSiniestro) || {};

            // Construir el objeto combinado final
            return {
                id: parseNumeric(comunicado.id) ?? String(comunicado.id || '').trim(),
                idComunicado: parseNumeric(comunicado.id) ?? String(comunicado.id || '').trim(),
                idSustituido: parseNumeric(comunicado.idSustituido),
                idCuenta: parseNumeric(cuentaActual.id) ?? cuentaIdNumerico,
                cuenta: String(cuentaActual.referencia || ''),
                comunicado: String(comunicado.comunicado || ''),
                status: comunicado.status ?? '',
                idDatosGenerales: parseNumeric(datoGeneral.id),
                descripcion: String(datoGeneral.descripcion || ''),
                fecha: formatDateValue(datoGeneral.fecha),
                idEstado: parseNumeric(datoGeneral.idEstado),
                estado: String(estado.estado || estado.nombre || ''),
                idDistritoRiego: parseNumeric(datoGeneral.idDR),
                distrito: String(distrito.distritoRiego || distrito.nombre || ''),
                // idEmpresa: parseNumeric(datoGeneral.idEmpresa),
                // empresa: String(empresa.razonSocial || empresa.nombre || ''),
                fechaAsignacion: formatDateValue(datoGeneral.fechaAsignacion) || null,
                idSiniestro: parseNumeric(datoGeneral.idSiniestro),
                siniestro: String(siniestro.siniestro || siniestro.nombre || ''),
                fenomeno: String(siniestro.fenomeno || ''),
                fondo: String(siniestro.fondo || ''),
                fi: String(siniestro.fi || '')
                // ... añade aquí datos de otras tablas opcionales ...
            };
        }).sort((a, b) => { // Ordenar al final
            const nombreA = String(a.comunicado || '').toLowerCase();
            const nombreB = String(b.comunicado || '').toLowerCase();
            return nombreA.localeCompare(nombreB, 'es', { sensitivity: 'base' });
        });

        const datosSanitizados = JSON.parse(JSON.stringify(datosIntegrados));

        console.log('readComunicadosPorCuenta: Completado', { cuentaId: cuentaIdNumerico, total: datosSanitizados.length });

        return {
            success: true,
            data: datosSanitizados,
            message: 'Comunicados encontrados'
        };

    } catch (error) {
        console.error('Error catastrófico en readComunicadosPorCuenta:', error);
        // Asegúrate que tu función 'crearRespuestaError' maneje bien el caso donde idCuenta podría no ser numérico aquí
        return crearRespuestaError(`Error inesperado al leer comunicados: ${error.message}`, {
            source: contexto,
            error,
            details: { idCuenta: idCuenta } // Usar el id original en el log de error
        });
    }

    // Salvaguarda en caso de que se alcance un estado no contemplado.
    console.warn('readComunicadosPorCuenta: se alcanzó el final sin generar respuesta explícita.', {
        idCuenta: idCuenta
    });
    return crearRespuestaError('No se pudo generar la respuesta de comunicados.', {
        source: contexto,
        details: { idCuenta: idCuenta }
    });
}

/**
 * Enriquece un comunicado con datos relacionados
 * @param {Object} comunicado - Comunicado base
 * @return {Object} Comunicado enriquecido
 */
/**
 * === ENRIQUECER COMUNICADO ===
 * Adjunta catálogos, empresa, aseguradora y presupuesto a un comunicado
 */
function enriquecerComunicado(comunicado) {
    try {
        // Leer datos generales
        const datosGeneralesResult = buscarPorCampo('datosGenerales', 'idComunicado', comunicado.id);
        if (!datosGeneralesResult.success) {
            console.warn('enriquecerComunicado: error al leer datosGenerales', {
                comunicadoId: comunicado.id,
                message: datosGeneralesResult.message
            });
            return {
                id: comunicado.id,
                idCuenta: comunicado.idCuenta,
                comunicado: comunicado.comunicado,
                status: comunicado.status,
                idSustituido: comunicado.idSustituido || null,
                datosGenerales: null,
                descripcion: '',
                fecha: '',
                fechaAsignacion: '',
                estado: null,
                estadoNombre: '',
                distrito: null,
                distritoNombre: '',
                siniestro: null,
                siniestroNombre: '',
                actualizacionVigente: null,
                empresa: null,
                empresaNombre: '',
                aseguradora: null,
                aseguradoraNombre: '',
                presupuestoVigente: null,
                presupuestoTotal: null,
                presupuestoDetalles: [],
                error: datosGeneralesResult.message || 'No fue posible obtener datos generales.'
            };
        }

        const datosGenerales = datosGeneralesResult.data;

        if (!datosGenerales) {
            return {
                id: comunicado.id,
                idCuenta: comunicado.idCuenta,
                comunicado: comunicado.comunicado,
                status: comunicado.status,
                idSustituido: comunicado.idSustituido || null,
                datosGenerales: null,
                descripcion: '',
                fecha: '',
                fechaAsignacion: '',
                estado: null,
                estadoNombre: '',
                distrito: null,
                distritoNombre: '',
                siniestro: null,
                siniestroNombre: '',
                actualizacionVigente: null,
                empresa: null,
                empresaNombre: '',
                aseguradora: null,
                aseguradoraNombre: '',
                presupuestoVigente: null,
                presupuestoTotal: null,
                presupuestoDetalles: [],
                error: 'El comunicado no tiene datos generales asociados.'
            };
        }

        // Leer catálogos
        const estadoResult = buscarPorId('estados', datosGenerales.idEstado);
        const estado = estadoResult.success ? estadoResult.data : null;

        const distritoResult = buscarPorId('distritosRiego', datosGenerales.idDR);
        const distrito = distritoResult.success ? distritoResult.data : null;

        const siniestroResult = buscarPorId('siniestros', datosGenerales.idSiniestro);
        const siniestro = siniestroResult.success ? siniestroResult.data : null;

        // Leer actualización vigente
        let actualizacionVigente = null;
        let empresaActual = null;
        let aseguradoraActual = null;
        let presupuestoVigente = null;

        if (datosGenerales.idActualizacion) {
            const actualizacionResult = buscarPorId('actualizaciones', datosGenerales.idActualizacion);
            actualizacionVigente = actualizacionResult.success ? actualizacionResult.data : null;

            if (actualizacionVigente) {
                const empresaResult = buscarPorId('empresas', actualizacionVigente.idEmpresa);
                empresaActual = empresaResult.success ? empresaResult.data : null;

                const aseguradoraResult = buscarPorId('aseguradoras', actualizacionVigente.idAseguradora);
                aseguradoraActual = aseguradoraResult.success ? aseguradoraResult.data : null;

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

        // Construir objeto enriquecido
        return {
            // Datos del comunicado
            id: comunicado.id,
            idCuenta: comunicado.idCuenta,
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

            // Actualización y empresa
            idActualizacion: datosGenerales.idActualizacion,
            actualizacionVigente: actualizacionVigente,

            idEmpresa: actualizacionVigente ? actualizacionVigente.idEmpresa : null,
            empresa: empresaActual,
            empresaNombre: empresaActual ? empresaActual.razonSocial : '',

            idAseguradora: actualizacionVigente ? actualizacionVigente.idAseguradora : null,
            aseguradora: aseguradoraActual,
            aseguradoraNombre: aseguradoraActual ? aseguradoraActual.descripcion : '',

            // Presupuesto
            presupuestoVigente: presupuestoVigente,
            presupuestoTotal: presupuestoVigente ? presupuestoVigente.total : null,
            presupuestoDetalles: presupuestoVigente ? (presupuestoVigente.detalles || []) : [],

            // Objeto completo de datos generales
            datosGenerales: datosGenerales
        };

    } catch (error) {
        console.error('Error en enriquecerComunicado:', error);
        return {
            ...comunicado,
            error: error.message,
            datosGenerales: null
        };
    }
}

/**
 * Actualiza un comunicado
 * @param {string|number} id - ID del comunicado
 * @param {Object} updates - Campos a actualizar
 * @return {Object} {success, message}
 */
/**
 * === ACTUALIZAR COMUNICADO ===
 * Modifica campos puntuales de un comunicado existente
 */
function updateComunicado(id, updates) {
    const contexto = 'updateComunicado';
    try {
        const comunicadoId = String(id || '').trim();
        if (!comunicadoId) {
            return crearRespuestaError('Se requiere el ID del comunicado', { source: contexto });
        }

        const comunicadoResult = buscarPorId('comunicados', comunicadoId);
        if (!comunicadoResult.success) {
            return propagarRespuestaError(contexto, comunicadoResult);
        }

        if (updates.comunicado && String(updates.comunicado).length > 15) {
            return crearRespuestaError('El comunicado no puede exceder 15 caracteres', { source: contexto });
        }

        const response = actualizarRegistro('comunicados', comunicadoId, updates);
        return response;

    } catch (error) {
        console.error('Error en updateComunicado:', error);
        return crearRespuestaError(`Error al actualizar comunicado: ${error.message}`, {
            source: contexto,
            error,
            details: { id, updates }
        });
    }
}

/**
 * Elimina un comunicado y sus datos relacionados
 * @param {string|number} id - ID del comunicado
 * @return {Object} {success, message}
 */
/**
 * === ELIMINAR COMUNICADO ===
 * Borra un comunicado junto con sus datos generales vinculados
 */
function deleteComunicado(id) {
    const contexto = 'deleteComunicado';
    try {
        const comunicadoId = String(id || '').trim();
        if (!comunicadoId) {
            return crearRespuestaError('Se requiere el ID del comunicado', { source: contexto });
        }

        const comunicadoResult = buscarPorId('comunicados', comunicadoId);
        if (!comunicadoResult.success) {
            return propagarRespuestaError(contexto, comunicadoResult);
        }

        const comunicado = comunicadoResult.data;

        const datosGeneralesResult = buscarPorCampo('datosGenerales', 'idComunicado', comunicadoId);
        if (!datosGeneralesResult.success) {
            return propagarRespuestaError(contexto, datosGeneralesResult);
        }

        const datosGenerales = datosGeneralesResult.data;

        if (datosGenerales) {
            const deleteDataResponse = eliminarRegistro('datosGenerales', datosGenerales.id);
            if (!deleteDataResponse.success) {
                return propagarRespuestaError(contexto, deleteDataResponse, {
                    message: `Error al eliminar datos generales: ${deleteDataResponse.message}`
                });
            }
        }

        const response = eliminarRegistro('comunicados', comunicadoId);
        if (!response.success) {
            return propagarRespuestaError(contexto, response);
        }

        return {
            success: true,
            message: `Comunicado "${comunicado.comunicado}" eliminado correctamente`
        };

    } catch (error) {
        console.error('Error en deleteComunicado:', error);
        return crearRespuestaError(`Error al eliminar comunicado: ${error.message}`, {
            source: contexto,
            error,
            details: { id }
        });
    }
}

/**
 * Obtiene un comunicado completo con todos sus datos
 * @param {string|number} idComunicado - ID del comunicado
 * @return {Object} {success, data}
 */
/**
 * === OBTENER COMUNICADO COMPLETO ===
 * Recupera un comunicado con toda su información relacionada
 */
function getComunicadoCompleto(idComunicado) {
    const contexto = 'getComunicadoCompleto';
    try {
        const comunicadoId = String(idComunicado || '').trim();
        if (!comunicadoId) {
            return crearRespuestaError('Se requiere el ID del comunicado', { source: contexto });
        }

        const comunicadoResult = buscarPorId('comunicados', comunicadoId);
        if (!comunicadoResult.success) {
            return propagarRespuestaError(contexto, comunicadoResult);
        }

        const comunicado = comunicadoResult.data;
        const comunicadoEnriquecido = enriquecerComunicado(comunicado);

        return {
            success: true,
            data: comunicadoEnriquecido
        };

    } catch (error) {
        console.error('Error en getComunicadoCompleto:', error);
        return crearRespuestaError(`Error al obtener comunicado: ${error.message}`, {
            source: contexto,
            error,
            details: { idComunicado }
        });
    }
}

/**
 * Lee todos los comunicados
 * @return {Object} {success, data}
 */
/**
 * === LISTAR TODOS LOS COMUNICADOS ===
 * Retorna todos los comunicados registrados
 */
function readComunicados() {
    const contexto = 'readComunicados';
    try {
        const response = readAllRows('comunicados');
        return response;
    } catch (error) {
        console.error('Error en readComunicados:', error);
        return crearRespuestaError(`Error al leer comunicados: ${error.message}`, {
            source: contexto,
            error
        });
    }
}

/**
 * Función de depuración simple para inspeccionar un valor.
 * Muestra el valor, su tipo y si es un array.
 * @param {*} inputData - El valor a depurar.
 */
function debugFunction() {
    const inputData = [1]
    console.log("--- DEBUG START ---"); // Marca el inicio en la consola
    console.log(typeof (inputData))
    const resultado = readComunicadosPorCuenta(inputData)
    console.log(resultado)
    console.log("--- DEBUG END ---"); // Marca el fin en la consola
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