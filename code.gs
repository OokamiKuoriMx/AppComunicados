/**
 * =================================================================
 * PUNTO DE ENTRADA DE LA APLICACIÓN
 * =================================================================
 */
function doGet(e) {
    return HtmlService.createTemplateFromFile('index')
        .evaluate()
        .setTitle('Gestión de Comunicados')
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
        .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function include(filename) {
    return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

/**
 * =================================================================
 * ARCHIVO CRUD (Create, Read, Update, Delete)
 * =================================================================
 * Contiene las funciones principales para interactuar con los datos
 * de las hojas de cálculo como si fueran una base de datos.
 */

/**
 * Crea un nuevo registro en la tabla especificada.
 * @param {string} nombreTabla La clave de la tabla en TABLE_DEFINITIONS (ej: 'cuentas').
 * @param {object} datos Objeto con los datos a insertar (ej: { cuenta: 'Nueva Cuenta' }).
 * @returns {{ success: boolean, data?: object, message: string }}
 */
function createRow(nombreTabla, datos) {
    const contexto = 'createRow';
    const def = TABLE_DEFINITIONS[nombreTabla];
    if (!def) {
        return crearRespuestaError(`La tabla "${nombreTabla}" no está definida.`, {
            source: contexto,
            details: { nombreTabla }
        });
    }

    for (const campo of def.requiredFields) {
        if (!datos[campo] || String(datos[campo]).trim() === '') {
            return crearRespuestaError(`El campo requerido "${campo}" no puede estar vacío.`, {
                source: contexto,
                details: { nombreTabla, campo }
            });
        }
    }

    const { sheet, headers, rows } = obtenerDatosTabla(def.sheetName);
    if (!sheet) {
        return crearRespuestaError(`La hoja de cálculo "${def.sheetName}" no fue encontrada.`, {
            source: contexto,
            details: { nombreTabla, sheetName: def.sheetName }
        });
    }

    const indiceId = buscarIndiceColumna(headers, def.headers?.[def.primaryField] || def.primaryField);
    if (indiceId === -1) {
        return crearRespuestaError(`La columna del campo primario "${def.primaryField}" no se encontró en la hoja.`, {
            source: contexto,
            details: { nombreTabla, primaryField: def.primaryField }
        });
    }

    const idProporcionado = Object.prototype.hasOwnProperty.call(datos, def.primaryField)
        && String(datos[def.primaryField] ?? '').trim() !== '';

    if (!idProporcionado) {
        datos[def.primaryField] = obtenerSiguienteId(rows, indiceId);
    } else {
        const valorId = datos[def.primaryField];
        const existeId = rows.some(fila => normalizarTexto(fila[indiceId]) === normalizarTexto(valorId));
        if (existeId) {
            return crearRespuestaError(`El valor "${valorId}" para el campo primario "${def.primaryField}" ya existe.`, {
                source: contexto,
                details: { nombreTabla, primaryField: def.primaryField, valorId }
            });
        }
    }

    for (const campo of def.uniqueFields) {
        const indiceCampoUnico = headers.findIndex(h => def.headers[campo]?.map(c => normalizarTexto(c)).includes(normalizarTexto(h)));
        if (indiceCampoUnico !== -1 && datos[campo]) {
            const existe = rows.some(fila => normalizarTexto(fila[indiceCampoUnico]) === normalizarTexto(datos[campo]));
            if (existe) {
                return crearRespuestaError(`El valor "${datos[campo]}" para el campo "${campo}" ya existe y debe ser único.`, {
                    source: contexto,
                    details: { nombreTabla, campo, valor: datos[campo] }
                });
            }
        }
    }

    const nuevaFila = headers.map(header => {
        const clave = Object.keys(def.headers).find(k => def.headers[k].map(c => normalizarTexto(c)).includes(normalizarTexto(header)));
        return datos[clave] ?? '';
    });

    try {
        sheet.appendRow(nuevaFila);
        return { success: true, data: datos, message: 'Registro creado con éxito.' };
    } catch (error) {
        return crearRespuestaError(`Error al escribir en la hoja: ${error.message}`, {
            source: contexto,
            error,
            details: { nombreTabla, sheetName: def.sheetName }
        });
    }
}

/**
 * Lee un único registro de la tabla por su ID.
 * @param {string} nombreTabla La clave de la tabla.
 * @param {string|number} id El ID del registro a buscar.
 * @returns {{ success: boolean, data?: object, message: string }}
 */
function readRow(nombreTabla, id) {
    const contexto = 'readRow';
    const def = TABLE_DEFINITIONS[nombreTabla];
    if (!def) {
        return crearRespuestaError(`Tabla "${nombreTabla}" no definida.`, {
            source: contexto,
            details: { nombreTabla }
        });
    }

    const { headers, rows } = obtenerDatosTabla(def.sheetName);
    const indiceId = headers.findIndex(h => normalizarTexto(h) === normalizarTexto(def.primaryField));
    if (indiceId === -1) {
        return crearRespuestaError('No se encontró la columna ID.', {
            source: contexto,
            details: { nombreTabla, sheetName: def.sheetName }
        });
    }

    const filaEncontrada = rows.find(fila => fila[indiceId] == id);
    if (!filaEncontrada) {
        return crearRespuestaError(`Registro con ID "${id}" no encontrado.`, {
            source: contexto,
            details: { nombreTabla, id }
        });
    }

    const objetoFila = filaAObjeto(def, headers, filaEncontrada);
    return { success: true, data: objetoFila, message: 'Registro encontrado.' };
}

/**
 * Actualiza un registro existente en la tabla por su ID.
 * @param {string} nombreTabla La clave de la tabla.
 * @param {string|number} id El ID del registro a actualizar.
 * @param {object} nuevosDatos Objeto con los campos y valores a modificar.
 * @returns {{ success: boolean, data?: object, message: string }}
 */
function updateRow(nombreTabla, id, nuevosDatos) {
    const contexto = 'updateRow';
    const def = TABLE_DEFINITIONS[nombreTabla];
    if (!def) {
        return crearRespuestaError(`Tabla "${nombreTabla}" no definida.`, {
            source: contexto,
            details: { nombreTabla }
        });
    }

    const { sheet, headers, rows } = obtenerDatosTabla(def.sheetName);
    const indiceId = headers.findIndex(h => normalizarTexto(h) === normalizarTexto(def.primaryField));
    if (indiceId === -1) {
        return crearRespuestaError('No se encontró la columna ID.', {
            source: contexto,
            details: { nombreTabla, sheetName: def.sheetName }
        });
    }

    const indiceFila = rows.findIndex(fila => fila[indiceId] == id);
    if (indiceFila === -1) {
        return crearRespuestaError(`Registro con ID "${id}" no encontrado para actualizar.`, {
            source: contexto,
            details: { nombreTabla, id }
        });
    }

    const filaActualizada = headers.map((header, i) => {
        const clave = Object.keys(def.headers).find(k => def.headers[k].map(c => normalizarTexto(c)).includes(normalizarTexto(header)));
        return nuevosDatos[clave] !== undefined ? nuevosDatos[clave] : rows[indiceFila][i];
    });

    try {
        sheet.getRange(indiceFila + 2, 1, 1, filaActualizada.length).setValues([filaActualizada]);
        return { success: true, data: filaAObjeto(def, headers, filaActualizada), message: 'Registro actualizado con éxito.' };
    } catch (error) {
        return crearRespuestaError(`Error al actualizar la hoja: ${error.message}`, {
            source: contexto,
            error,
            details: { nombreTabla, id }
        });
    }
}

/**
 * Elimina un registro de la tabla por su ID.
 * @param {string} nombreTabla La clave de la tabla.
 * @param {string|number} id El ID del registro a eliminar.
 * @returns {{ success: boolean, message: string }}
 */
function deleteRow(nombreTabla, id) {
    const contexto = 'deleteRow';
    const def = TABLE_DEFINITIONS[nombreTabla];
    if (!def) {
        return crearRespuestaError(`Tabla "${nombreTabla}" no definida.`, {
            source: contexto,
            details: { nombreTabla }
        });
    }

    const { sheet, headers, rows } = obtenerDatosTabla(def.sheetName);
    const indiceId = headers.findIndex(h => normalizarTexto(h) === normalizarTexto(def.primaryField));
    if (indiceId === -1) {
        return crearRespuestaError('No se encontró la columna ID.', {
            source: contexto,
            details: { nombreTabla, sheetName: def.sheetName }
        });
    }

    const indiceFila = rows.findIndex(fila => fila[indiceId] == id);
    if (indiceFila === -1) {
        return crearRespuestaError(`Registro con ID "${id}" no encontrado para eliminar.`, {
            source: contexto,
            details: { nombreTabla, id }
        });
    }

    try {
        sheet.deleteRow(indiceFila + 2);
        return { success: true, message: 'Registro eliminado con éxito.' };
    } catch (error) {
        return crearRespuestaError(`Error al eliminar la fila: ${error.message}`, {
            source: contexto,
            error,
            details: { nombreTabla, id }
        });
    }
}

/**
 * Lee todos los registros de una tabla y los devuelve como un arreglo de objetos.
 * @param {string} nombreTabla La clave de la tabla en TABLE_DEFINITIONS.
 * @returns {{ success: boolean, data?: object[], message: string }}
 */
function readAllRows(nombreTabla) {
    if (typeof confirmarConfiguracion === 'function') confirmarConfiguracion();
    const contexto = 'readAllRows';
    console.log(`[${contexto}] Iniciando lectura de tabla: "${nombreTabla}"`);

    const def = TABLE_DEFINITIONS[nombreTabla];
    if (!def) {
        console.error(`[${contexto}] Tabla no definida: "${nombreTabla}"`);
        return crearRespuestaError(`Tabla "${nombreTabla}" no está definida.`, {
            source: contexto,
            details: { nombreTabla }
        });
    }

    console.log(`[${contexto}] Definición encontrada. sheetName: "${def.sheetName}"`);

    const { headers, rows } = obtenerDatosTabla(def.sheetName);

    console.log(`[${contexto}] obtenerDatosTabla retornó:`, {
        headersLength: headers?.length || 0,
        rowsLength: rows?.length || 0,
        headers: headers
    });

    if (!headers || headers.length === 0) {
        console.warn(`[${contexto}] Headers vacíos para tabla "${nombreTabla}"`);
        return { success: true, data: [], message: 'No se encontraron datos.' };
    }

    const datos = rows.map(fila => filaAObjeto(def, headers, fila));

    console.log(`[${contexto}] Datos procesados para "${nombreTabla}":`, {
        totalRegistros: datos.length,
        primerRegistro: datos[0] || null
    });

    return { success: true, data: datos, message: 'Registros leídos con éxito.' };
}

/**
 * Busca un registro por su identificador primario.
 * @param {string} nombreTabla - Clave de la tabla definida en TABLE_DEFINITIONS.
 * @param {string|number} id - Valor del campo primario a localizar.
 * @return {{ success: boolean, data?: object|null, message?: string }}
 */
function buscarPorId(nombreTabla, id) {
    const contexto = 'buscarPorId';
    try {
        const tabla = String(nombreTabla || '').trim();
        if (!tabla) {
            return crearRespuestaError('Se requiere el nombre de la tabla.', { source: contexto });
        }

        const def = TABLE_DEFINITIONS[tabla];
        if (!def) {
            return crearRespuestaError(`Tabla "${tabla}" no definida.`, {
                source: contexto,
                details: { nombreTabla: tabla }
            });
        }

        const idValor = String(id ?? '').trim();
        if (!idValor) {
            return crearRespuestaError('Se requiere el ID para realizar la búsqueda.', {
                source: contexto,
                details: { nombreTabla: tabla }
            });
        }

        const response = readAllRows(tabla);
        if (!response || response.success !== true || !Array.isArray(response.data)) {
            return propagarRespuestaError(contexto, response, {
                message: response?.message || `No fue posible leer los registros de "${tabla}".`
            });
        }

        const registro = response.data.find(row => String(row?.id ?? '').trim() === idValor) || null;
        if (!registro) {
            return crearRespuestaError(`No se encontró un registro con ID "${idValor}" en la tabla "${tabla}".`, {
                source: contexto,
                details: { nombreTabla: tabla, id: idValor }
            });
        }

        return { success: true, data: registro };
    } catch (error) {
        console.error('Error en buscarPorId:', error, { nombreTabla, id });
        return crearRespuestaError(error.message, {
            source: contexto,
            error,
            details: { nombreTabla, id }
        });
    }
}

/**
 * Busca un registro por el valor de un campo específico.
 * @param {string} nombreTabla - Clave de la tabla definida en TABLE_DEFINITIONS.
 * @param {string} campo - Nombre lógico del campo a comparar.
 * @param {*} valor - Valor a comparar (se compara como string normalizado).
 * @return {{ success: boolean, data?: object|null, message?: string }}
 */
function buscarPorCampo(nombreTabla, campo, valor) {
    const contexto = 'buscarPorCampo';
    try {
        const tabla = String(nombreTabla || '').trim();
        if (!tabla) {
            return crearRespuestaError('Se requiere el nombre de la tabla.', { source: contexto });
        }

        const def = TABLE_DEFINITIONS[tabla];
        if (!def) {
            return crearRespuestaError(`Tabla "${tabla}" no definida.`, {
                source: contexto,
                details: { nombreTabla: tabla }
            });
        }

        const campoBusqueda = String(campo || '').trim();
        if (!campoBusqueda) {
            return crearRespuestaError('Se requiere el nombre del campo para realizar la búsqueda.', {
                source: contexto,
                details: { nombreTabla: tabla }
            });
        }

        const response = readAllRows(tabla);
        if (!response || response.success !== true || !Array.isArray(response.data)) {
            return propagarRespuestaError(contexto, response, {
                message: response?.message || `No fue posible leer los registros de "${tabla}".`
            });
        }

        const normalizedField = normalizarTexto(campoBusqueda);
        const normalizedValue = String(valor ?? '').trim().toLowerCase();

        const registro = response.data.find(row => {
            if (!row || typeof row !== 'object') return false;
            const matchKey = Object.keys(row).find(key => normalizarTexto(key) === normalizedField);
            if (!matchKey) return false;
            const rowValue = row[matchKey];
            if (rowValue instanceof Date && valor instanceof Date) {
                return rowValue.getTime() === valor.getTime();
            }
            return String(rowValue ?? '').trim().toLowerCase() === normalizedValue;
        }) || null;

        if (!registro) {
            return {
                success: true,
                data: null,
                message: `No se encontró un registro en "${tabla}" donde "${campoBusqueda}" coincida con el valor solicitado.`
            };
        }

        return { success: true, data: registro };
    } catch (error) {
        console.error('Error en buscarPorCampo:', error, { nombreTabla, campo, valor });
        return crearRespuestaError(error.message, {
            source: contexto,
            error,
            details: { nombreTabla, campo, valor }
        });
    }
}

/**
 * Wrappers en español para mantener compatibilidad con módulos existentes.
 * Delegan en las funciones CRUD estándar.
 */
function insertarRegistro(nombreTabla, datos) {
    const resultado = createRow(nombreTabla, datos);
    return resultado && resultado.success === false
        ? propagarRespuestaError('insertarRegistro', resultado)
        : resultado;
}

function actualizarRegistro(nombreTabla, id, nuevosDatos) {
    const resultado = updateRow(nombreTabla, id, nuevosDatos);
    return resultado && resultado.success === false
        ? propagarRespuestaError('actualizarRegistro', resultado)
        : resultado;
}

function eliminarRegistro(nombreTabla, id) {
    const resultado = deleteRow(nombreTabla, id);
    return resultado && resultado.success === false
        ? propagarRespuestaError('eliminarRegistro', resultado)
        : resultado;
}

function debugReadCampo() {
    const resultado = buscarPorCampo('datosGenerales', 'idComunicado', 1)
    console.log(resultado, typeof resultado);
}

/**
 * Garantiza que exista un registro en el catálogo indicado.
 * Si el registro ya existe (búsqueda por campo principal), devuelve su ID.
 * Si no existe, lo crea y devuelve el ID del nuevo registro.
 * 
 * @param {string} catalogKey Clave del catálogo en TABLE_DEFINITIONS (ej: 'distritosRiego', 'siniestros').
 * @param {Object} data Datos del registro a buscar/crear. Debe incluir el campo principal del catálogo.
 * @returns {{ success: boolean, created?: boolean, id?: string|number, data?: Object|null, message?: string }}
 */
function ensureCatalogRecord(catalogKey, data) {
    try {
        const definition = TABLE_DEFINITIONS[catalogKey];
        if (!definition) {
            return { success: false, message: `No existe el catálogo "${catalogKey}".` };
        }

        if (!data || typeof data !== 'object') {
            return { success: true, created: false, id: '', data: null };
        }

        // Determinar el campo principal del catálogo
        const primaryField = definition.primaryField
            || definition.nameField
            || Object.keys(definition.headers || {})[0];

        if (!primaryField) {
            return { success: false, message: `No se pudo determinar el campo principal del catálogo "${catalogKey}".` };
        }

        // Obtener el valor a buscar
        const searchValue = data[primaryField] || data.nombre || '';
        if (!searchValue || String(searchValue).trim() === '') {
            return { success: true, created: false, id: '', data: null };
        }

        const normalizedSearch = String(searchValue).trim().toLowerCase();

        // Buscar si ya existe
        const allRecords = readAllRows(catalogKey);
        if (allRecords.success && allRecords.data) {
            const existing = allRecords.data.find(record => {
                const recordValue = record[primaryField] || record.nombre || '';
                return String(recordValue).trim().toLowerCase() === normalizedSearch;
            });

            if (existing) {
                return {
                    success: true,
                    created: false,
                    id: existing.id || '',
                    data: existing
                };
            }
        }

        // No existe, crear nuevo registro
        const createResult = createRow(catalogKey, data);
        if (createResult.success) {
            return {
                success: true,
                created: true,
                id: createResult.data?.id || '',
                data: createResult.data || null
            };
        } else {
            return {
                success: false,
                message: createResult.message || 'Error al crear registro en catálogo.'
            };
        }

    } catch (error) {
        console.error('Error en ensureCatalogRecord:', error);
        return { success: false, message: error.message };
    }
}

/**
 * FUNCIÓN DE DIAGNÓSTICO - Ejecutar desde el editor para probar
 * Ve a Ejecutar > Ejecutar función > debugReadCuentas
 * Luego revisa los logs en Ejecuciones
 */
function debugReadCuentas() {
    console.log('=== INICIO DIAGNÓSTICO ===');

    // 1. Verificar TABLE_DEFINITIONS
    const def = TABLE_DEFINITIONS['cuentas'];
    console.log('1. TABLE_DEFINITIONS.cuentas:', JSON.stringify(def, null, 2));

    // 2. Verificar que la hoja existe
    const hoja = SpreadsheetApp.getActive().getSheetByName(def.sheetName);
    console.log('2. Hoja encontrada:', !!hoja, 'Nombre:', def.sheetName);

    if (!hoja) {
        console.error('ERROR: La hoja no existe');
        return;
    }

    // 3. Leer datos directamente
    const valores = hoja.getDataRange().getValues();
    console.log('3. Total filas (incluyendo headers):', valores.length);
    console.log('4. Primera fila (headers):', JSON.stringify(valores[0]));
    if (valores.length > 1) {
        console.log('5. Segunda fila (primer dato):', JSON.stringify(valores[1]));
    }

    // 4. Probar obtenerDatosTabla
    const datosTabla = obtenerDatosTabla(def.sheetName);
    console.log('6. obtenerDatosTabla headers:', JSON.stringify(datosTabla.headers));
    console.log('7. obtenerDatosTabla rows count:', datosTabla.rows.length);

    // 5. Probar filaAObjeto con la primera fila
    if (datosTabla.rows.length > 0) {
        const objetoPrueba = filaAObjeto(def, datosTabla.headers, datosTabla.rows[0]);
        console.log('8. filaAObjeto resultado:', JSON.stringify(objetoPrueba));
    }

    // 6. Probar readAllRows completo
    const resultado = readAllRows('cuentas');
    console.log('9. readAllRows resultado:', JSON.stringify(resultado, null, 2));

    console.log('=== FIN DIAGNÓSTICO ===');
    return resultado;
}