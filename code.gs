/**
 * Punto de entrada principal para la aplicación web.
 * Esta función se ejecuta cuando alguien visita la URL de la aplicación.
 */
function doGet(e) {
    return HtmlService.createTemplateFromFile('index')
        .evaluate()
        .setTitle('Gestor de Comunicados')
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * Permite incrustar archivos HTML dentro de otros, útil para plantillas.
 * @param {string} nombreArchivo El nombre del archivo HTML a incluir (sin la extensión .html).
 * @returns {string} El contenido del archivo HTML.
 */
function include(nombreArchivo) {
    return HtmlService.createHtmlOutputFromFile(nombreArchivo).getContent();
}

/**
 * =================================================================
 * CONFIGURACIÓN CENTRAL DE TABLAS (ÚNICA FUENTE DE VERDAD)
 * =================================================================
 * Este objeto define la estructura, reglas y nombres para cada hoja
 * de cálculo que funciona como una tabla en la base de datos.
 */
const TABLE_DEFINITIONS = Object.freeze({

    // -- Definición para la tabla de Referencias (antes Cuentas) --
    cuentas: {
        sheetName: 'Referencias',
        primaryField: 'id',
        nameField: 'referencia',
        requiredFields: ['referencia'],
        uniqueFields: ['id', 'referencia'],
        headers: {
            id: ['id', 'ID', 'Folio'],
            referencia: ['referencia', 'Referencia', 'cuenta', 'Cuenta', 'NombreCuenta'],
            idAjustador: ['idAjustador', 'AjustadorId', 'Ajustador']
        }
    },

    // -- Definición para la tabla de Comunicados --
    comunicados: {
        sheetName: 'Comunicados',
        primaryField: 'id',
        requiredFields: ['idCuenta', 'comunicado'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID', 'Folio'],
            idCuenta: ['idCuenta', 'CuentaId', 'Cuenta'],
            comunicado: ['comunicado', 'Comunicado'],
            idDatosGenerales: ['idDatosGenerales', 'DatosGeneralesId', 'IdDetalle'],
            status: ['status', 'Estatus']
        }
    },

    // -- Definición para la tabla de DatosGenerales --
    datosGenerales: {
        sheetName: 'DatosGenerales',
        primaryField: 'id',
        requiredFields: ['descripcion', 'fecha'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID', 'Folio'],
            idComunicado: ['idComunicado', 'ComunicadoId'],
            descripcion: ['descripcion', 'Descripción', 'Detalle'],
            fecha: ['fecha', 'Fecha'],
            idEstado: ['idEstado', 'EstadoId', 'Estado'],
            idDR: ['idDR', 'IdDistritoRiego', 'DistritoRiegoId'],
            // idAjustador REMOVIDO DE AQUÍ
            fechaAsignacion: ['fechaAsignacion', 'FechaAsignacion'],
            idSiniestro: ['idSiniestro', 'SiniestroId'],
            idActualizacion: ['idActualizacion', 'ActualizacionId']
        }
    },

    // -- Definición para catálogos --
    estados: {
        sheetName: 'Estados',
        primaryField: 'id',
        requiredFields: ['estado'],
        uniqueFields: ['id', 'estado'],
        headers: {
            id: ['id', 'ID', 'IdEstado', 'id_estado', 'clave', 'Clave'],
            estado: ['estado', 'Estado', 'nombre', 'Nombre', 'entidad', 'Entidad', 'Nombre Estado', 'Nombre Entidad']
        }
    },
    distritosRiego: {
        sheetName: 'DistritosRiego',
        primaryField: 'id',
        nameField: 'distritoRiego', // Campo por el cual buscar/comparar
        requiredFields: ['distritoRiego'],
        uniqueFields: ['id', 'distritoRiego'],
        headers: {
            id: ['id', 'ID', 'IdDistrito'],
            distritoRiego: ['distritoRiego', 'Distrito', 'Nombre']
        }
    },
    siniestros: {
        sheetName: 'Siniestros',
        primaryField: 'id',
        nameField: 'siniestro', // Campo por el cual buscar/comparar
        requiredFields: ['siniestro'],
        uniqueFields: ['id', 'siniestro'],
        headers: {
            id: ['id', 'ID', 'IdSiniestro'],
            siniestro: ['siniestro', 'Siniestro', 'Nombre'],
            fenomeno: ['fenomeno', 'Fenomeno'],
            fondo: ['fondo', 'Fondo'],
            fi: ['fi', 'FI']
        }
    },
    aseguradoras: {
        sheetName: 'Aseguradoras',
        primaryField: 'id',
        requiredFields: ['descripcion'],
        uniqueFields: ['id', 'descripcion'],
        headers: {
            id: ['id', 'ID', 'IdAseguradora'],
            descripcion: ['descripcion', 'Aseguradora', 'Nombre']
        }
    },
    empresas: {
        sheetName: 'Empresas',
        primaryField: 'id',
        requiredFields: ['razonSocial'],
        uniqueFields: ['id', 'razonSocial'],
        headers: {
            id: ['id', 'ID', 'IdEmpresa'],
            razonSocial: ['razonSocial', 'RazonSocial', 'Razón Social', 'Empresa']
        }
    },
    ajustadores: {
        sheetName: 'Ajustadores',
        primaryField: 'id',
        nameField: 'nombreAjustador',
        requiredFields: ['nombreAjustador'],
        uniqueFields: ['id', 'nombreAjustador'],
        headers: {
            id: ['id', 'ID', 'IdAjustador'],
            nombreAjustador: ['nombreAjustador', 'NombreAjustador', 'Ajustador'],
            nombre: ['nombre', 'Nombre', 'Abreviatura']
        }
    },

    // -- Definición para tablas de seguimiento y presupuestos --
    bitacora: {
        sheetName: 'Bitacora',
        primaryField: 'id',
        requiredFields: ['fecha', 'usuario', 'accion'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID'],
            fecha: ['fecha', 'Fecha'],
            usuario: ['usuario', 'Usuario'],
            accion: ['accion', 'Accion'],
            detalle: ['detalle', 'Detalle']
        }
    },
    actualizaciones: {
        sheetName: 'Actualizaciones',
        primaryField: 'id',
        requiredFields: ['idComunicado'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID'],
            idComunicado: ['idComunicado', 'ComunicadoId'],
            consecutivo: ['consecutivo', 'Consecutivo'],
            esOrigen: ['esOrigen', 'EsOrigen'],
            revision: ['revision', 'Revision'],
            montoCapturado: ['montoCapturado', 'MontoCapturado'],
            monto: ['monto', 'Monto'],
            idPresupuesto: ['idPresupuesto', 'PresupuestoId'],
            fecha: ['fecha', 'Fecha']
        }
    },
    presupuestos: {
        sheetName: 'Presupuestos',
        primaryField: 'id',
        requiredFields: ['idActualizacion', 'fecha', 'total'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID', 'Folio'],
            idActualizacion: ['idActualizacion', 'ActualizacionId'],
            fecha: ['fecha', 'Fecha'],
            total: ['total', 'Total'],
            status: ['status', 'Estatus'],
            vigente: ['vigente', 'Vigente']
        }
    },
    detallePresupuesto: {
        sheetName: 'DetallePresupuesto',
        primaryField: 'id',
        requiredFields: ['idPresupuesto', 'concepto', 'cantidad', 'precioUnitario'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID'],
            idPresupuesto: ['idPresupuesto', 'PresupuestoId'],
            concepto: ['concepto', 'Concepto'],
            cantidad: ['cantidad', 'Cantidad'],
            precioUnitario: ['precioUnitario', 'PrecioUnitario', 'Precio Unitario'],
            subtotal: ['subtotal', 'Subtotal']
        }
    },

    // -- NUEVAS TABLAS --
    equipo: {
        sheetName: 'Equipo',
        primaryField: 'id',
        requiredFields: ['idComunicado', 'tipo', 'nombre'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID'],
            idComunicado: ['idComunicado', 'ComunicadoId'],
            tipo: ['tipo', 'Tipo'],
            nombre: ['nombre', 'Nombre'],
            detalles: ['detalles', 'Detalles', 'Cargo']
        }
    },
    financiero: {
        sheetName: 'Financiero',
        primaryField: 'id',
        requiredFields: ['idComunicado', 'tipo', 'monto'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID'],
            idComunicado: ['idComunicado', 'ComunicadoId'],
            tipo: ['tipo', 'Tipo'],
            numero: ['numero', 'Numero', 'Folio'],
            fecha: ['fecha', 'Fecha'],
            monto: ['monto', 'Monto'],
            status: ['status', 'Estatus']
        }
    },
    tickets: {
        sheetName: 'Tickets',
        primaryField: 'id',
        requiredFields: ['idComunicado', 'titulo'],
        uniqueFields: ['id'],
        headers: {
            id: ['id', 'ID'],
            idComunicado: ['idComunicado', 'ComunicadoId'],
            titulo: ['titulo', 'Titulo'],
            descripcion: ['descripcion', 'Descripcion'],
            prioridad: ['prioridad', 'Prioridad'],
            estado: ['estado', 'Estado', 'Estatus'],
            fechaCreacion: ['fechaCreacion', 'FechaCreacion']
        }
    }
});

function getAppInfo() {
    return {
        appName: 'App Comunicados',
        version: '1.0.0'
    };
}

function getTemplates(names) {
    if (!Array.isArray(names)) {
        // Comportamiento original si no se pasa un array
        return {
            sidebar: include('sidebar'),
            header: include('header')
        };
    }

    const templates = {};
    names.forEach(name => {
        try {
            // Si el nombre termina en .js, agregamos .html
            // Si no termina en .html, también lo agregamos
            let fileName = name;
            if (name.endsWith('.js')) {
                fileName = `${name}.html`;
            } else if (!name.endsWith('.html')) {
                fileName = name;
            }
            templates[name] = include(fileName);
        } catch (e) {
            Logger.log(`No se pudo cargar la plantilla: ${name}. Error: ${e.message}`);
            templates[name] = `<!-- Error: plantilla ${name} no encontrada -->`;
        }
    });
    return templates;
}