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
 * DEFINICIÓN DE TABLAS DE LA BASE DE DATOS
 * Actualizado al esquema: 12 de Diciembre 2025
 * Basado en archivos CSV: dbComunicados (Referencias, Ajustadores, Aseguradoras, etc.)
 */
const TABLE_DEFINITIONS = {

    // === MÓDULO PRINCIPAL ===

    cuentas: {
        sheetName: 'Referencias',
        primaryField: 'id',
        headers: ['id', 'referencia', 'idAjustador'],
        requiredFields: ['referencia'],
        uniqueFields: []
    },

    comunicados: {
        sheetName: 'Comunicados',
        primaryField: 'id',
        headers: ['id', 'idReferencia', 'comunicado', 'status', 'idSustituido'],
        requiredFields: ['idReferencia', 'comunicado'],
        uniqueFields: []
    },

    datosGenerales: {
        sheetName: 'DatosGenerales',
        primaryField: 'id',
        headers: [
            'id',
            'idComunicado',
            'descripcion',
            'fecha',
            'idEstado',
            'idDR',
            'idEmpresa',
            'fechaAsignacion',
            'idSiniestro',
            'idActualizacion',
            'idAjustador' // Asegurando compatibilidad con createRow/datosGen
        ],
        requiredFields: ['idComunicado'],
        uniqueFields: []
    },

    // === NUEVOS CATÁLOGOS Y RELACIONES ===

    ajustadores: {
        sheetName: 'Ajustadores',
        primaryField: 'id',
        headers: ['id', 'nombreAjustador', 'nombre'],
        requiredFields: [],
        uniqueFields: []
    },

    aseguradoras: {
        sheetName: 'Aseguradoras',
        primaryField: 'id',
        headers: ['id', 'descripción'],
        requiredFields: [],
        uniqueFields: []
    },

    empresas: {
        sheetName: 'Empresas',
        primaryField: 'id',
        headers: ['id', 'razonSocial'],
        requiredFields: [],
        uniqueFields: []
    },

    // === CATÁLOGOS GEOGRÁFICOS Y DE SINIESTROS ===

    estados: {
        sheetName: 'Estados',
        primaryField: 'id',
        headers: ['id', 'estado'],
        requiredFields: [],
        uniqueFields: []
    },

    distritosRiego: {
        sheetName: 'DistritosRiego',
        primaryField: 'id',
        headers: ['id', 'distritoRiego'],
        requiredFields: [],
        uniqueFields: []
    },

    siniestros: {
        sheetName: 'Siniestros',
        primaryField: 'id',
        headers: ['id', 'siniestro', 'fenomeno', 'fondo', 'fi', 'idAseguradora'],
        requiredFields: ['siniestro'],
        uniqueFields: []
    },

    // === PRESUPUESTO Y FINANZAS ===

    actualizaciones: {
        sheetName: 'Actualizaciones',
        primaryField: 'id',
        headers: [
            'id',
            'idComunicado',
            'consecutivo',
            'esOrigen',
            'revision',
            'montoCapturado',
            'monto',
            'idPresupuesto',
            'fecha'
        ],
        requiredFields: ['idComunicado'],
        uniqueFields: []
    },

    presupuestos: {
        sheetName: 'Presupuestos',
        primaryField: 'id',
        headers: [
            'id',
            'idPadre',
            'esPartida',
            'consecutivo',
            'codigo',
            'descripcion',
            'unidad',
            'fecha'
        ],
        requiredFields: [],
        uniqueFields: []
    },

    detallePresupuesto: {
        sheetName: 'DetallePresupuesto',
        primaryField: 'id',
        headers: [
            'id',
            'idActualizacion',
            'idPresupuesto',
            'cantidad',
            'precioUnitario',
            'importe'
        ],
        requiredFields: [],
        uniqueFields: []
    },

    // === SISTEMA ===

    bitacora: {
        sheetName: 'Bitacora',
        primaryField: 'id',
        headers: ['id', 'idComunicado', 'tipo', 'fecha', 'registro'],
        requiredFields: [],
        uniqueFields: []
    }
};

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

/**
 * Wrapper específico para obtener cuentas.
 * Requerido por la lógica del cliente en script.html.
 */
function readCuentas() {
    return readAllRows('cuentas');
}