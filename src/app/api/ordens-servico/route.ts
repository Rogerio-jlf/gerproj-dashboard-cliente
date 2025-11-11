import { firebirdQuery } from '../../../lib/firebird/firebird-client';
import { NextResponse } from 'next/server';

// ==================== TIPOS ====================
interface QueryParams {
  isAdmin: boolean;
  codCliente?: string;
  mes: number;
  ano: number;
  codClienteFilter?: string;
  codRecursoFilter?: string;
  status?: string;
}

interface OS {
  COD_OS: number;
  DTINI_OS: string;
  HRINI_OS: string;
  HRFIM_OS: string;
  OBS_OS: string;
  STATUS_OS: string;
  CHAMADO_OS?: string;
  NUM_OS: string;
  COMP_OS: string;
  DTINC_OS: string;
  CODTRF_OS?: number;
  COD_CLIENTE: number;
  NOME_CLIENTE: string;
  COD_RECURSO: number;
  NOME_RECURSO: string;
  STATUS_CHAMADO?: string;
}

interface OSComHoras extends OS {
  TOTAL_HRS_OS: number;
}

// ==================== VALIDAÇÕES ====================
function validarParametros(searchParams: URLSearchParams): QueryParams | NextResponse {
  const isAdmin = searchParams.get('isAdmin') === 'true';
  const codCliente = searchParams.get('codCliente')?.trim();
  const mes = Number(searchParams.get('mes'));
  const ano = Number(searchParams.get('ano'));

  if (!mes || mes < 1 || mes > 12) {
    return NextResponse.json(
      { error: "Parâmetro 'mes' deve ser um número entre 1 e 12" },
      { status: 400 }
    );
  }

  if (!ano || ano < 2000 || ano > 3000) {
    return NextResponse.json(
      { error: "Parâmetro 'ano' deve ser um número válido" },
      { status: 400 }
    );
  }

  if (!isAdmin && !codCliente) {
    return NextResponse.json(
      { error: "Parâmetro 'codCliente' é obrigatório para usuários não admin" },
      { status: 400 }
    );
  }

  return {
    isAdmin,
    codCliente,
    mes,
    ano,
    codClienteFilter: searchParams.get('codClienteFilter')?.trim(),
    codRecursoFilter: searchParams.get('codRecursoFilter')?.trim(),
    status: searchParams.get('status') || undefined
  };
}

// ==================== CONSTRUÇÃO DE DATAS ====================
function construirDatas(mes: number, ano: number): { dataInicio: string; dataFim: string } {
  const mesFormatado = mes.toString().padStart(2, '0');
  const dataInicio = `01.${mesFormatado}.${ano}`;
  
  const dataFim = mes === 12 
    ? `01.01.${ano + 1}`
    : `01.${(mes + 1).toString().padStart(2, '0')}.${ano}`;

  return { dataInicio, dataFim };
}

// ==================== CONSTRUÇÃO DE SQL ====================
const SQL_BASE = `
  SELECT 
    OS.COD_OS,
    OS.DTINI_OS,
    OS.HRINI_OS,
    OS.HRFIM_OS,
    OS.OBS_OS,
    OS.STATUS_OS,
    OS.CHAMADO_OS,
    OS.NUM_OS,
    OS.COMP_OS,
    OS.DTINC_OS,
    OS.CODTRF_OS,
    CLIENTE.COD_CLIENTE,
    CLIENTE.NOME_CLIENTE,
    RECURSO.COD_RECURSO,
    RECURSO.NOME_RECURSO,
    CHAMADO.STATUS_CHAMADO
  FROM OS
  LEFT JOIN TAREFA ON OS.CODTRF_OS = TAREFA.COD_TAREFA
  LEFT JOIN PROJETO ON TAREFA.CODPRO_TAREFA = PROJETO.COD_PROJETO
  LEFT JOIN CLIENTE ON PROJETO.CODCLI_PROJETO = CLIENTE.COD_CLIENTE
  LEFT JOIN RECURSO ON OS.CODREC_OS = RECURSO.COD_RECURSO
  LEFT JOIN CHAMADO ON CAST(OS.CHAMADO_OS AS INTEGER) = CHAMADO.COD_CHAMADO
  WHERE OS.DTINI_OS >= ? AND OS.DTINI_OS < ?
`;

const SQL_TOTALIZADORES_BASE = `
  SELECT 
    COUNT(*) AS TOTAL_OS,
    COUNT(DISTINCT OS.CHAMADO_OS) AS TOTAL_CHAMADOS,
    COUNT(DISTINCT RECURSO.COD_RECURSO) AS TOTAL_RECURSOS
  FROM OS
  LEFT JOIN TAREFA ON OS.CODTRF_OS = TAREFA.COD_TAREFA
  LEFT JOIN PROJETO ON TAREFA.CODPRO_TAREFA = PROJETO.COD_PROJETO
  LEFT JOIN CLIENTE ON PROJETO.CODCLI_PROJETO = CLIENTE.COD_CLIENTE
  LEFT JOIN RECURSO ON OS.CODREC_OS = RECURSO.COD_RECURSO
  LEFT JOIN CHAMADO ON CAST(OS.CHAMADO_OS AS INTEGER) = CHAMADO.COD_CHAMADO
  WHERE OS.DTINI_OS >= ? AND OS.DTINI_OS < ?
`;

function aplicarFiltros(
  sqlBase: string,
  params: QueryParams,
  paramsArray: any[]
): { sql: string; params: any[] } {
  let sql = sqlBase;

  // Filtro obrigatório para não-admin
  if (!params.isAdmin && params.codCliente) {
    sql += ` AND CLIENTE.COD_CLIENTE = ?`;
    paramsArray.push(parseInt(params.codCliente));
  }
  
  // Filtros opcionais
  if (params.codClienteFilter) {
    sql += ` AND CLIENTE.COD_CLIENTE = ?`;
    paramsArray.push(parseInt(params.codClienteFilter));
  }

  if (params.codRecursoFilter) {
    sql += ` AND RECURSO.COD_RECURSO = ?`;
    paramsArray.push(parseInt(params.codRecursoFilter));
  }

  if (params.status) {
    sql += ` AND UPPER(CHAMADO.STATUS_CHAMADO) LIKE UPPER(?)`;
    paramsArray.push(`%${params.status}%`);
  }

  return { sql, params: paramsArray };
}

// ==================== CÁLCULOS ====================
function calcularHorasTrabalhadas(hrIni: string = '0000', hrFim: string = '0000'): number {
  const horaIni = parseInt(hrIni.substring(0, 2));
  const minIni = parseInt(hrIni.substring(2, 4));
  const horaFim = parseInt(hrFim.substring(0, 2));
  const minFim = parseInt(hrFim.substring(2, 4));
  
  const totalMinutos = (horaFim * 60 + minFim) - (horaIni * 60 + minIni);
  return parseFloat((totalMinutos / 60).toFixed(2));
}

function processarChamadosComHoras(chamados: OS[]): OSComHoras[] {
  return chamados.map(chamado => ({
    ...chamado,
    TOTAL_HRS_OS: calcularHorasTrabalhadas(chamado.HRINI_OS, chamado.HRFIM_OS)
  }));
}

function agruparHoras(chamados: OSComHoras[]): {
  horasPorChamado: Map<string, number>;
  horasPorTarefa: Map<number, number>;
} {
  const horasPorChamado = new Map<string, number>();
  const horasPorTarefa = new Map<number, number>();
  
  chamados.forEach(os => {
    const horas = os.TOTAL_HRS_OS || 0;
    
    if (os.CHAMADO_OS) {
      const atual = horasPorChamado.get(os.CHAMADO_OS) || 0;
      horasPorChamado.set(os.CHAMADO_OS, atual + horas);
    } else if (os.CODTRF_OS) {
      const atual = horasPorTarefa.get(os.CODTRF_OS) || 0;
      horasPorTarefa.set(os.CODTRF_OS, atual + horas);
    }
  });

  return { horasPorChamado, horasPorTarefa };
}

function calcularTotalizadores(
  chamados: OSComHoras[],
  horasPorChamado: Map<string, number>,
  horasPorTarefa: Map<number, number>,
  totaisDB: any
) {
  const totalHoras = chamados.reduce((acc, os) => acc + (os.TOTAL_HRS_OS || 0), 0);
  
  const totalChamadosComHoras = horasPorChamado.size;
  const totalHorasChamados = Array.from(horasPorChamado.values()).reduce((acc, h) => acc + h, 0);
  const mediaHorasPorChamado = totalChamadosComHoras > 0 
    ? totalHorasChamados / totalChamadosComHoras 
    : 0;

  const totalTarefasComHoras = horasPorTarefa.size;
  const totalHorasTarefas = Array.from(horasPorTarefa.values()).reduce((acc, h) => acc + h, 0);
  const mediaHorasPorTarefa = totalTarefasComHoras > 0 
    ? totalHorasTarefas / totalTarefasComHoras 
    : 0;

  return {
    ...(totaisDB[0] || {
      TOTAL_OS: 0,
      TOTAL_CHAMADOS: 0,
      TOTAL_RECURSOS: 0
    }),
    TOTAL_HRS: parseFloat(totalHoras.toFixed(2)),
    TOTAL_HRS_CHAMADOS: parseFloat(totalHorasChamados.toFixed(2)),
    TOTAL_HRS_TAREFAS: parseFloat(totalHorasTarefas.toFixed(2)),
    MEDIA_HRS_POR_CHAMADO: parseFloat(mediaHorasPorChamado.toFixed(2)),
    MEDIA_HRS_POR_TAREFA: parseFloat(mediaHorasPorTarefa.toFixed(2)),
    TOTAL_CHAMADOS_COM_HORAS: totalChamadosComHoras,
    TOTAL_TAREFAS_COM_HORAS: totalTarefasComHoras
  };
}

// ==================== HANDLER PRINCIPAL ====================
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    
    // Validar parâmetros
    const params = validarParametros(searchParams);
    if (params instanceof NextResponse) return params;

    // Construir datas
    const { dataInicio, dataFim } = construirDatas(params.mes, params.ano);

    // Construir query principal
    const { sql: sqlPrincipal, params: paramsPrincipal } = aplicarFiltros(
      SQL_BASE,
      params,
      [dataInicio, dataFim]
    );
    const sqlFinal = `${sqlPrincipal} ORDER BY OS.DTINI_OS DESC, OS.HRINI_OS DESC`;

    // Construir query de totalizadores
    const { sql: sqlTotais, params: paramsTotais } = aplicarFiltros(
      SQL_TOTALIZADORES_BASE,
      params,
      [dataInicio, dataFim]
    );

    // Executar queries em paralelo para melhor performance
    const [chamados, totalizadoresDB] = await Promise.all([
      firebirdQuery(sqlFinal, paramsPrincipal),
      firebirdQuery(sqlTotais, paramsTotais)
    ]);

    // Processar dados
    const chamadosComHoras = processarChamadosComHoras(chamados);
    const { horasPorChamado, horasPorTarefa } = agruparHoras(chamadosComHoras);
    const totalizadores = calcularTotalizadores(
      chamadosComHoras,
      horasPorChamado,
      horasPorTarefa,
      totalizadoresDB
    );

    return NextResponse.json({
      chamados: chamadosComHoras,
      totalizadores
    });

  } catch (error) {
    console.error('Erro ao buscar chamados:', error);
    console.error('Stack trace:', error instanceof Error ? error.stack : 'N/A');
    console.error('Message:', error instanceof Error ? error.message : error);
    
    return NextResponse.json(
      { 
        error: 'Erro no servidor',
        message: error instanceof Error ? error.message : 'Erro desconhecido',
        details: process.env.NODE_ENV === 'development' ? error : undefined
      },
      { status: 500 }
    );
  }
}