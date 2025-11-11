import { firebirdQuery } from '@/lib/firebird/firebird-client';
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

interface Apontamento {
  COD_CLIENTE?: number;
  NOME_CLIENTE?: string;
  COD_TAREFA?: number;
  LIMMES_TAREFA?: number;
  HRINI_OS?: string;
  HRFIM_OS?: string;
}

interface DadosCliente {
  nome_cliente: string | null;
  limmes_tarefas: number[];
  tarefasDistintas: Set<number>;
  horasExecutadas: number;
}

interface DetalheCliente {
  cod_cliente: string;
  nome_cliente: string | null;
  horasContratadas: number;
  horasExecutadas: number;
  totalLimmesTarefas: number;
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
    CLIENTE.COD_CLIENTE,
    CLIENTE.NOME_CLIENTE,
    TAREFA.COD_TAREFA,
    TAREFA.LIMMES_TAREFA,
    OS.HRINI_OS,
    OS.HRFIM_OS
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

// ==================== CÁLCULOS DE TEMPO ====================
function converterHoraParaMinutos(hora: string | null): number {
  if (!hora) return 0;

  const horaStr = hora.toString().padStart(4, '0');
  const horas = parseInt(horaStr.substring(0, 2)) || 0;
  const minutos = parseInt(horaStr.substring(2, 4)) || 0;

  return horas * 60 + minutos;
}

function calcularDiferencaHoras(hrInicio: string | null, hrFim: string | null): number {
  if (!hrInicio || !hrFim) return 0;

  const minutosInicio = converterHoraParaMinutos(hrInicio);
  const minutosFim = converterHoraParaMinutos(hrFim);

  let diferencaMinutos = minutosFim - minutosInicio;

  // Se a hora fim for menor que a início, assume que passou para o dia seguinte
  if (diferencaMinutos < 0) {
    diferencaMinutos += 24 * 60;
  }

  return diferencaMinutos / 60;
}

// ==================== PROCESSAMENTO DE DADOS ====================
function agruparPorCliente(apontamentos: Apontamento[]): Map<string, DadosCliente> {
  const clientesMap = new Map<string, DadosCliente>();

  apontamentos.forEach(apontamento => {
    const codCliente = apontamento.COD_CLIENTE?.toString() || 'SEM_CODIGO';

    // Inicializa cliente se não existir
    if (!clientesMap.has(codCliente)) {
      clientesMap.set(codCliente, {
        nome_cliente: apontamento.NOME_CLIENTE || null,
        limmes_tarefas: [],
        tarefasDistintas: new Set<number>(),
        horasExecutadas: 0,
      });
    }

    const cliente = clientesMap.get(codCliente)!;

    // Adiciona limmes_tarefa e registra tarefa distinta
    if (apontamento.LIMMES_TAREFA) {
      cliente.limmes_tarefas.push(apontamento.LIMMES_TAREFA);
      
      if (apontamento.COD_TAREFA) {
        cliente.tarefasDistintas.add(apontamento.COD_TAREFA);
      }
    }

    // Calcula e acumula horas executadas
    const horasApontamento = calcularDiferencaHoras(
      apontamento.HRINI_OS || null,
      apontamento.HRFIM_OS || null
    );
    
    cliente.horasExecutadas += horasApontamento;
  });

  return clientesMap;
}

function arredondar(valor: number): number {
  return Math.round(valor * 100) / 100;
}

function calcularDetalhesClientes(clientesMap: Map<string, DadosCliente>): {
  detalhes: DetalheCliente[];
  totalContratadas: number;
  totalExecutadas: number;
} {
  let totalHorasContratadas = 0;
  let totalHorasExecutadas = 0;

  const detalhesClientes = Array.from(clientesMap.entries()).map(
    ([codCliente, dados]) => {
      // Maior limmes_tarefa para este cliente
      const horasContratadasCliente =
        dados.limmes_tarefas.length > 0
          ? Math.max(...dados.limmes_tarefas)
          : 0;

      const horasExecutadasCliente = arredondar(dados.horasExecutadas);

      totalHorasContratadas += horasContratadasCliente;
      totalHorasExecutadas += horasExecutadasCliente;

      return {
        cod_cliente: codCliente,
        nome_cliente: dados.nome_cliente,
        horasContratadas: horasContratadasCliente,
        horasExecutadas: horasExecutadasCliente,
        totalLimmesTarefas: dados.tarefasDistintas.size,
      };
    }
  );

  return {
    detalhes: detalhesClientes,
    totalContratadas: arredondar(totalHorasContratadas),
    totalExecutadas: arredondar(totalHorasExecutadas),
  };
}

function calcularResumo(
  totalContratadas: number,
  totalExecutadas: number,
  totalClientes: number
) {
  return {
    totalClientes,
    diferencaHoras: arredondar(totalContratadas - totalExecutadas),
    percentualExecucao:
      totalContratadas > 0
        ? arredondar((totalExecutadas / totalContratadas) * 100)
        : 0,
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

    // Construir e executar query
    const { sql, params: sqlParams } = aplicarFiltros(
      SQL_BASE,
      params,
      [dataInicio, dataFim]
    );

    const apontamentos = await firebirdQuery(sql, sqlParams);

    // Processar dados
    const clientesMap = agruparPorCliente(apontamentos);
    
    const { detalhes, totalContratadas, totalExecutadas } = 
      calcularDetalhesClientes(clientesMap);
    
    const resumo = calcularResumo(
      totalContratadas,
      totalExecutadas,
      clientesMap.size
    );

    return NextResponse.json({
      totalHorasContratadas: totalContratadas,
      totalHorasExecutadas: totalExecutadas,
      detalhesClientes: detalhes,
      resumo,
    });

  } catch (error) {
    console.error('Erro ao calcular horas contratadas vs executadas:', error);
    console.error('Stack trace:', error instanceof Error ? error.stack : 'N/A');
    console.error('Message:', error instanceof Error ? error.message : error);
    
    return NextResponse.json(
      { 
        error: 'Erro interno do servidor',
        message: error instanceof Error ? error.message : 'Erro desconhecido',
        details: process.env.NODE_ENV === 'development' ? error : undefined
      },
      { status: 500 }
    );
  }
}