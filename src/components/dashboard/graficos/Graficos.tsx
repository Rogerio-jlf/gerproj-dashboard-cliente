import { useAuth } from '@/context/AuthContext';
import { formatarHorasTotaisSufixo } from '@/formatters/formatar-hora';
import { formatarNumeros } from '@/formatters/formatar-numeros';
import React, { useCallback, useEffect, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

// Cores para os gráficos
const COLORS = {
  primary: '#3b82f6', // azul
  secondary: '#8b5cf6', // roxo
  success: '#10b981', // verde
  warning: '#f59e0b', // amarelo
  danger: '#ef4444', // vermelho
  info: '#06b6d4', // ciano
  gradient: '#f97316', // laranja para o novo gráfico
  pie: [
    '#3b82f6', // azul
    '#8b5cf6', // roxo
    '#10b981', // verde
    '#f59e0b', // amarelo
    '#ef4444', // vermelho
    '#06b6d4', // ciano
    '#ec4899', // rosa
    '#6366f1', // índigo
  ],
};

// Componente de Tooltip customizado
type CustomTooltipProps = {
  active?: boolean;
  payload?: Array<{ name?: string; value?: any; color?: string }>;
  label?: any;
  labelFormatter?: (label: any) => string;
  valueFormatter?: (value: any) => string;
};

const CustomTooltip: React.FC<CustomTooltipProps> = ({
  active,
  payload,
  label,
  labelFormatter,
  valueFormatter,
}) => {
  if (!active || !payload || !payload.length) return null;

  return (
    <div className="bg-white p-3 rounded-lg shadow-lg border border-gray-200">
      <p className="font-semibold text-gray-900 mb-2">
        {labelFormatter ? labelFormatter(label) : label}
      </p>
      {payload.map((entry, index) => (
        <p
          key={index}
          className="text-sm"
          style={{ color: entry?.color || undefined }}
        >
          {entry?.name}:{' '}
          <span className="font-bold">
            {valueFormatter ? valueFormatter(entry?.value) : entry?.value}
          </span>
        </p>
      ))}
    </div>
  );
};

// Componente de card para os gráficos
type ChartCardProps = {
  title: string;
  children?: React.ReactNode;
};

const ChartCard: React.FC<ChartCardProps> = ({ title, children }) => (
  <div className="bg-white rounded-xl shadow-md shadow-black/20 p-6 border border-gray-200 transition-all hover:shadow-xl hover:border-purple-500 ">
    <h3 className="text-lg font-extrabold text-black mb-4 tracking-widest select-none uppercase">
      {title}
    </h3>
    {children}
  </div>
);

// Componente de Loading
const LoadingSpinner = () => (
  <div className="flex items-center justify-center h-64">
    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
  </div>
);

// Componente de Erro
type ErrorMessageProps = {
  message: string;
  onRetry?: () => void;
};
const ErrorMessage: React.FC<ErrorMessageProps> = ({ message, onRetry }) => (
  <div className="bg-red-50 border border-red-200 rounded-lg p-4">
    <p className="text-red-800 mb-2">❌ {message}</p>
    {onRetry && (
      <button
        onClick={onRetry}
        className="px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition-all"
      >
        Tentar Novamente
      </button>
    )}
  </div>
);

// Interface para os filtros recebidos via props
interface FilterProps {
  filters: {
    ano: number;
    mes: number;
    cliente: string;
    recurso: string;
    status: string;
  };
}

export function Graficos({ filters }: FilterProps) {
  const { isAdmin, codCliente } = useAuth();
  const [dados, setDados] = useState<any | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const buscarDados = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Montar query params
      const params = new URLSearchParams({
        isAdmin: isAdmin.toString(),
        mes: filters.mes.toString(),
        ano: filters.ano.toString(),
      });

      // Adicionar parâmetros opcionais apenas se existirem
      if (!isAdmin && codCliente) {
        params.append('codCliente', codCliente);
      }
      if (filters.cliente) {
        params.append('codClienteFilter', filters.cliente);
      }
      if (filters.recurso) {
        params.append('codRecursoFilter', filters.recurso);
      }
      if (filters.status) {
        params.append('status', filters.status);
      }

      const response = await fetch(`/api/ordens-servico?${params.toString()}`);

      if (!response.ok) {
        throw new Error(`Erro HTTP: ${response.status}`);
      }

      const data = await response.json();
      setDados(data);
    } catch (err: unknown) {
      if (err instanceof Error) {
        setError(err.message);
        console.error('Erro ao buscar dados:', err);
      } else {
        setError(String(err) || 'Erro ao buscar dados');
        console.error('Erro ao buscar dados:', err);
      }
    } finally {
      setLoading(false);
    }
  }, [filters, isAdmin, codCliente]);

  useEffect(() => {
    buscarDados();
  }, [buscarDados]);

  // Renderização condicional
  if (loading) {
    return (
      <div className="p-6 border border-gray-200 bg-gradient-to-br from-white via-indigo-50/20 to-purple-50/30 min-h-screen rounded-xl shadow-md">
        <h1 className="text-3xl font-bold text-gray-900 mb-8">
          GRÁFICOS DE ANÁLISES
        </h1>
        <LoadingSpinner />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6 border border-gray-200 bg-gradient-to-br from-white via-indigo-50/20 to-purple-50/30 min-h-screen rounded-xl shadow-md">
        <h1 className="text-3xl font-bold text-gray-900 mb-8">
          GRÁFICOS DE ANÁLISES
        </h1>
        <ErrorMessage message={error} onRetry={buscarDados} />
      </div>
    );
  }

  if (!dados || !dados.graficos) {
    return (
      <div className="p-6 border border-gray-200 bg-gradient-to-br from-white via-indigo-50/20 to-purple-50/30 min-h-screen rounded-xl shadow-md">
        <h1 className="text-3xl font-bold text-gray-900 mb-8">
          GRÁFICOS DE ANÁLISES
        </h1>
        <p className="text-gray-600">Nenhum dado disponível</p>
      </div>
    );
  }

  const {
    horasPorDia,
    topChamados,
    horasPorStatus,
    horasPorRecurso,
    horasPorCliente,
    horasPorMes,
  } = dados.graficos;
  const { totalizadores } = dados;

  return (
    <div className="p-6 border border-gray-200 bg-gradient-to-br from-white via-indigo-50/20 to-purple-50/30 min-h-screen rounded-xl shadow-md shadow-black/20">
      <div className="w-full">
        {/* Cabeçalho */}
        <div className="mb-8">
          <h1 className="text-3xl font-extrabold text-black tracking-widest select-none">
            GRÁFICOS DE ANÁLISES
          </h1>
        </div>

        {/* Grid de gráficos */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Gráfico 1: Evolução Diária de Horas (Linha) */}
          {horasPorDia && horasPorDia.length > 0 && (
            <ChartCard title="Evolução Diária de Horas">
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={horasPorDia}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis
                    dataKey="data"
                    stroke="#6b7280"
                    style={{ fontSize: '12px' }}
                  />
                  <YAxis
                    stroke="#6b7280"
                    style={{ fontSize: '12px' }}
                    label={{
                      value: 'Horas',
                      angle: -90,
                      position: 'insideLeft',
                    }}
                  />
                  <Tooltip
                    content={
                      <CustomTooltip
                        labelFormatter={(label) => `Data: ${label}`}
                        valueFormatter={(value) => `${value}h`}
                      />
                    }
                  />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="horas"
                    stroke={COLORS.primary}
                    strokeWidth={2}
                    dot={{ fill: COLORS.primary, r: 4 }}
                    activeDot={{ r: 6 }}
                    name="Horas Trabalhadas"
                  />
                </LineChart>
              </ResponsiveContainer>
              <div className="mt-4 p-3 bg-blue-100 rounded-md border border-blue-300">
                <p className="text-base text-black tracking-widest select-none font-extrabold">
                  <span>Total no período:</span> {totalizadores?.TOTAL_HRS || 0}
                  h
                </p>
              </div>
            </ChartCard>
          )}

          {/* Gráfico 2: Top Chamados por Horas (Barras) */}
          {topChamados && topChamados.length > 0 && (
            <ChartCard title="Top 5 Chamados por Horas">
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={topChamados.slice(0, 5)}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  {/* ===== */}
                  <XAxis
                    dataKey="chamado"
                    stroke="#6b7280" // cor da linha do eixo
                    tickLine={false} // remove a linha dos ticks
                    axisLine={{ stroke: '#d1d5db', strokeWidth: 1 }} // linha principal do eixo
                    tick={{
                      fill: '#111827', // cor do texto
                      fontSize: 13, // tamanho da fonte
                      fontWeight: 800, // negrito
                      letterSpacing: '0.2em', // espaçamento entre letras
                      textAnchor: 'middle', // centraliza o texto
                    }}
                    tickFormatter={formatarNumeros} // 🔹 aplica sua função aqui
                    textAnchor="end" // ajusta o alinhamento quando rotacionado
                    interval={0} // mostra todos os rótulos (sem pular)
                    height={60} // espaço para labels longos
                  />
                  {/* ===== */}
                  <YAxis
                    stroke="#6b7280"
                    tickLine={false}
                    axisLine={{ stroke: '#d1d5db', strokeWidth: 1 }}
                    tick={{
                      fill: '#111827',
                      fontSize: 13, // tamanho da fonte
                      fontWeight: 800, // negrito
                      letterSpacing: '0.2em', // espaçamento entre letras
                    }}
                    label={{
                      value: 'Horas',
                      angle: -90,
                      position: 'insideLeft',
                      fill: '#111827',
                      fontSize: 13,
                      fontWeight: 700,
                    }}
                  />
                  {/* ===== */}
                  <Tooltip
                    content={
                      <CustomTooltip
                        labelFormatter={(label) => `Chamado #${label}`}
                        valueFormatter={(value) => `${value}h`}
                      />
                    }
                  />
                  <Legend />
                  <Bar
                    dataKey="horas"
                    fill={COLORS.secondary}
                    radius={[8, 8, 0, 0]}
                    name="Horas Consumidas"
                  />
                </BarChart>
              </ResponsiveContainer>
              <div className="mt-4 space-y-1">
                {topChamados.slice(0, 3).map(
                  (
                    chamado: {
                      chamado: string | number;
                      cliente: string;
                      horas: number;
                    },
                    index: number,
                  ) => (
                    <div
                      key={chamado.chamado}
                      className="flex items-center justify-between text-sm"
                    >
                      <span className="text-black tracking-widest select-none font-extrabold text-sm">
                        {index + 1} - {chamado.cliente}
                      </span>
                      <span className="font-extrabold text-black tracking-widest select-none text-sm">
                        {formatarHorasTotaisSufixo(chamado.horas)}
                      </span>
                    </div>
                  ),
                )}
              </div>
            </ChartCard>
          )}

          {/* Gráfico 3: Distribuição por Status (Pizza) */}
          {isAdmin && horasPorStatus && horasPorStatus.length > 0 && (
            <ChartCard title="Distribuição de Horas por Status">
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={horasPorStatus}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={(entry: { status?: string; percentual?: number }) =>
                      `${entry.status}: ${entry.percentual}%`
                    }
                    outerRadius={90}
                    fill="#8884d8"
                    dataKey="horas"
                  >
                    {horasPorStatus.map(
                      (
                        entry: {
                          status: string;
                          horas: number;
                          percentual?: number;
                        },
                        index: number,
                      ) => (
                        <Cell
                          key={`cell-${index}`}
                          fill={COLORS.pie[index % COLORS.pie.length]}
                        />
                      ),
                    )}
                  </Pie>
                  <Tooltip
                    content={
                      <CustomTooltip valueFormatter={(value) => `${value}h`} />
                    }
                  />
                </PieChart>
              </ResponsiveContainer>
              <div className="mt-4 grid grid-cols-2 gap-2">
                {horasPorStatus.map(
                  (
                    status: {
                      status: string;
                      horas: number;
                      percentual?: number;
                    },
                    index: number,
                  ) => (
                    <div
                      key={status.status}
                      className="flex items-center gap-2"
                    >
                      <div
                        className="w-3 h-3 rounded-full"
                        style={{
                          backgroundColor:
                            COLORS.pie[index % COLORS.pie.length],
                        }}
                      />
                      <span className="text-sm text-gray-700">
                        {status.status}
                      </span>
                    </div>
                  ),
                )}
              </div>
            </ChartCard>
          )}

          {/* Gráfico 4: Horas por Recurso (Barras Horizontais) */}
          {horasPorRecurso && horasPorRecurso.length > 0 && (
            <ChartCard title="Horas por Recurso">
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={horasPorRecurso} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis
                    type="number"
                    stroke="#6b7280"
                    style={{ fontSize: '12px' }}
                  />
                  <YAxis
                    dataKey="recurso"
                    type="category"
                    width={100}
                    stroke="#6b7280"
                    style={{ fontSize: '12px' }}
                  />
                  <Tooltip
                    content={
                      <CustomTooltip
                        labelFormatter={(label) => label}
                        valueFormatter={(value) => `${value}h`}
                      />
                    }
                  />
                  <Legend />
                  <Bar
                    dataKey="horas"
                    fill={COLORS.success}
                    radius={[0, 8, 8, 0]}
                    name="Horas Trabalhadas"
                  />
                </BarChart>
              </ResponsiveContainer>
              <div className="mt-4 p-3 bg-green-50 rounded-md">
                <p className="text-sm text-green-800">
                  <span className="font-semibold">Média por recurso:</span>{' '}
                  {(
                    horasPorRecurso.reduce(
                      (acc: number, r: { horas: number }) => acc + r.horas,
                      0,
                    ) / horasPorRecurso.length
                  ).toFixed(2)}
                  h
                </p>
              </div>
            </ChartCard>
          )}

          {/* Gráfico 5: Horas por Cliente (apenas para Admin) */}
          {isAdmin && horasPorCliente && horasPorCliente.length > 0 && (
            <ChartCard title="Horas por Cliente">
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={horasPorCliente.slice(0, 10)}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis
                    dataKey="cliente"
                    stroke="#6b7280"
                    style={{ fontSize: '10px' }}
                    angle={-45}
                    textAnchor="end"
                    height={100}
                  />
                  <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
                  <Tooltip
                    content={
                      <CustomTooltip valueFormatter={(value) => `${value}h`} />
                    }
                  />
                  <Legend />
                  <Bar
                    dataKey="horas"
                    fill={COLORS.info}
                    radius={[8, 8, 0, 0]}
                    name="Horas"
                  />
                </BarChart>
              </ResponsiveContainer>
            </ChartCard>
          )}

          {/* Gráfico 6: Horas Totais por Mês (NOVO) */}
          {horasPorMes && horasPorMes.length > 0 && (
            <ChartCard title={`Horas Totais por Mês - ${filters.ano}`}>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={horasPorMes}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis
                    dataKey="mes"
                    stroke="#6b7280"
                    style={{ fontSize: '12px' }}
                  />
                  <YAxis
                    stroke="#6b7280"
                    style={{ fontSize: '12px' }}
                    label={{
                      value: 'Horas',
                      angle: -90,
                      position: 'insideLeft',
                    }}
                  />
                  <Tooltip
                    content={
                      <CustomTooltip
                        labelFormatter={(label) => label}
                        valueFormatter={(value) => `${value}h`}
                      />
                    }
                  />
                  <Legend />
                  <Bar
                    dataKey="horas"
                    fill={COLORS.gradient}
                    radius={[8, 8, 0, 0]}
                    name="Total de Horas"
                  >
                    {horasPorMes.map((entry: any, index: number) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={
                          entry.mesNum === filters.mes
                            ? COLORS.danger
                            : COLORS.gradient
                        }
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
              <div className="mt-4 p-3 bg-orange-50 rounded-md">
                <p className="text-sm text-orange-800">
                  <span className="font-semibold">Total anual:</span>{' '}
                  {horasPorMes
                    .reduce(
                      (acc: number, m: { horas: number }) => acc + m.horas,
                      0,
                    )
                    .toFixed(2)}
                  h<span className="ml-3 font-semibold">Média mensal:</span>{' '}
                  {(
                    horasPorMes.reduce(
                      (acc: number, m: { horas: number }) => acc + m.horas,
                      0,
                    ) / 12
                  ).toFixed(2)}
                  h
                </p>
              </div>
            </ChartCard>
          )}
        </div>

        {/* Resumo Estatístico */}
        {totalizadores && (
          <div className="mt-6 bg-white rounded-lg shadow-md p-6 border border-gray-200">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">
              Resumo Estatístico
            </h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="text-center p-4 bg-blue-50 rounded-lg">
                <p className="text-sm text-blue-600 font-medium">
                  Total de Horas
                </p>
                <p className="text-2xl font-bold text-blue-900">
                  {totalizadores.TOTAL_HRS}h
                </p>
              </div>
              <div className="text-center p-4 bg-purple-50 rounded-lg">
                <p className="text-sm text-purple-600 font-medium">
                  Ordens de Serviço
                </p>
                <p className="text-2xl font-bold text-purple-900">
                  {totalizadores.TOTAL_OS}
                </p>
              </div>
              <div className="text-center p-4 bg-green-50 rounded-lg">
                <p className="text-sm text-green-600 font-medium">Recursos</p>
                <p className="text-2xl font-bold text-green-900">
                  {totalizadores.TOTAL_RECURSOS}
                </p>
              </div>
              <div className="text-center p-4 bg-orange-50 rounded-lg">
                <p className="text-sm text-orange-600 font-medium">Chamados</p>
                <p className="text-2xl font-bold text-orange-900">
                  {totalizadores.TOTAL_CHAMADOS}
                </p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
