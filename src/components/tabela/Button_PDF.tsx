'use client';

// IMPORTS
import jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable';
import { useState } from 'react';

// HELPERS
import { corrigirTextoCorrompido } from '../../formatters/formatar-texto-corrompido';

// ICONS
import { formatarDataParaBR } from '@/formatters/formatar-data';
import { formatarHora } from '@/formatters/formatar-hora';
import { formatarNumeros } from '@/formatters/formatar-numeros';
import { renderizarDoisPrimeirosNomes } from '@/formatters/remover-acentuacao';
import { FaFilePdf } from 'react-icons/fa';
import type { TableRowProps } from './Colunas_Tabela';

// ================================================================================
// INTERFACES
// ================================================================================

interface ExportaPDFButtonProps {
  data: TableRowProps[];
  fileName: string;
  title: string;
  columns: Array<{
    key: keyof TableRowProps;
    label: string;
  }>;
  logoUrl?: string;
  footerText?: string;
  className?: string;
  disabled?: boolean;
}

// ================================================================================
// FUNÇÕES AUXILIARES
// ================================================================================

function getNomeMes(mes: string): string {
  const meses = [
    '',
    'Janeiro',
    'Fevereiro',
    'Março',
    'Abril',
    'Maio',
    'Junho',
    'Julho',
    'Agosto',
    'Setembro',
    'Outubro',
    'Novembro',
    'Dezembro',
  ];
  return meses[parseInt(mes)] || '';
}

// ================================================================================
// COMPONENTE DE LOADING SPINNER
// ================================================================================
function LoadingSpinner() {
  return (
    <svg
      className="animate-spin h-6 w-6 text-white"
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
    >
      <circle
        className="opacity-25"
        cx="12"
        cy="12"
        r="10"
        stroke="currentColor"
        strokeWidth="4"
      ></circle>
      <path
        className="opacity-75"
        fill="currentColor"
        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
      ></path>
    </svg>
  );
}

// ================================================================================
// COMPONENTE
// ================================================================================
export function ExportaPDFButton({
  data,
  fileName,
  title,
  columns,
  footerText = 'Gerado pelo sistema em',
  className = '',
  disabled = false,
}: ExportaPDFButtonProps) {
  const [isExporting, setIsExporting] = useState(false);

  const exportToPDF = async () => {
    if (data.length === 0) {
      alert('Não há dados para exportar!');
      return;
    }

    setIsExporting(true);

    // Pequeno delay para garantir que o loading apareça
    await new Promise(resolve => setTimeout(resolve, 100));

    try {
      const doc = new jsPDF('l', 'mm', 'a4');
      let yPosition = 15;

      // ================================================================================
      // CABEÇALHO DO RELATÓRIO
      // ================================================================================
      doc.setFillColor(15, 118, 110);
      doc.rect(0, 0, 297, 20, 'F');

      doc.setTextColor(255, 255, 255);
      doc.setFontSize(18);
      doc.setFont('helvetica', 'bold');
      doc.text(title.toUpperCase(), 148.5, 11, {
        align: 'center',
      });

      doc.setFontSize(9);
      doc.setFont('helvetica', 'italic');
      doc.text(
        `${footerText}: ${new Date().toLocaleString('pt-BR')}`,
        148.5,
        17,
        {
          align: 'center',
        },
      );

      yPosition = 26;

      // ================================================================================
      // TOTALIZADORES
      // ================================================================================
      doc.setFillColor(0, 0, 0);
      doc.rect(15, yPosition, 80, 6, 'F');
      doc.setDrawColor(229, 231, 235);

      doc.setTextColor(255, 255, 255);
      doc.setFontSize(10);
      doc.setFont('helvetica', 'bold');
      doc.text('TOTALIZADORES', 55, yPosition + 4, { align: 'center' });

      yPosition += 6;

      // Calcular totalizadores
      const totalChamados = data.length;
      const totalRecursos = Array.from(
        new Set(data.map((item) => item.nome_recurso || '')),
      ).filter(Boolean).length;

      // Calcular total de horas
      let totalMs = 0;
      data.forEach((item) => {
        if (item.total_horas && item.total_horas !== '-') {
          const match = item.total_horas.match(/(\d+)h:?(\d+)/);
          if (match) {
            const horas = parseInt(match[1]);
            const minutos = parseInt(match[2]);
            totalMs += (horas * 60 + minutos) * 60 * 1000;
          }
        }
      });

      const totalMinutes = Math.floor(totalMs / (1000 * 60));
      const horas = Math.floor(totalMinutes / 60);
      const minutos = totalMinutes % 60;
      const totalHoras = `${String(horas).padStart(2, '0')}h:${String(minutos).padStart(2, '0')}min`;

      const totalizadores = [
        {
          label: 'Total de Chamados',
          value: totalChamados.toLocaleString('pt-BR'),
          color: [128, 0, 128],
        },
        {
          label: 'Total de Recursos',
          value: totalRecursos.toLocaleString('pt-BR'),
          color: [0, 0, 255],
        },
        {
          label: 'Total de Horas',
          value: totalHoras,
          color: [75, 0, 130],
        },
      ];

      totalizadores.forEach((tot) => {
        doc.setFillColor(tot.color[0], tot.color[1], tot.color[2]);
        doc.rect(15, yPosition, 35, 6, 'F');

        doc.setTextColor(255, 255, 255);
        doc.setFontSize(8);
        doc.setFont('helvetica', 'bold');
        doc.text(tot.label, 32.5, yPosition + 4, { align: 'center' });

        doc.setFillColor(255, 255, 255);
        doc.rect(50, yPosition, 45, 6);
        doc.setDrawColor(229, 231, 235);

        doc.setTextColor(0, 0, 0);
        doc.setFontSize(8);
        doc.setFont('helvetica', 'normal');
        doc.text(tot.value, 72.5, yPosition + 4, { align: 'center' });

        yPosition += 6;
      });

      yPosition += 5;

      // ================================================================================
      // TABELA DE DETALHES
      // ================================================================================
      const tableHeaders = columns.map((col) => col.label);

      const tableData = data.map((row) => {
        return columns.map((col) => {
          const value = (row as any)[col.key];

          if (col.key === 'chamado_os') {
            if (value) {
              return formatarNumeros(String(value));
            }
            if ((row as any).codtrf_os) {
              return `T-${formatarNumeros(String((row as any).codtrf_os))}`;
            }
            return 'n/a';
          }
          if (col.key === 'cod_os') {
            return formatarNumeros(String(value || null));
          }
          if (col.key === 'dtini_os') {
            return formatarDataParaBR(String(value || null));
          }
          if (col.key === 'nome_cliente') {
            return renderizarDoisPrimeirosNomes(String(value || null));
          }
          if (col.key === 'status_chamado') {
            return String(value || 'Tarefa');
          }
          if (col.key === 'nome_recurso') {
            return renderizarDoisPrimeirosNomes(
              corrigirTextoCorrompido(String(value || null)),
            );
          }
          if (col.key === 'hrini_os') {
            return formatarHora(String(value || null));
          }
          if (col.key === 'hrfim_os') {
            return formatarHora(String(value || null));
          }
          if (col.key === 'total_horas') {
            return String(value || 'n/a');
          }
          if (col.key === 'obs') {
            return corrigirTextoCorrompido(String(value || 'Sem observação'));
          }

          return String(value || 'n/a');
        });
      });

      const columnStyles: any = {
        0: { cellWidth: 18, halign: 'center' },
        1: { cellWidth: 12, halign: 'center' },
        2: { cellWidth: 17, halign: 'center' },
        3: { cellWidth: 35, halign: 'left' },
        4: { cellWidth: 27, halign: 'left' },
        5: { cellWidth: 35, halign: 'left' },
        6: { cellWidth: 17, halign: 'center' },
        7: { cellWidth: 15, halign: 'center' },
        8: { cellWidth: 17, halign: 'center' },
        9: { cellWidth: 20, halign: 'center' },
        10: { cellWidth: 64, halign: 'left' },
      };

      autoTable(doc, {
        startY: yPosition,
        head: [tableHeaders],
        body: tableData,
        theme: 'grid',
        headStyles: {
          fillColor: [15, 118, 110],
          textColor: [255, 255, 255],
          fontStyle: 'bold',
          fontSize: 8,
          halign: 'center',
          valign: 'middle',
        },
        bodyStyles: {
          fontSize: 7,
          cellPadding: 2,
          valign: 'middle',
        },
        columnStyles: columnStyles,
        didParseCell: (data) => {
          if (data.column.index === 4 && data.section === 'body') {
            const status = data.cell.text[0]?.toLowerCase();

            if (status?.includes('concluído') || status?.includes('concluido')) {
              data.cell.styles.fillColor = [34, 197, 94];
              data.cell.styles.textColor = [255, 255, 255];
              data.cell.styles.fontStyle = 'bold';
            } else if (status?.includes('andamento')) {
              data.cell.styles.fillColor = [234, 179, 8];
              data.cell.styles.textColor = [0, 0, 0];
              data.cell.styles.fontStyle = 'bold';
            } else if (status?.includes('pendente')) {
              data.cell.styles.fillColor = [239, 68, 68];
              data.cell.styles.textColor = [255, 255, 255];
              data.cell.styles.fontStyle = 'bold';
            } else if (status?.includes('cancelado')) {
              data.cell.styles.fillColor = [107, 114, 128];
              data.cell.styles.textColor = [255, 255, 255];
              data.cell.styles.fontStyle = 'bold';
            }
          }

          if (data.column.index === 8 && data.section === 'body') {
            const validacao = data.cell.text[0]?.toUpperCase();

            if (validacao === 'SIM') {
              data.cell.styles.fillColor = [59, 130, 246];
              data.cell.styles.textColor = [255, 255, 255];
              data.cell.styles.fontStyle = 'bold';
            } else if (validacao === 'NAO' || validacao === 'NÃO') {
              data.cell.styles.fillColor = [239, 68, 68];
              data.cell.styles.textColor = [255, 255, 255];
              data.cell.styles.fontStyle = 'bold';
            }
          }
        },
        margin: { left: 10, right: 10 },
      });

      // ================================================================================
      // SALVAR PDF
      // ================================================================================
      const timestamp = new Date().getTime();
      const nomeArquivo = `${fileName}_${timestamp}.pdf`;
      doc.save(nomeArquivo);
    } catch (error) {
      console.error('Erro ao exportar PDF:', error);
      alert('Erro ao gerar o PDF. Tente novamente.');
    } finally {
      setIsExporting(false);
    }
  };

  // ================================================================================
  // RENDERIZAÇÃO
  // ================================================================================
  return (
    <button
      onClick={exportToPDF}
      disabled={isExporting || disabled}
      title={
        disabled
          ? 'Não há dados para exportar'
          : isExporting
            ? 'Gerando PDF...'
            : 'Exportar para PDF'
      }
      className={`group cursor-pointer rounded-md bg-gradient-to-br from-red-600 to-red-700 p-3 shadow-md shadow-black hover:shadow-xl hover:shadow-black transition-all hover:scale-110 active:scale-95 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:scale-100 ${className}`}
    >
      {isExporting ? (
        <LoadingSpinner />
      ) : (
        <FaFilePdf className="text-white group-hover:scale-110" size={24} />
      )}
    </button>
  );
}