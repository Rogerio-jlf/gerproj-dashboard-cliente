'use client';

import { debounce } from 'lodash';
import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { IoIosArrowDown } from 'react-icons/io';
import { IoClose } from 'react-icons/io5';

// ================================================================================
// INTERFACES
// ================================================================================
interface InputFilterProps {
  value: string;
  onChange: (value: string) => void;
  columnId?: string;
}

interface DropdownValidacaoProps {
  value: string;
  onChange: (value: string) => void;
}

// ================================================================================
// CONSTANTES
// ================================================================================
const DEBOUNCE_DELAY = 600;

// Limites de caracteres por coluna
const COLUMN_MAX_LENGTH: Record<string, number> = {
  chamado_os: 6, // Aumentado para suportar separador (ex: 13.455)
  cod_os: 6,     // Aumentado para suportar separador (ex: 13.455)
  dtini_os: 10,
  nome_cliente: 15,
  status_chamado: 15,
  nome_recurso: 15,
  hrini_os: 5,   // Aumentado para suportar formato HH:MM
  hrfim_os: 5,   // Aumentado para suportar formato HH:MM
  obs: 30,
};

// Colunas de data
const DATE_COLUMNS = new Set(['dtini_os']);

// Colunas numéricas com separador de milhar
const NUMERIC_COLUMNS = new Set(['cod_os', 'chamado_os']);

// Colunas de hora
const TIME_COLUMNS = new Set(['hrini_os', 'hrfim_os']);

// Limite de caracteres para campos de hora (HH:MM = 5 caracteres)
const TIME_MAX_LENGTH = 5;

// ================================================================================
// FUNÇÕES AUXILIARES
// ================================================================================

/**
 * Formata uma string de números para o formato DD/MM/YYYY
 */
function formatDateString(input: string): string {
  const numbersOnly = input.replace(/\D/g, '');

  if (numbersOnly.length === 0) return '';
  if (numbersOnly.length <= 2) return numbersOnly;
  if (numbersOnly.length <= 4) {
    return `${numbersOnly.slice(0, 2)}/${numbersOnly.slice(2)}`;
  }
  if (numbersOnly.length <= 8) {
    return `${numbersOnly.slice(0, 2)}/${numbersOnly.slice(2, 4)}/${numbersOnly.slice(4)}`;
  }

  return `${numbersOnly.slice(0, 2)}/${numbersOnly.slice(2, 4)}/${numbersOnly.slice(4, 8)}`;
}

/**
 * Formata números com separador de milhar (ex: 13455 -> 13.455)
 */
function formatNumberWithThousands(input: string): string {
  const numbersOnly = input.replace(/\D/g, '');
  
  if (numbersOnly.length === 0) return '';
  if (numbersOnly.length <= 3) return numbersOnly;
  
  // Adiciona o ponto separador após o primeiro ou segundo dígito
  // Ex: 1234 -> 1.234, 12345 -> 12.345
  const withSeparator = numbersOnly.replace(/^(\d{1,2})(\d{3})$/, '$1.$2');
  
  return withSeparator;
}

/**
 * Formata hora no formato HH:MM de forma mais permissiva
 */
function formatTimeString(input: string): string {
  const numbersOnly = input.replace(/\D/g, '');
  
  if (numbersOnly.length === 0) return '';
  if (numbersOnly.length <= 2) return numbersOnly;
  
  // Adiciona o separador ':' após os dois primeiros dígitos
  const hours = numbersOnly.slice(0, 2);
  const minutes = numbersOnly.slice(2, 4);
  
  return `${hours}:${minutes}`;
}

/**
 * Normaliza uma data para comparação, mantendo zeros à esquerda
 */
function normalizeDateForComparison(dateStr: string | null): string {
  if (!dateStr) return '';

  // Se já está no formato DD/MM/YYYY, retorna como está
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(dateStr)) {
    return dateStr;
  }

  // Se é uma data parcial (ex: "03" ou "03/01"), normaliza para comparação
  if (/^\d{1,2}(\/\d{1,2})?(\/\d{4})?$/.test(dateStr)) {
    const parts = dateStr.split('/');
    
    // Adiciona zeros à esquerda se necessário
    if (parts[0]) {
      parts[0] = parts[0].padStart(2, '0');
    }
    if (parts[1]) {
      parts[1] = parts[1].padStart(2, '0');
    }
    
    return parts.join('/');
  }

  // Tenta converter formato ISO ou outros formatos
  try {
    const date = new Date(dateStr);
    if (!isNaN(date.getTime())) {
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      return `${day}/${month}/${year}`;
    }
  } catch (e) {
    // Se falhar, continua
  }

  // Tenta extrair data de string com timestamp
  const datePart = dateStr.split(/[\sT]/)[0];

  if (/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
    const [year, month, day] = datePart.split('-');
    return `${day}/${month}/${year}`;
  }

  return dateStr;
}

/**
 * Normaliza texto para comparação (remove acentos e converte para minúsculo)
 */
function normalizeText(text: string): string {
  return text
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();
}

// ================================================================================
// COMPONENTE DROPDOWN VALIDAÇÃO
// ================================================================================
const DropdownValidacao = memo(
  ({ value, onChange }: DropdownValidacaoProps) => {
    const [isOpen, setIsOpen] = useState(false);
    const dropdownRef = useRef<HTMLDivElement>(null);

    const options = [
      { name: 'Todos', code: '' },
      { name: 'Aprovado', code: 'SIM' },
      { name: 'Recusado', code: 'NAO' },
    ];

    const selectedOption =
      options.find((opt) => opt.code === value) || options[0];

    useEffect(() => {
      function handleClickOutside(event: MouseEvent) {
        if (
          dropdownRef.current &&
          !dropdownRef.current.contains(event.target as Node)
        ) {
          setIsOpen(false);
        }
      }

      document.addEventListener('mousedown', handleClickOutside);
      return () =>
        document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const handleSelect = (code: string) => {
      onChange(code);
      setIsOpen(false);
    };

    const handleClear = (e: React.MouseEvent) => {
      e.stopPropagation();
      onChange('');
      setIsOpen(false);
    };

    return (
      <div ref={dropdownRef} className="relative w-full">
        <button
          onClick={() => setIsOpen(!isOpen)}
          className={`group relative flex w-full cursor-pointer items-center justify-between rounded-md py-2 pr-3 pl-3 text-sm font-bold tracking-widest italic shadow-sm shadow-black transition-all hover:shadow-lg hover:shadow-black focus:ring-2 focus:ring-pink-600 focus:outline-none active:scale-95 ${
            value
              ? 'bg-white text-black ring-2 ring-pink-600'
              : 'border border-teal-950 bg-teal-900 text-white'
          }`}
        >
          <span className={`truncate ${!value ? 'opacity-50' : ''}`}>
            {selectedOption.name}
          </span>

          <div className="flex items-center gap-1 ml-2">
            {value && (
              <span
                onClick={handleClear}
                className="bg-slate-300 p-0.5 rounded-full hover:bg-red-500 cursor-pointer shadow-sm shadow-black"
                title="Limpar Filtro"
              >
                <IoClose
                  size={16}
                  className="text-black group-hover:text-white group-hover:rotate-180 transition-all"
                />
              </span>
            )}
            <span
              className={`transition-all ${isOpen ? 'rotate-180' : ''} ${
                value ? 'text-black' : 'text-white'
              }`}
            >
              <IoIosArrowDown size={18} />
            </span>
          </div>
        </button>

        {isOpen && (
          <div className="absolute top-full right-0 left-0 z-50 mt-1 overflow-hidden rounded-md bg-white shadow-lg shadow-black ring-1 ring-black">
            {options.map((option) => (
              <button
                key={option.code}
                onClick={() => handleSelect(option.code)}
                className={`w-full px-3 py-2 text-left text-sm font-semibold tracking-widest italic transition-all ${
                  value === option.code
                    ? 'bg-blue-600 text-white'
                    : 'text-black hover:bg-black hover:text-white'
                }`}
              >
                {option.name}
              </button>
            ))}
          </div>
        )}
      </div>
    );
  },
);

DropdownValidacao.displayName = 'DropdownValidacao';

// ================================================================================
// COMPONENTE INPUT FILTRO COM DEBOUNCE
// ================================================================================
const InputFilterWithDebounce = memo(
  ({ value, onChange, columnId }: InputFilterProps) => {
    const [localValue, setLocalValue] = useState(value);
    const isUserTyping = useRef(false);
    const inputRef = useRef<HTMLInputElement>(null);

    const maxLength = useMemo(
      () => (columnId ? COLUMN_MAX_LENGTH[columnId] : undefined),
      [columnId],
    );

    const isDateColumn = useMemo(
      () => (columnId ? DATE_COLUMNS.has(columnId) : false),
      [columnId],
    );

    const isNumericColumn = useMemo(
      () => (columnId ? NUMERIC_COLUMNS.has(columnId) : false),
      [columnId],
    );

    const isTimeColumn = useMemo(
      () => (columnId ? TIME_COLUMNS.has(columnId) : false),
      [columnId],
    );

    // Sincroniza o valor local quando o valor externo muda (exceto quando o usuário está digitando)
    useEffect(() => {
      if (!isUserTyping.current && value !== localValue) {
        setLocalValue(value);
      }
    }, [value, localValue]);

    const debouncedOnChange = useMemo(
      () =>
        debounce((newValue: string) => {
          onChange(newValue);
          isUserTyping.current = false;
        }, DEBOUNCE_DELAY),
      [onChange],
    );

    useEffect(() => {
      return () => {
        debouncedOnChange.cancel();
      };
    }, [debouncedOnChange]);

   const handleChange = useCallback(
  (e: React.ChangeEvent<HTMLInputElement>) => {
    isUserTyping.current = true;
    let inputValue = e.target.value;

    let processedValue = inputValue;

    // Para campos de tempo, permite apagar livremente
    if (isTimeColumn) {
      // Se o usuário está apagando (valor atual é menor que o anterior)
      if (inputValue.length < localValue.length) {
        // Remove apenas números, mantém a lógica de formatação
        const numbersOnly = inputValue.replace(/\D/g, '');
        processedValue = numbersOnly.length === 0 ? '' : formatTimeString(numbersOnly);
      } else {
        // Se está adicionando caracteres, formata normalmente
        processedValue = formatTimeString(inputValue);
      }
    }
    // Formata números com separador de milhar para colunas numéricas
    else if (isNumericColumn) {
      processedValue = formatNumberWithThousands(inputValue);
    }
    // Formata data para colunas de data
    else if (isDateColumn) {
      processedValue = formatDateString(inputValue.replace(/\D/g, ''));
    }

    // Aplica limites de comprimento APÓS a formatação
    const effectiveMaxLength = isTimeColumn ? TIME_MAX_LENGTH : maxLength;
    if (effectiveMaxLength && processedValue.length > effectiveMaxLength) {
      processedValue = processedValue.slice(0, effectiveMaxLength);
    }

    setLocalValue(processedValue);
    debouncedOnChange(processedValue);
  },
  [debouncedOnChange, maxLength, isDateColumn, isNumericColumn, isTimeColumn, localValue.length],
);

    const handleClear = useCallback(() => {
      isUserTyping.current = false;
      setLocalValue('');
      onChange('');
      debouncedOnChange.cancel();

      requestAnimationFrame(() => {
        inputRef.current?.focus();
      });
    }, [onChange, debouncedOnChange]);

    const handleKeyDown = useCallback(
      (e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Escape') {
          e.preventDefault();
          handleClear();
        } else if (e.key === 'Enter') {
          // Força a aplicação imediata do filtro ao pressionar Enter
          debouncedOnChange.flush();
        }
      },
      [handleClear, debouncedOnChange],
    );


    return (
      <div className="group relative w-full">
        <input
          ref={inputRef}
          type="text"
          value={localValue}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          placeholder="Filtrar..."
          className={`hover:bg-opacity-90 w-full rounded-md px-3 py-2 text-sm font-bold shadow-sm shadow-black transition-all select-none focus:ring-2 focus:ring-purple-500 focus:outline-none active:scale-95 ${
            localValue
              ? 'bg-white text-black ring-2 ring-purple-500'
              : 'border border-teal-950 bg-teal-900 text-white placeholder-white/50 hover:shadow-lg hover:shadow-black'
          }`}
        />

        {localValue && (
          <button
            onClick={handleClear}
            aria-label="Limpar filtro"
            title="Limpar Filtro"
            className="absolute top-1/2 right-3 -translate-y-1/2 bg-slate-300 p-0.5 rounded-full hover:bg-red-500 cursor-pointer shadow-sm shadow-black transition-all"
            type="button"
          >
            <IoClose
              size={16}
              className="text-black group-hover:text-white group-hover:rotate-180 transition-all"
            />
          </button>
        )}
      </div>
    );
  },
);

InputFilterWithDebounce.displayName = 'InputFilterWithDebounce';

// ================================================================================
// COMPONENTE PRINCIPAL - WRAPPER
// ================================================================================
export const FiltroHeaderChamados = memo(
  ({ value, onChange, columnId }: InputFilterProps) => {
    if (columnId === 'valcli_os') {
      return <DropdownValidacao value={value} onChange={onChange} />;
    }

    return (
      <InputFilterWithDebounce
        value={value}
        onChange={onChange}
        columnId={columnId}
      />
    );
  },
);

FiltroHeaderChamados.displayName = 'FiltroHeaderChamados';

// ================================================================================
// HOOK PERSONALIZADO PARA FUNÇÕES DE FILTRO
// ================================================================================
export const useFiltrosChamados = () => {
  const columnFilterFn = useCallback(
    (row: any, columnId: string, filterValue: string) => {
      // Se não há filtro, retorna true
      if (!filterValue || filterValue.trim() === '') {
        return true;
      }

      const cellValue = row.getValue(columnId);
      const filterTrimmed = filterValue.trim();

      // Tratamento para valores nulos ou undefined
      if (cellValue == null || cellValue === '') {
        return false;
      }

      // Converte o valor da célula para string
      const cellString = String(cellValue).trim();

      // Tratamento especial para validação (SIM/NAO)
      if (columnId === 'valcli_os') {
        const cellUpper = cellString.toUpperCase();
        const filterUpper = filterTrimmed.toUpperCase();
        return cellUpper === filterUpper;
      }

      // Tratamento especial para campos de data
      if (DATE_COLUMNS.has(columnId)) {
        const normalizedCell = normalizeDateForComparison(cellString);
        const normalizedFilter = normalizeDateForComparison(filterTrimmed);
        
        // Permite busca parcial em datas (ex: "03" encontra dias 03, "03/01" encontra 03 de janeiro)
        return normalizedCell.startsWith(normalizedFilter);
      }

      // Tratamento para campos numéricos (cod_os, chamado_os)
      if (NUMERIC_COLUMNS.has(columnId)) {
        // Remove caracteres não numéricos para comparação (remove o ponto separador)
        const cellNumbers = cellString.replace(/\D/g, '');
        const filterNumbers = filterTrimmed.replace(/\D/g, '');

        return cellNumbers.includes(filterNumbers);
      }

      // Tratamento para campos de hora (hrini_os, hrfim_os)
      if (TIME_COLUMNS.has(columnId)) {
        // Remove os dois pontos para comparação
        const cellTime = cellString.replace(/:/g, '');
        const filterTime = filterTrimmed.replace(/:/g, '');

        return cellTime.includes(filterTime);
      }

      // Tratamento padrão para texto (case insensitive e sem acentos)
      const normalizedCell = normalizeText(cellString);
      const normalizedFilter = normalizeText(filterTrimmed);

      return normalizedCell.includes(normalizedFilter);
    },
    [],
  );

  return { columnFilterFn };
};