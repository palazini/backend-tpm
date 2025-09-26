// utils/status.ts
export const CHAMADO_STATUS = {
  ABERTO: 'Aberto',
  EM_ANDAMENTO: 'Em Andamento',
  CONCLUIDO: 'Concluido',
  CANCELADO: 'Cancelado',
} as const;

export type ChamadoStatus = typeof CHAMADO_STATUS[keyof typeof CHAMADO_STATUS];

export const AGENDAMENTO_STATUS = {
  AGENDADO: 'agendado',
  INICIADO: 'iniciado',
  CONCLUIDO: 'concluido',
  CANCELADO: 'cancelado',
} as const;

export type AgendamentoStatus = typeof AGENDAMENTO_STATUS[keyof typeof AGENDAMENTO_STATUS];

function normalizeBase(value?: string | null): string {
  return String(value ?? '')
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '') // remove acentos
    .replace(/[_-]+/g, ' ')         // trata "em_andamento", "em-andamento"
    .toLowerCase()
    .trim()
    .replace(/\s+/g, ' ');          // colapsa espaços
}

/**
 * Normaliza qualquer variação para os 4 canônicos SEM acento.
 * Nunca retorna null (fallback 'Aberto').
 */
export function normalizeChamadoStatus(value?: string | null): ChamadoStatus {
  const n = normalizeBase(value);

  if (n.startsWith('conclu'))        return CHAMADO_STATUS.CONCLUIDO;      // concluido, concluído, conclu
  if (n.includes('andament'))        return CHAMADO_STATUS.EM_ANDAMENTO;   // em andamento / andamento
  if (n.startsWith('cancel'))        return CHAMADO_STATUS.CANCELADO;      // cancelado / cancel
  if (n.startsWith('abert'))         return CHAMADO_STATUS.ABERTO;         // aberto

  // fallback seguro
  return CHAMADO_STATUS.ABERTO;
}

/**
 * Normaliza qualquer variação de status de agendamento.
 * Nunca retorna null (fallback 'agendado').
 */
export function normalizeAgendamentoStatus(value?: string | null): AgendamentoStatus {
  const n = normalizeBase(value);

  if (n.startsWith('conclu'))  return AGENDAMENTO_STATUS.CONCLUIDO;
  if (n.startsWith('inici'))   return AGENDAMENTO_STATUS.INICIADO;
  if (n.startsWith('cancel'))  return AGENDAMENTO_STATUS.CANCELADO;
  if (n.startsWith('agend'))   return AGENDAMENTO_STATUS.AGENDADO;

  // fallback
  return AGENDAMENTO_STATUS.AGENDADO;
}

/* -------- Helpers úteis (opcional) -------- */

/** Map chave i18n para badge/classes “aberto|em_andamento|concluido|cancelado”. */
export const CHAMADO_STATUS_KEY: Record<ChamadoStatus, 'aberto' | 'em_andamento' | 'concluido' | 'cancelado'> = {
  [CHAMADO_STATUS.ABERTO]: 'aberto',
  [CHAMADO_STATUS.EM_ANDAMENTO]: 'em_andamento',
  [CHAMADO_STATUS.CONCLUIDO]: 'concluido',
  [CHAMADO_STATUS.CANCELADO]: 'cancelado',
};

/** Retorna a key i18n/badge a partir de qualquer string solta. */
export function chamadoStatusKey(value?: string | null) {
  return CHAMADO_STATUS_KEY[normalizeChamadoStatus(value)];
}

// Quais contam como "ativos" (para criação/listagens padrão)
export const CHAMADOS_ATIVOS: ChamadoStatus[] = [
  CHAMADO_STATUS.ABERTO,
  CHAMADO_STATUS.EM_ANDAMENTO,
];

// Helper de checagem (aceita string solta ou ChamadoStatus)
export function isStatusAtivo(value?: string | ChamadoStatus | null): boolean {
  const s = normalizeChamadoStatus(typeof value === 'string' ? value : (value ?? ''));
  return s === CHAMADO_STATUS.ABERTO || s === CHAMADO_STATUS.EM_ANDAMENTO;
}