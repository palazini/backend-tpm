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

function normalize(value?: string | null): string {
  return String(value ?? '')
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .trim();
}

export function normalizeChamadoStatus(value?: string | null): ChamadoStatus | null {
  const normalized = normalize(value);
  if (normalized.startsWith('conclu')) return CHAMADO_STATUS.CONCLUIDO;
  if (normalized.startsWith('em and')) return CHAMADO_STATUS.EM_ANDAMENTO;
  if (normalized.startsWith('abert')) return CHAMADO_STATUS.ABERTO;
  if (normalized.startsWith('canc')) return CHAMADO_STATUS.CANCELADO;
  return null;
}

export function normalizeAgendamentoStatus(value?: string | null): AgendamentoStatus | null {
  const normalized = normalize(value);
  if (normalized.startsWith('conclu')) return AGENDAMENTO_STATUS.CONCLUIDO;
  if (normalized.startsWith('inici')) return AGENDAMENTO_STATUS.INICIADO;
  if (normalized.startsWith('canc')) return AGENDAMENTO_STATUS.CANCELADO;
  if (normalized.startsWith('agend')) return AGENDAMENTO_STATUS.AGENDADO;
  return null;
}
