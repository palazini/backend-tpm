export type ChamadoStatus = 'Aberto' | 'Em Andamento' | 'Concluido';

// útil quando você quer filtrar “ativos” em consultas
export const CHAMADOS_ATIVOS: ChamadoStatus[] = ['Aberto', 'Em Andamento'];
