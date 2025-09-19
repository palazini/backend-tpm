export function roleToFuncao(role?: string | null): string {
  const normalized = (role ?? '').toLowerCase();

  switch (normalized) {
    case 'gestor':
      return 'Gestor';
    case 'manutentor':
      return 'Técnico Eletromecânico';
    default:
      return 'Operador de CNC';
  }
}
