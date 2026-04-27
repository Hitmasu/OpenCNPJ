export function formatBytes(bytes: number | string | null | undefined) {
  const value = Number(bytes || 0);
  if (!Number.isFinite(value) || value <= 0) return '-';

  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  const unitValue = value / 1024 ** index;

  return `${unitValue.toLocaleString('pt-BR', { maximumFractionDigits: 1 })} ${units[index]}`;
}

export function formatDate(value: string | null | undefined) {
  if (!value) return '-';

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);

  return date.toLocaleString('pt-BR', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function formatCount(value: number | null | undefined) {
  return typeof value === 'number' ? value.toLocaleString('pt-BR') : '-';
}
