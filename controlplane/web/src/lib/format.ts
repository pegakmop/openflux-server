// Byte/date/id formatting shared across the panel.

export function formatBytes(n: number | null | undefined, lang: 'ru' | 'en' = 'ru'): string {
	if (n == null || isNaN(n)) return '—';
	const units = lang === 'ru' ? ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ'] : ['B', 'KB', 'MB', 'GB', 'TB'];
	let v = Number(n);
	let i = 0;
	while (v >= 1024 && i < units.length - 1) {
		v /= 1024;
		i++;
	}
	const digits = v >= 100 ? 0 : v >= 10 ? 1 : 2;
	return `${v.toFixed(digits)} ${units[i]}`;
}

export function formatDate(value: string | null | undefined, lang: 'ru' | 'en' = 'ru'): string {
	if (!value) return '—';
	const d = new Date(value);
	if (isNaN(d.getTime())) return value;
	return d.toLocaleString(lang === 'ru' ? 'ru-RU' : 'en-GB', {
		day: '2-digit',
		month: '2-digit',
		year: 'numeric',
		hour: '2-digit',
		minute: '2-digit'
	});
}

export function formatDateShort(value: string | null | undefined, lang: 'ru' | 'en' = 'ru'): string {
	if (!value) return '—';
	const d = new Date(value);
	if (isNaN(d.getTime())) return value;
	return d.toLocaleDateString(lang === 'ru' ? 'ru-RU' : 'en-GB');
}

export function shortId(id: string): string {
	return id.length > 8 ? `${id.slice(0, 8)}…` : id;
}

export function formatPercent(value: number | null | undefined): string {
	if (value == null || isNaN(value)) return '—';
	const v = Math.max(0, Math.min(100, value));
	return `${v.toFixed(v >= 10 ? 0 : 1)}%`;
}

export function formatUptime(seconds: number | null | undefined, lang: 'ru' | 'en' = 'ru'): string {
	if (seconds == null || seconds < 0) return '—';
	const s = Math.floor(seconds);
	const days = Math.floor(s / 86400);
	const hours = Math.floor((s % 86400) / 3600);
	const mins = Math.floor((s % 3600) / 60);
	const dLabel = lang === 'ru' ? 'д' : 'd';
	const hLabel = lang === 'ru' ? 'ч' : 'h';
	const mLabel = lang === 'ru' ? 'м' : 'm';
	if (days > 0) return `${days}${dLabel} ${hours}${hLabel}`;
	if (hours > 0) return `${hours}${hLabel} ${mins}${mLabel}`;
	return `${mins}${mLabel}`;
}

export function timeAgoMs(value: string | null | undefined, now = Date.now()): number | null {
	if (!value) return null;
	const d = new Date(value).getTime();
	if (isNaN(d)) return null;
	return now - d;
}