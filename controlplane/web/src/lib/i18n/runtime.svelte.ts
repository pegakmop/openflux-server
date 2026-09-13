import { browser } from '$app/environment';

import { en } from './en';
import { ru, type Dict } from './ru';

export type Lang = 'ru' | 'en';
export const LANGS: Lang[] = ['ru', 'en'];

const LANG_KEY = 'openflux_web_lang';

function initialLang(): Lang {
	if (!browser) return 'ru';
	try {
		const stored = localStorage.getItem(LANG_KEY);
		if (stored === 'ru' || stored === 'en') return stored;
	} catch {
		/* ignore */
	}
	return 'ru';
}

let lang = $state<Lang>(initialLang());

export const i18n = {
	get lang() {
		return lang;
	},
	setLang(next: Lang) {
		lang = next;
		if (browser) {
			try {
				localStorage.setItem(LANG_KEY, next);
			} catch {
				/* ignore */
			}
			document.documentElement.lang = next;
		}
	}
};

export function t(key: keyof Dict | string, vars?: Record<string, string | number>): string {
	const dict: Dict = lang === 'en' ? en : ru;
	const value = dict[key as keyof Dict] ?? String(key);
	if (!vars) return value;
	return value.replace(/\{(\w+)\}/g, (_, name: string) => {
		const replacement = vars[name];
		return replacement == null ? '' : String(replacement);
	});
}