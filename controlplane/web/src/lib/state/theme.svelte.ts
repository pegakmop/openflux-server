import { browser } from '$app/environment';

export type Theme = 'dark' | 'light';

const THEME_KEY = 'openflux_web_theme';

function initialTheme(): Theme {
	if (!browser) return 'dark';
	try {
		const stored = localStorage.getItem(THEME_KEY);
		if (stored === 'dark' || stored === 'light') return stored;
	} catch {
		/* ignore */
	}
	return window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

const initialThemeValue = initialTheme();
let theme = $state<Theme>(initialThemeValue);

function apply(t: Theme) {
	// Also persisted pre-hydration by the inline script in app.html; keep this
	// as the source of truth for afterwards.
	if (browser) document.documentElement.dataset.theme = t;
}

export const themeStore = {
	get current() {
		return theme;
	},
	toggle() {
		theme = theme === 'dark' ? 'light' : 'dark';
		apply(theme);
		if (browser) {
			try {
				localStorage.setItem(THEME_KEY, theme);
			} catch {
				/* ignore */
			}
		}
	}
};

// Make sure the attribute matches even when the module initialises after the
// inline script (e.g. client-side navigation into the app).
apply(initialThemeValue);