import { browser } from '$app/environment';

// Keeps the same localStorage key the old embedded panel used, so anyone who
// already logged in once doesn't have to re-enter the token after an upgrade.
const TOKEN_KEY = 'openflux_admin_token';

function readToken(): string | null {
	if (!browser) return null;
	try {
		return localStorage.getItem(TOKEN_KEY);
	} catch {
		return null;
	}
}

let token = $state<string | null>(readToken());

export const auth = {
	get token() {
		return token;
	},
	get isAuthed() {
		return !!token && token.length > 0;
	},
	set(t: string) {
		token = t;
		if (browser) {
			try {
				localStorage.setItem(TOKEN_KEY, t);
			} catch {
				/* storage unavailable - session-only */
			}
		}
	},
	clear() {
		token = null;
		if (browser) {
			try {
				localStorage.removeItem(TOKEN_KEY);
			} catch {
				/* ignore */
			}
		}
	}
};