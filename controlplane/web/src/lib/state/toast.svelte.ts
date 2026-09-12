export interface Toast {
	id: number;
	message: string;
	tone: 'ok' | 'error' | 'info';
}

let nextId = 1;
let toasts = $state<Toast[]>([]);

function dismiss(id: number) {
	toasts = toasts.filter((t) => t.id !== id);
}

export const toast = {
	get all() {
		return toasts;
	},
	push(message: string, tone: Toast['tone'] = 'info', ttl = 4000) {
		const id = nextId++;
		toasts = [...toasts, { id, message, tone }];
		setTimeout(() => dismiss(id), ttl);
	},
	ok(message: string) {
		toast.push(message, 'ok');
	},
	error(message: string) {
		toast.push(message, 'error', 6000);
	},
	close(id: number) {
		dismiss(id);
	}
};