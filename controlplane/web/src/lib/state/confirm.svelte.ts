export interface ConfirmRequest {
	title: string;
	message?: string;
	confirmLabel: string;
	tone: 'danger' | 'accent';
	onConfirm: () => void | Promise<void>;
}

let pending = $state<ConfirmRequest | null>(null);

export const confirmDialog = {
	get current() {
		return pending;
	},
	ask(req: ConfirmRequest) {
		pending = req;
	},
	close() {
		pending = null;
	}
};