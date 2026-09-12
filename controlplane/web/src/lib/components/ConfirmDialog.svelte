<script lang="ts">
	import Modal from './Modal.svelte';
	import Button from './Button.svelte';
	import { t } from '$lib/i18n';
	import { confirmDialog } from '$lib/state/confirm.svelte';

	let busy = $state(false);

	async function confirm() {
		const req = confirmDialog.current;
		if (!req || busy) return;
		busy = true;
		try {
			await req.onConfirm();
			confirmDialog.close();
		} finally {
			busy = false;
		}
	}
</script>

<Modal
	open={!!confirmDialog.current}
	title={confirmDialog.current?.title ?? ''}
	onClose={() => !busy && confirmDialog.close()}
>
	{#if confirmDialog.current?.message}
		<p class="text-sm text-[var(--of-muted)]">{confirmDialog.current.message}</p>
	{/if}

	{#snippet footer()}
		<Button size="sm" onclick={() => confirmDialog.close()} disabled={busy}>
			{t('generic.cancel')}
		</Button>
		<Button
			size="sm"
			variant={confirmDialog.current?.tone === 'danger' ? 'danger' : 'primary'}
			onclick={confirm}
			disabled={busy}
		>
			{confirmDialog.current?.confirmLabel ?? t('generic.confirm')}
		</Button>
	{/snippet}
</Modal>