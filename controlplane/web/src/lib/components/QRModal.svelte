<script lang="ts">
	import QRCode from 'qrcode';
	import Modal from './Modal.svelte';

	interface Props {
		open: boolean;
		text: string;
		title: string;
		hint?: string;
		onClose?: () => void;
	}

	let { open, text, title, hint, onClose }: Props = $props();

	let dataUrl = $state<string | null>(null);

	$effect(() => {
		if (!open || !text) {
			dataUrl = null;
			return;
		}
		let cancelled = false;
		dataUrl = null;
		QRCode.toDataURL(text, { width: 320, margin: 1 })
			.then((url) => {
				if (!cancelled) dataUrl = url;
			})
			.catch(() => {
				if (!cancelled) dataUrl = null;
			});
		return () => {
			cancelled = true;
		};
	});
</script>

<Modal {open} {title} {hint} {onClose}>
	<div class="flex flex-col items-center gap-4 py-2">
		<div class="rounded-xl border border-[var(--of-line)] bg-white p-3">
			{#if dataUrl}
				<img src={dataUrl} alt="QR" class="h-56 w-56" />
			{:else if open}
				<div class="flex h-56 w-56 items-center justify-center text-sm text-[var(--of-muted)]">…</div>
			{/if}
		</div>
		<p class="max-w-full text-center font-mono text-xs break-all text-[var(--of-muted)]">{text}</p>
	</div>
</Modal>