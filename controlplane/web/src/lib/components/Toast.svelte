<script lang="ts">
	import { CheckCircle2, XCircle, Info, X } from 'lucide-svelte';
	import { toast } from '$lib/state/toast.svelte';

	const icons = {
		ok: CheckCircle2,
		error: XCircle,
		info: Info
	} as const;

	const accents = {
		ok: 'text-[var(--of-ok)]',
		error: 'text-[var(--of-danger)]',
		info: 'text-[var(--of-accent)]'
	} as const;
</script>

<div class="fixed inset-x-0 bottom-4 z-[60] flex flex-col items-center gap-2 px-4 sm:items-end sm:pr-6">
	{#each toast.all as item (item.id)}
		{@const Icon = icons[item.tone]}
		<div
			class="pointer-events-auto flex w-full max-w-md items-start gap-3 rounded-lg border border-[var(--of-line)] bg-[var(--of-panel)] px-4 py-3 shadow-lg"
			role="status"
		>
			<span class={accents[item.tone]}><Icon class="mt-0.5 h-4 w-4 shrink-0" /></span>
			<p class="flex-1 text-sm text-[var(--of-ink)]">{item.message}</p>
			<button
				type="button"
				class="text-[var(--of-muted)] transition-colors hover:text-[var(--of-ink)]"
				onclick={() => toast.close(item.id)}
				aria-label="Dismiss"
			>
				<X class="h-4 w-4" />
			</button>
		</div>
	{/each}
</div>