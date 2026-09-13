<script lang="ts">
	import { Copy, Check } from 'lucide-svelte';
	import { t } from '$lib/i18n';

	interface Props {
		label: string;
		value: string;
		hint?: string;
	}

	let { label, value, hint }: Props = $props();

	let copied = $state(false);

	async function copy() {
		try {
			await navigator.clipboard.writeText(value);
		} catch {
			const el = document.createElement('textarea');
			el.value = value;
			document.body.appendChild(el);
			el.select();
			document.execCommand('copy');
			el.remove();
		}
		copied = true;
		setTimeout(() => (copied = false), 1500);
	}
</script>

<div>
	<div class="mb-1 flex items-center justify-between gap-2">
		<span class="text-xs font-medium text-[var(--of-muted)]">{label}</span>
		<button
			type="button"
			class="inline-flex items-center gap-1 text-xs text-[var(--of-accent)] transition-colors hover:opacity-80"
			onclick={copy}
		>
			{#if copied}
				<Check class="h-3.5 w-3.5" />
				{t('generic.copied')}
			{:else}
				<Copy class="h-3.5 w-3.5" />
				{t('generic.copy')}
			{/if}
		</button>
	</div>
	<div
		class="of-code w-full overflow-x-auto rounded-lg border border-[var(--of-line)] bg-[var(--of-raise)] px-3 py-2.5 font-mono text-sm break-all text-[var(--of-ink)]"
		title={value}
	>
		{value}
	</div>
	{#if hint}
		<p class="mt-1 text-xs text-[var(--of-muted)]">{hint}</p>
	{/if}
</div>