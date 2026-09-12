<script lang="ts">
	import { base } from '$app/paths';
	import { t } from '$lib/i18n';
	import Button from '$lib/components/Button.svelte';

	let { status, message }: { status: number; message?: string } = $props();
</script>

<svelte:head>
	<title>OpenFlux · {status}</title>
</svelte:head>

<div class="flex min-h-screen flex-col items-center justify-center gap-4 bg-[var(--of-bg)] px-4">
	<p class="text-6xl font-bold text-[var(--of-accent)] tabular-nums">{status}</p>
	<p class="text-lg font-medium text-[var(--of-ink)]">
		{status === 404 ? 'Not found' : t('generic.error')}
	</p>
	{#if message}
		<p class="max-w-md text-center font-mono text-xs break-all text-[var(--of-muted)]">{message}</p>
	{/if}
	<a
		href={`${base}/`}
		class="rounded-lg bg-[var(--of-accent)] px-3.5 py-2 text-sm font-medium text-white transition-opacity hover:opacity-90"
	>
		{t('nav.dashboard')}
	</a>
</div>