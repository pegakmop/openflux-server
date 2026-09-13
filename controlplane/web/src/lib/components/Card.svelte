<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		title?: string;
		hint?: string;
		actions?: Snippet;
		children: Snippet;
		flush?: boolean;
		class?: string;
	}

	let { title, hint, actions, children, flush = false, class: klass = '' }: Props = $props();
</script>

<section class="rounded-xl border border-[var(--of-line)] bg-[var(--of-panel)] shadow-sm {klass}">
	{#if title}
		<header class="flex flex-wrap items-center justify-between gap-2 px-4 pt-4 pb-2 sm:px-5">
			<div>
				<h2 class="text-sm font-semibold text-[var(--of-ink)]">{title}</h2>
				{#if hint}
					<p class="mt-0.5 text-xs text-[var(--of-muted)]">{hint}</p>
				{/if}
			</div>
			{#if actions}
				<div class="flex items-center gap-2">{@render actions()}</div>
			{/if}
		</header>
	{/if}
	<div class={flush ? '' : 'p-4 sm:p-5'}>
		{@render children()}
	</div>
</section>