<script lang="ts">
	import { t } from '$lib/i18n';
	import { formatBytes, formatDateShort } from '$lib/format';

	interface Point {
		day: string;
		sent: number;
		recv: number;
	}

	interface Props {
		data: Point[];
		lang?: 'ru' | 'en';
		height?: number;
	}

	let { data, lang = 'ru', height = 190 }: Props = $props();

	const W = 800;
	const H = $derived(height);
	const PLOT_TOP = 14;
	const PLOT_BOTTOM = $derived(H - 30);
	const PLOT_HEIGHT = $derived(PLOT_BOTTOM - PLOT_TOP);

	const totals = $derived(data.map((d) => d.sent + d.recv));
	const max = $derived(Math.max(...totals, 1));

	const frames = $derived(
		[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000]
			.find((f) => max / f <= 5) ?? 1_000_000
	);
	const ySteps = $derived(
		((): number[] => {
			const f = frames;
			const raw = [];
			let v = f;
			while (v < max * 1.1) {
				raw.push(v);
				v += f;
			}
			raw.push(f * Math.ceil((max * 1.1) / f));
			return raw;
		})()
	);

	function yFor(value: number): number {
		return PLOT_BOTTOM - (value / (max * 1.1)) * PLOT_HEIGHT;
	}

	const slot = $derived(data.length > 0 ? W / data.length : 1);
	const barW = $derived(Math.max(2, slot * 0.62));
	const labelEvery = $derived(Math.max(1, Math.ceil(data.length / 14)));
</script>

<div class="w-full" style="height: {height}px">
	<svg viewBox="0 0 {W} {H}" class="h-full w-full" preserveAspectRatio="none">
		{#if data.length === 0}
			<text
				x="50%"
				y="50%"
				text-anchor="middle"
				fill="var(--of-muted)"
				font-size="13"
			>{t('generic.loading')}</text>
		{:else}
			<g stroke="var(--of-line)" stroke-width="1">
				{#each ySteps as step (step)}
					<line x1="0" x2="{W}" y1="{yFor(step)}" y2="{yFor(step)}" />
				{/each}
			</g>
			<g class="tabular-nums" fill="var(--of-muted)" font-size="10">
				{#each ySteps as step (step)}
					<text x="4" y="{yFor(step) - 3}">{formatBytes(step, lang)}</text>
				{/each}
			</g>
			<g>
				{#each data as d, i (d.day)}
					{@const x = i * slot + (slot - barW) / 2}
					{@const yRecv = yFor(d.recv)}
					{@const ySent = yFor(d.sent + d.recv)}
					<g>
						<title>{formatDateShort(d.day, lang)}: {formatBytes(d.sent + d.recv, lang)} ({t('dash.sent')}: {formatBytes(d.sent, lang)})</title>
						<rect x="{x}" y="{yRecv}" width="{barW}" height="{Math.max(0, PLOT_BOTTOM - yRecv)}" rx="2" fill="var(--of-accent)" />
						<rect x="{x}" y="{ySent}" width="{barW}" height="{Math.max(0, PLOT_BOTTOM - ySent)}" fill="var(--of-warn)" />
					</g>
				{/each}
			</g>
			<g fill="var(--of-muted)" font-size="10" text-anchor="middle">
				{#each data as d, i (d.day)}
					{#if i % labelEvery === 0}
						<text x="{i * slot + slot / 2}" y="{H - 10}">{d.day.slice(5)}</text>
					{/if}
				{/each}
			</g>
		{/if}
	</svg>
</div>