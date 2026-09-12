<script lang="ts">
	import { Cpu, MemoryStick, HardDrive, Activity, Server } from 'lucide-svelte';
	import { t, i18n } from '$lib/i18n';
	import type { SystemInfoDTO } from '$lib/api';
	import { formatBytes, formatPercent, formatUptime } from '$lib/format';

	interface Props {
		system: SystemInfoDTO | null;
	}

	let { system }: Props = $props();
</script>

{#if system}
	<div class="space-y-4">
		<dl class="grid grid-cols-2 gap-x-4 gap-y-3 text-sm">
			{#if system.hostname}
				<div class="flex items-center gap-2">
					<Server class="h-4 w-4 shrink-0 text-[var(--of-muted)]" />
					<div class="min-w-0">
						<dt class="text-xs text-[var(--of-muted)]">{t('dash.hostname')}</dt>
						<dd class="truncate font-medium text-[var(--of-ink)]">{system.hostname}</dd>
					</div>
				</div>
			{/if}
			{#if system.num_cpu > 0}
				<div class="flex items-center gap-2">
					<Cpu class="h-4 w-4 shrink-0 text-[var(--of-muted)]" />
					<div>
						<dt class="text-xs text-[var(--of-muted)]">{t('dash.cpu')}</dt>
						<dd class="font-medium text-[var(--of-ink)] tabular-nums">
							{formatPercent(system.cpu_percent)} ({system.num_cpu} {t('dash.cores')})
						</dd>
					</div>
				</div>
			{/if}
			{#if system.uptime_sec > 0}
				<div class="flex items-center gap-2">
					<Activity class="h-4 w-4 shrink-0 text-[var(--of-muted)]" />
					<div>
						<dt class="text-xs text-[var(--of-muted)]">{t('dash.load')}</dt>
						<dd class="font-medium text-[var(--of-ink)] tabular-nums">
							{system.load1.toFixed(1)} / {system.load5.toFixed(1)} / {system.load15.toFixed(1)}
						</dd>
					</div>
				</div>
			{/if}
			{#if system.uptime_sec > 0}
				<div>
					<dt class="text-xs text-[var(--of-muted)]">{t('dash.uptime')}</dt>
					<dd class="font-medium text-[var(--of-ink)] tabular-nums">
						{formatUptime(system.uptime_sec, i18n.lang)}
					</dd>
				</div>
			{/if}
		</dl>

		<!-- Overall CPU -->
		<div>
			<div class="mb-1 flex items-center justify-between text-xs">
				<span class="flex items-center gap-1.5 text-[var(--of-muted)]">
					<Cpu class="h-3.5 w-3.5" /> {t('dash.cpu')}
				</span>
				<span class="tabular-nums text-[var(--of-ink)]">{formatPercent(system.cpu_percent)}</span>
			</div>
			<div class="of-meter">
				<div
					class="of-meter-fill"
					class:of-meter-fill-warn={system.cpu_percent > 60}
					class:of-meter-fill-danger={system.cpu_percent > 85}
					style="width: {Math.min(100, system.cpu_percent)}%"
				></div>
			</div>
		</div>

		<!-- Memory -->
		{#if system.mem_total > 0}
			{@const pct = (system.mem_used / system.mem_total) * 100}
			<div>
				<div class="mb-1 flex items-center justify-between text-xs">
					<span class="flex items-center gap-1.5 text-[var(--of-muted)]">
						<MemoryStick class="h-3.5 w-3.5" /> {t('dash.memory')}
					</span>
					<span class="tabular-nums text-[var(--of-ink)]"
						>{formatBytes(system.mem_used, i18n.lang)}
						<span class="text-[var(--of-muted)]">/ {formatBytes(system.mem_total, i18n.lang)}</span></span
					>
				</div>
				<div class="of-meter">
					<div
						class="of-meter-fill"
						class:of-meter-fill-warn={pct > 80}
						class:of-meter-fill-danger={pct > 92}
						style="width: {Math.min(100, pct)}%"
					></div>
				</div>
			</div>
		{/if}

		<!-- Disk -->
		{#if system.disk_total > 0}
			{@const pct = (system.disk_used / system.disk_total) * 100}
			<div>
				<div class="mb-1 flex items-center justify-between text-xs">
					<span class="flex items-center gap-1.5 text-[var(--of-muted)]">
						<HardDrive class="h-3.5 w-3.5" /> {t('dash.disk')}
					</span>
					<span class="tabular-nums text-[var(--of-ink)]"
						>{formatBytes(system.disk_used, i18n.lang)}
						<span class="text-[var(--of-muted)]">/ {formatBytes(system.disk_total, i18n.lang)}</span></span
					>
				</div>
				<div class="of-meter">
					<div
						class="of-meter-fill"
						class:of-meter-fill-warn={pct > 80}
						class:of-meter-fill-danger={pct > 92}
						style="width: {Math.min(100, pct)}%"
					></div>
				</div>
			</div>
		{/if}
	</div>
{:else}
	<div class="flex h-40 items-center justify-center text-sm text-[var(--of-muted)]">
		{t('generic.loading')}
	</div>
{/if}