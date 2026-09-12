<script lang="ts">
	import { browser } from '$app/environment';
	import { page } from '$app/stores';
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import '../app.css';
	import Toast from '$lib/components/Toast.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import { auth } from '$lib/state/auth.svelte';

	let { children } = $props();

	const LOGIN_HREF = `${base}/login`;

	$effect(() => {
		if (!browser) return;
		const p = $page.url.pathname.replace(/\/+$/, '');
		const login = LOGIN_HREF.replace(/\/+$/, '');
		if (auth.isAuthed && p === login) {
			goto(`${base}/`);
		} else if (!auth.isAuthed && p !== login && p !== base) {
			goto(LOGIN_HREF);
		}
	});
</script>

{@render children()}
<Toast />
<ConfirmDialog />