import { sveltekit } from '@sveltejs/kit/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	server: {
		port: 3000,
		strictPort: false,
		proxy: {
			// Dev mirrors production's single-origin split: /v1/* forwards to the Go controlplane.
			'/v1': {
				target: process.env.CONTROLPLANE_UPSTREAM || 'http://127.0.0.1:8080',
				changeOrigin: true
			}
		}
	}
});