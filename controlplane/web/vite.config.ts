import { sveltekit } from '@sveltejs/kit/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	server: {
		port: 3000,
		strictPort: false,
		proxy: {
			// Dev uses the same single-origin split as production: /v1/* lives
			// at the origin and is forwarded straight to the Go controlplane.
			// The API base is overridable for people running controlplane
			// somewhere other than the default dev port.
			'/v1': {
				target: process.env.CONTROLPLANE_UPSTREAM || 'http://127.0.0.1:8080',
				changeOrigin: true
			}
		}
	}
});