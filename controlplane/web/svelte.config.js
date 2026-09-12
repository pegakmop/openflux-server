import adapter from '@sveltejs/adapter-node';

const config = {
	kit: {
		adapter: adapter(),
		// The panel lives under /admin/ (the URL the deployer has been printing
		// since the Go-embedded panel era). With base set, SvelteKit serves all
		// of its routes below /admin and strips the prefix before routing, so
		// nothing else in the repo (nginx split, deployssh panel URL) needs to
		// change.
		paths: { base: '/admin' },
		csrf: {
			// The panel authenticates with a Bearer header over fetch (no HTML
			// form posts across origins), so the default origin-check CSRF
			// protection has nothing to protect and would only reject
			// proxied requests whose Origin header got rewritten.
			trustedOrigins: ['http://127.0.0.1:3000', 'http://localhost:3000']
		}
	}
};

export default config;