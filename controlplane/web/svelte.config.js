import adapter from '@sveltejs/adapter-node';

const config = {
	kit: {
		adapter: adapter(),
		// The panel lives under /admin/, the URL the deployer prints; base strips that prefix so nothing else in the repo needs to change.
		paths: { base: '/admin' },
		csrf: {
			// The panel authenticates with a Bearer header over fetch, not HTML form posts, so origin-check CSRF has nothing to protect.
			trustedOrigins: ['http://127.0.0.1:3000', 'http://localhost:3000']
		}
	}
};

export default config;