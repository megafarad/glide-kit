import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        environment: "node",      // you’re testing a Node lib/app
        globals: true,            // lets you use describe/it/test/expect without imports
        setupFiles: [],           // e.g., ["./test/setup.ts"] if you need one
        // Vitest will happily resolve TS behind ESM-style ".js" imports in source.
    },
});
