// tsup.config.ts
import { defineConfig } from 'tsup';

export default defineConfig({
    entry: ['src/index.ts'], // Specify your entry file(s) here
    format: ['esm', "cjs"],
    dts: true,
    clean: true,
    target: 'node20',
    splitting: false,
    shims: false,
    treeshake: true,
    minify: false,
});