import commonjs from '@rollup/plugin-commonjs';
import resolve from '@rollup/plugin-node-resolve';
import typescript from '@rollup/plugin-typescript';
import { builtinModules } from 'node:module';

export default {
  input: 'amp/plugin.ts',
  output: { file: 'dist/plugin.js', format: 'esm' },
  // The runtime provides Node built-ins; @ampcode/plugin is only imported for types.
  external: (id) => id.startsWith('node:') || builtinModules.includes(id),
  plugins: [
    typescript({
      include: ['core/**/*.ts', 'c-to-rust/**/*.ts', 'redisearch/**/*.ts', 'amp/**/*.ts'],
      exclude: ['**/test/**'],
      noEmit: false,
      outDir: 'dist',
    }),
    resolve({ exportConditions: ['node'], preferBuiltins: true }),
    commonjs(),
  ],
  onwarn(warning, warn) {
    if (warning.code === 'CIRCULAR_DEPENDENCY' && warning.ids?.[0].includes('node_modules')) return;
    warn(warning);
  },
};
