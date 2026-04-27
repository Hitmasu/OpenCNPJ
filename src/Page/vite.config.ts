import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  base: './',
  plugins: [react()],
  publicDir: 'assets',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
});
