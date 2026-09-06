import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: { include: ['src/**/*.test.{js,jsx}'] },
  build: { rollupOptions: { output: { manualChunks: { charts: ['chart.js', 'react-chartjs-2', 'chartjs-plugin-annotation'], react: ['react', 'react-dom', 'react-router-dom'] } } } },
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
    },
  },
})
