import { defineConfig } from 'vite'; // v4.3.9
import react from '@vitejs/plugin-react'; // v4.0.0
import tsconfigPaths from 'vite-tsconfig-paths'; // v4.2.0
import path from 'path';

/**
 * Vite configuration for the self-healing data pipeline frontend
 * 
 * This configuration provides:
 * - React support with Fast Refresh
 * - TypeScript path resolution
 * - Development server with API proxying
 * - Production build optimization
 * - Chunk splitting for better performance
 */
export default defineConfig(({ mode }) => {
  const isProduction = mode === 'production';
  
  return {
    plugins: [
      react(), // Enable React support with Fast Refresh
      tsconfigPaths() // Resolve imports using TypeScript path mappings
    ],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src') // Configure '@' to point to the src directory for clean imports
      }
    },
    server: {
      port: 3000,
      open: true, // Automatically open browser on dev server start
      proxy: {
        // Proxy API requests to backend server
        '/api': {
          target: 'http://localhost:8000',
          changeOrigin: true,
          secure: false
        },
        // Proxy WebSocket connections for real-time features
        '/ws': {
          target: 'ws://localhost:8000',
          ws: true
        }
      }
    },
    build: {
      outDir: 'dist',
      sourcemap: true, // Generate source maps for debugging
      minify: 'terser',
      terserOptions: {
        compress: {
          drop_console: isProduction // Remove console.log in production builds
        }
      },
      rollupOptions: {
        output: {
          manualChunks: {
            // Split bundles for better caching and performance
            vendor: ['react', 'react-dom', 'react-router-dom'], // Core libraries
            charts: ['recharts'] // Visualization libraries
          }
        }
      }
    },
    preview: {
      port: 3000 // Use the same port for the preview server
    },
    define: {
      global: 'window' // Define global as window for libraries that expect a global object
    }
  };
});