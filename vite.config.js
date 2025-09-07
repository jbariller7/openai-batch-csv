import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Export a basic Vite configuration that enables the React plugin. Vite
// automatically serves the frontend in development and builds to the
// `dist` directory for production deployment. Netlify will use this
// configuration when running `npm run build` during deployment.
export default defineConfig({
  plugins: [react()],
});
