import { defineConfig } from 'vitest/config'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath } from 'url'

/**
 * Vitest 配置：与 vite.config.ts 同源（vue 插件 + @ 别名），
 * 测试环境用 jsdom 以支持 WebView 风格 DOM / localStorage / Clipboard API。
 */
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.spec.ts'],
  },
})
