import js from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  {
    ignores: [
      'dist/',
      'coverage/',
      'node_modules/',
      'data/',
      'data-test/',
      'architecturte/',
      'test-automation/',
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  {
    languageOptions: {
      parserOptions: { project: './tsconfig.json', tsconfigRootDir: import.meta.dirname },
    },
  },
  {
    // The browser page: plain JS, run in the browser, not type-checked.
    files: ['src/harvest/ui/**/*.js'],
    ...tseslint.configs.disableTypeChecked,
    languageOptions: {
      sourceType: 'module',
      globals: {
        document: 'readonly',
        window: 'readonly',
        location: 'readonly',
        fetch: 'readonly',
        setTimeout: 'readonly',
        clearTimeout: 'readonly',
      },
    },
  },
);
