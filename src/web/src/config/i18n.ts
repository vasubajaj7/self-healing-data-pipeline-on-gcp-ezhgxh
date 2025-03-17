import i18n from 'i18next'; // v22.4.6
import { initReactI18next } from 'react-i18next'; // v12.1.1
import LanguageDetector from 'i18next-browser-languagedetector'; // v7.0.1
import Backend from 'i18next-http-backend'; // v2.1.1
import { DEFAULT_LANGUAGE } from './constants';

/**
 * Internationalization Configuration
 * 
 * Sets up i18next with the following features:
 * - Browser language detection
 * - HTTP backend for loading translations
 * - React integration
 * - Multiple namespaces for different parts of the application
 */
i18n
  // Use the HTTP backend to load translations
  .use(Backend)
  // Detect user language
  .use(LanguageDetector)
  // Use React integration
  .use(initReactI18next)
  // Initialize i18next
  .init({
    // Set default language from constants
    lng: DEFAULT_LANGUAGE,
    // Fallback to English if translation is missing
    fallbackLng: 'en',
    // Default namespace is 'common'
    defaultNS: 'common',
    // Language detection options
    detection: {
      // Order of language detection
      order: ['localStorage', 'navigator', 'htmlTag'],
      // Cache language detection in localStorage
      caches: ['localStorage'],
    },
    // Backend configuration
    backend: {
      // Path to load translation files
      loadPath: '/locales/{{lng}}/{{ns}}.json',
    },
    // Interpolation options
    interpolation: {
      // React already escapes values, so we don't need i18next to escape them
      escapeValue: false,
    },
    // React options
    react: {
      // Use React Suspense for loading translations
      useSuspense: true,
    },
    // Debug mode (enable only in development)
    debug: process.env.NODE_ENV === 'development',
  });

export default i18n;