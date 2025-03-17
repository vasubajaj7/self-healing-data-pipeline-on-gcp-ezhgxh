import { test as base, Page, BrowserContext, expect } from '@playwright/test'; // @playwright/test ^1.32.0
import LoginPage from './page-objects/loginPage';
import { users, loginResponses } from './test-data';

// Storage keys for authentication data
const AUTH_TOKEN_KEY = 'auth_token';
const REFRESH_TOKEN_KEY = 'refresh_token';
const TOKEN_EXPIRY_KEY = 'token_expiry';
const USER_DATA_KEY = 'user_data';

/**
 * Sets up authentication storage with tokens for a specific user role
 * @param context Playwright browser context
 * @param userRole Role of the user to authenticate as
 */
async function setupAuthStorage(context: BrowserContext, userRole: string): Promise<void> {
  // Get the appropriate login response data for the user role
  const loginResponse = loginResponses[userRole];
  if (!loginResponse || !loginResponse.data) {
    throw new Error(`No login response data found for user role: ${userRole}`);
  }

  // Extract authentication tokens and data
  const { token, refreshToken, expiresAt, user } = loginResponse.data;

  // Set up local storage
  await context.addInitScript(({ token, refreshToken, expiresAt, user, keys }) => {
    localStorage.setItem(keys.AUTH_TOKEN_KEY, token);
    if (refreshToken) {
      localStorage.setItem(keys.REFRESH_TOKEN_KEY, refreshToken);
    }
    localStorage.setItem(keys.TOKEN_EXPIRY_KEY, expiresAt.toString());
    localStorage.setItem(keys.USER_DATA_KEY, JSON.stringify(user));
  }, { token, refreshToken, expiresAt, user, keys: { AUTH_TOKEN_KEY, REFRESH_TOKEN_KEY, TOKEN_EXPIRY_KEY, USER_DATA_KEY } });

  // Set up cookies if needed
  await context.addCookies([
    {
      name: 'auth_session',
      value: 'authenticated',
      domain: 'localhost',
      path: '/',
      httpOnly: false,
      secure: false,
      sameSite: 'Lax',
      expires: Math.floor(Date.now() / 1000) + 86400, // 24 hours
    }
  ]);
}

/**
 * Clears authentication storage from the browser context
 * @param context Playwright browser context
 */
async function clearAuthStorage(context: BrowserContext): Promise<void> {
  // Clear local storage
  await context.addInitScript(({ keys }) => {
    localStorage.removeItem(keys.AUTH_TOKEN_KEY);
    localStorage.removeItem(keys.REFRESH_TOKEN_KEY);
    localStorage.removeItem(keys.TOKEN_EXPIRY_KEY);
    localStorage.removeItem(keys.USER_DATA_KEY);
  }, { keys: { AUTH_TOKEN_KEY, REFRESH_TOKEN_KEY, TOKEN_EXPIRY_KEY, USER_DATA_KEY } });

  // Clear cookies
  await context.clearCookies();
}

// Extend the base test fixture
export const test = base.extend({
  // Default authenticated page (uses admin role)
  authenticatedPage: async ({ page, context }, use) => {
    // Set up authentication with admin user
    await setupAuthStorage(context, 'admin');
    
    // Navigate to the base URL
    await page.goto('/');
    
    // Verify authentication is successful
    await page.waitForSelector('[data-testid="dashboard-content"]', { state: 'visible' });
    
    // Use the authenticated page
    await use(page);
    
    // Clean up after test
    await clearAuthStorage(context);
  },

  // Admin role authenticated page
  adminPage: async ({ page, context }, use) => {
    // Set up authentication with admin user
    await setupAuthStorage(context, 'admin');
    
    // Navigate to the base URL
    await page.goto('/');
    
    // Verify admin authentication is successful
    await page.waitForSelector('[data-testid="dashboard-content"]', { state: 'visible' });
    
    // Use the authenticated page
    await use(page);
    
    // Clean up after test
    await clearAuthStorage(context);
  },

  // Engineer role authenticated page
  engineerPage: async ({ page, context }, use) => {
    // Set up authentication with engineer user
    await setupAuthStorage(context, 'engineer');
    
    // Navigate to the base URL
    await page.goto('/');
    
    // Verify engineer authentication is successful
    await page.waitForSelector('[data-testid="dashboard-content"]', { state: 'visible' });
    
    // Use the authenticated page
    await use(page);
    
    // Clean up after test
    await clearAuthStorage(context);
  },

  // Analyst role authenticated page
  analystPage: async ({ page, context }, use) => {
    // Set up authentication with analyst user
    await setupAuthStorage(context, 'analyst');
    
    // Navigate to the base URL
    await page.goto('/');
    
    // Verify analyst authentication is successful
    await page.waitForSelector('[data-testid="dashboard-content"]', { state: 'visible' });
    
    // Use the authenticated page
    await use(page);
    
    // Clean up after test
    await clearAuthStorage(context);
  },

  // Operator role authenticated page
  operatorPage: async ({ page, context }, use) => {
    // Set up authentication with operator user
    await setupAuthStorage(context, 'operator');
    
    // Navigate to the base URL
    await page.goto('/');
    
    // Verify operator authentication is successful
    await page.waitForSelector('[data-testid="dashboard-content"]', { state: 'visible' });
    
    // Use the authenticated page
    await use(page);
    
    // Clean up after test
    await clearAuthStorage(context);
  },

  // Login page fixture
  loginPage: async ({ page }, use) => {
    // Create a new LoginPage instance
    const loginPage = new LoginPage(page);
    
    // Navigate to the login page
    await loginPage.goto();
    
    // Use the login page
    await use(loginPage);
    
    // No cleanup needed for login page
  }
});

export { expect };