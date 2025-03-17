import { test as baseTest, expect, Page } from '@playwright/test'; // @playwright/test ^1.32.0
import LoginPage from '../fixtures/page-objects/loginPage';
import { users, loginResponses, mfaResponses } from '../fixtures/test-data';
import { test } from '../fixtures/auth.setup';

// Mock API responses
const mockLoginApi = async (page: Page) => {
  // Mock login API response
  await page.route('/api/auth/login', async (route, request) => {
    const requestBody = JSON.parse(request.postData() || '{}');
    const { username, password } = requestBody;

    if (username === users.admin.username && password === users.admin.password) {
      await route.fulfill({ status: 200, body: JSON.stringify(loginResponses.admin.data) });
    } else if (username === users.mfaUser.username && password === users.mfaUser.password) {
      await route.fulfill({ status: 200, body: JSON.stringify(loginResponses.mfaRequired.data) });
    } else if (username === users.inactive.username && password === users.inactive.password) {
      await route.fulfill({ status: 403, body: JSON.stringify(loginResponses.inactiveAccount.error) });
    } else {
      await route.fulfill({ status: 401, body: JSON.stringify(loginResponses.invalidCredentials.error) });
    }
  });
};

const mockMfaApi = async (page: Page) => {
  // Mock MFA verification API response
  await page.route('/api/auth/verify-mfa', async (route, request) => {
    const requestBody = JSON.parse(request.postData() || '{}');
    const { verificationCode } = requestBody;

    if (verificationCode === '123456') {
      await route.fulfill({ status: 200, body: JSON.stringify(mfaResponses.valid.data) });
    } else {
      await route.fulfill({ status: 401, body: JSON.stringify(mfaResponses.invalid.error) });
    }
  });
};

test.describe('Login Page', () => {
  let loginPage: LoginPage;

  test.beforeEach(async ({ page }) => {
    // Setup API mocks
    await mockLoginApi(page);
    await mockMfaApi(page);
    
    // Create a new LoginPage instance
    loginPage = new LoginPage(page);
    
    // Clear browser storage and cookies
    await page.context().clearCookies();
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
  });

  test.afterEach(async ({ page }) => {
    // Clear browser storage and cookies
    await page.context().clearCookies();
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
  });

  test('should display login form with all elements', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Verify username input is visible
    await expect(page.locator(loginPage.usernameInput)).toBeVisible();
    
    // Verify password input is visible
    await expect(page.locator(loginPage.passwordInput)).toBeVisible();
    
    // Verify login button is visible
    await expect(page.locator(loginPage.loginButton)).toBeVisible();
    
    // Verify remember me checkbox is visible
    await expect(page.locator(loginPage.rememberMeCheckbox)).toBeVisible();
    
    // Verify forgot password link is visible
    await expect(page.locator(loginPage.forgotPasswordLink)).toBeVisible();
  });

  test('should show validation errors for empty fields', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Click login button without entering credentials
    await page.click(loginPage.loginButton);
    
    // Verify username validation error is displayed
    const usernameError = await loginPage.getUsernameValidationError();
    expect(usernameError).not.toBeNull();
    expect(usernameError).toContain('Username is required');
    
    // Verify password validation error is displayed
    const passwordError = await loginPage.getPasswordValidationError();
    expect(passwordError).not.toBeNull();
    expect(passwordError).toContain('Password is required');
  });

  test('should toggle password visibility', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Enter a password
    await loginPage.fillPassword('TestPassword123!');
    
    // Verify password field type is 'password'
    expect(await loginPage.getPasswordFieldType()).toBe('password');
    
    // Click the show password button
    await loginPage.togglePasswordVisibility();
    
    // Verify password field type is 'text'
    expect(await loginPage.getPasswordFieldType()).toBe('text');
    
    // Click the show password button again
    await loginPage.togglePasswordVisibility();
    
    // Verify password field type is 'password'
    expect(await loginPage.getPasswordFieldType()).toBe('password');
  });

  test('should login successfully with valid credentials', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Login with admin user credentials
    await loginPage.login(users.admin.username, users.admin.password);
    
    // Verify redirect to dashboard page
    await page.waitForURL(/.*\/dashboard/);
    
    // Verify user profile element is visible
    await expect(page.locator('[data-testid="user-profile"]')).toBeVisible();
  });

  test('should show error message for invalid credentials', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Login with invalid credentials
    await loginPage.login('wronguser', 'wrongpassword');
    
    // Verify error message about invalid credentials is displayed
    const errorMessage = await loginPage.getErrorMessage();
    expect(errorMessage).not.toBeNull();
    expect(errorMessage).toContain('Invalid username or password');
    
    // Verify URL is still login page
    expect(page.url()).toContain('/login');
  });

  test('should show error message for inactive account', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Login with inactive user credentials
    await loginPage.login(users.inactive.username, users.inactive.password);
    
    // Verify error message about inactive account is displayed
    const errorMessage = await loginPage.getErrorMessage();
    expect(errorMessage).not.toBeNull();
    expect(errorMessage).toContain('Account is inactive');
    
    // Verify URL is still login page
    expect(page.url()).toContain('/login');
  });

  test('should handle MFA flow correctly', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Login with MFA-enabled user credentials
    await loginPage.login(users.mfaUser.username, users.mfaUser.password);
    
    // Verify MFA input is displayed
    await expect(page.locator(loginPage.mfaCodeInput)).toBeVisible();
    
    // Enter valid MFA code
    await loginPage.verifyMfa('123456');
    
    // Verify redirect to dashboard page
    await page.waitForURL(/.*\/dashboard/);
    
    // Verify user profile element is visible
    await expect(page.locator('[data-testid="user-profile"]')).toBeVisible();
  });

  test('should show error for invalid MFA code', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Login with MFA-enabled user credentials
    await loginPage.login(users.mfaUser.username, users.mfaUser.password);
    
    // Verify MFA input is displayed
    await expect(page.locator(loginPage.mfaCodeInput)).toBeVisible();
    
    // Enter invalid MFA code
    await loginPage.verifyMfa('999999');
    
    // Verify error message about invalid MFA code is displayed
    const errorMessage = await loginPage.getMfaErrorMessage();
    expect(errorMessage).not.toBeNull();
    expect(errorMessage).toContain('Invalid verification code');
    
    // Verify still on MFA verification screen
    await expect(page.locator(loginPage.mfaCodeInput)).toBeVisible();
  });

  test('should navigate to forgot password page', async ({ page }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Click the forgot password link
    await loginPage.clickForgotPassword();
    
    // Verify navigation to forgot password page
    expect(page.url()).toContain('/forgot-password');
    
    // Verify forgot password form is displayed
    await expect(page.locator('[data-testid="forgot-password-form"]')).toBeVisible();
  });

  test('should persist login with remember me option', async ({ page, browser }) => {
    // Navigate to the login page
    await loginPage.goto();
    
    // Login with valid credentials and remember me checked
    await loginPage.login(users.admin.username, users.admin.password, true);
    
    // Verify successful login
    await page.waitForURL(/.*\/dashboard/);
    
    // Get tokens from local storage to validate they exist
    const authToken = await page.evaluate(() => localStorage.getItem('auth_token'));
    expect(authToken).not.toBeNull();
    
    // Create a new browser context
    const newContext = await browser.newContext();
    const newPage = await newContext.newPage();
    
    // Manually inject the token into the new context
    await newPage.addInitScript(token => {
      localStorage.setItem('auth_token', token);
    }, authToken);
    
    // Navigate to the application in the new context
    await newPage.goto('/');
    
    // Verify user is still logged in without re-authentication
    await expect(newPage.locator('[data-testid="dashboard-content"]')).toBeVisible();
    
    // Clean up
    await newContext.close();
  });
});