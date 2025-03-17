import { Page } from '@playwright/test'; // @playwright/test ^1.32.0
import { users } from '../test-data';

/**
 * Page object representing the login page of the application
 */
export default class LoginPage {
  readonly page: Page;
  readonly url: string;
  readonly usernameInput: string;
  readonly passwordInput: string;
  readonly loginButton: string;
  readonly rememberMeCheckbox: string;
  readonly forgotPasswordLink: string;
  readonly registerLink: string;
  readonly showPasswordButton: string;
  readonly errorMessage: string;
  readonly usernameValidationError: string;
  readonly passwordValidationError: string;
  readonly mfaCodeInput: string;
  readonly mfaVerifyButton: string;
  readonly mfaErrorMessage: string;

  /**
   * Initialize the LoginPage object with selectors
   * @param page Playwright page object
   */
  constructor(page: Page) {
    this.page = page;
    this.url = '/login';
    
    // Define selectors for login page elements
    this.usernameInput = '[data-testid="username-input"]';
    this.passwordInput = '[data-testid="password-input"]';
    this.loginButton = '[data-testid="login-button"]';
    this.rememberMeCheckbox = '[data-testid="remember-me-checkbox"]';
    this.forgotPasswordLink = '[data-testid="forgot-password-link"]';
    this.registerLink = '[data-testid="register-link"]';
    this.showPasswordButton = '[data-testid="show-password-button"]';
    this.errorMessage = '[data-testid="login-error-message"]';
    this.usernameValidationError = '[data-testid="username-validation-error"]';
    this.passwordValidationError = '[data-testid="password-validation-error"]';
    this.mfaCodeInput = '[data-testid="mfa-code-input"]';
    this.mfaVerifyButton = '[data-testid="mfa-verify-button"]';
    this.mfaErrorMessage = '[data-testid="mfa-error-message"]';
  }

  /**
   * Navigate to the login page
   * @returns Promise that resolves when navigation is complete
   */
  async goto(): Promise<void> {
    await this.page.goto(this.url);
    await this.page.waitForSelector(this.usernameInput, { state: 'visible' });
  }

  /**
   * Fill the username input field
   * @param username The username to enter
   * @returns Promise that resolves when username is filled
   */
  async fillUsername(username: string): Promise<void> {
    await this.page.fill(this.usernameInput, '');
    await this.page.fill(this.usernameInput, username);
  }

  /**
   * Fill the password input field
   * @param password The password to enter
   * @returns Promise that resolves when password is filled
   */
  async fillPassword(password: string): Promise<void> {
    await this.page.fill(this.passwordInput, '');
    await this.page.fill(this.passwordInput, password);
  }

  /**
   * Click the login button
   * @returns Promise that resolves when button is clicked
   */
  async clickLoginButton(): Promise<void> {
    await this.page.click(this.loginButton);
    await this.page.waitForResponse(response => 
      response.url().includes('/api/auth/login') && response.status() !== 0
    );
  }

  /**
   * Set the remember me checkbox state
   * @param checked Whether the checkbox should be checked
   * @returns Promise that resolves when checkbox state is set
   */
  async setRememberMe(checked: boolean): Promise<void> {
    const isChecked = await this.page.isChecked(this.rememberMeCheckbox);
    if (isChecked !== checked) {
      await this.page.click(this.rememberMeCheckbox);
    }
  }

  /**
   * Click the forgot password link
   * @returns Promise that resolves when link is clicked
   */
  async clickForgotPassword(): Promise<void> {
    await this.page.click(this.forgotPasswordLink);
    await this.page.waitForURL(/.*\/forgot-password/);
  }

  /**
   * Click the register link
   * @returns Promise that resolves when link is clicked
   */
  async clickRegister(): Promise<void> {
    await this.page.click(this.registerLink);
    await this.page.waitForURL(/.*\/register/);
  }

  /**
   * Toggle password visibility
   * @returns Promise that resolves when visibility is toggled
   */
  async togglePasswordVisibility(): Promise<void> {
    await this.page.click(this.showPasswordButton);
  }

  /**
   * Get the current type of the password field
   * @returns Promise that resolves with the password field type (password or text)
   */
  async getPasswordFieldType(): Promise<string> {
    return await this.page.getAttribute(this.passwordInput, 'type') || 'password';
  }

  /**
   * Get the error message text
   * @returns Promise that resolves with error message text or null if not present
   */
  async getErrorMessage(): Promise<string | null> {
    const isVisible = await this.page.isVisible(this.errorMessage);
    if (isVisible) {
      return await this.page.textContent(this.errorMessage);
    }
    return null;
  }

  /**
   * Get the username validation error text
   * @returns Promise that resolves with validation error text or null if not present
   */
  async getUsernameValidationError(): Promise<string | null> {
    const isVisible = await this.page.isVisible(this.usernameValidationError);
    if (isVisible) {
      return await this.page.textContent(this.usernameValidationError);
    }
    return null;
  }

  /**
   * Get the password validation error text
   * @returns Promise that resolves with validation error text or null if not present
   */
  async getPasswordValidationError(): Promise<string | null> {
    const isVisible = await this.page.isVisible(this.passwordValidationError);
    if (isVisible) {
      return await this.page.textContent(this.passwordValidationError);
    }
    return null;
  }

  /**
   * Check if MFA verification is required
   * @returns Promise that resolves with true if MFA is required, false otherwise
   */
  async isMfaRequired(): Promise<boolean> {
    return await this.page.isVisible(this.mfaCodeInput);
  }

  /**
   * Fill the MFA verification code input
   * @param code The verification code to enter
   * @returns Promise that resolves when MFA code is filled
   */
  async fillMfaCode(code: string): Promise<void> {
    await this.page.fill(this.mfaCodeInput, '');
    await this.page.fill(this.mfaCodeInput, code);
  }

  /**
   * Click the MFA verification button
   * @returns Promise that resolves when button is clicked
   */
  async clickVerifyMfaButton(): Promise<void> {
    await this.page.click(this.mfaVerifyButton);
    await this.page.waitForResponse(response => 
      response.url().includes('/api/auth/verify-mfa') && response.status() !== 0
    );
  }

  /**
   * Get the MFA error message text
   * @returns Promise that resolves with MFA error message text or null if not present
   */
  async getMfaErrorMessage(): Promise<string | null> {
    const isVisible = await this.page.isVisible(this.mfaErrorMessage);
    if (isVisible) {
      return await this.page.textContent(this.mfaErrorMessage);
    }
    return null;
  }

  /**
   * Perform login with provided credentials
   * @param username The username to login with
   * @param password The password to login with
   * @param rememberMe Whether to check the remember me checkbox
   * @returns Promise that resolves when login is attempted
   */
  async login(username: string, password: string, rememberMe = false): Promise<void> {
    // Go to login page if not already there
    if (!this.page.url().includes(this.url)) {
      await this.goto();
    }
    
    await this.fillUsername(username);
    await this.fillPassword(password);
    
    if (rememberMe) {
      await this.setRememberMe(true);
    }
    
    await this.clickLoginButton();
  }

  /**
   * Login with a predefined test user
   * @param userKey The key of the test user (e.g., 'admin', 'engineer')
   * @returns Promise that resolves when login is attempted
   */
  async loginWithTestUser(userKey: string): Promise<void> {
    const user = users[userKey];
    await this.login(user.username, user.password);
  }

  /**
   * Verify MFA with provided code
   * @param code The verification code to use
   * @returns Promise that resolves when MFA verification is attempted
   */
  async verifyMfa(code: string): Promise<void> {
    const mfaRequired = await this.isMfaRequired();
    if (mfaRequired) {
      await this.fillMfaCode(code);
      await this.clickVerifyMfaButton();
    } else {
      throw new Error('MFA verification not required but verifyMfa was called');
    }
  }

  /**
   * Check if user is logged in
   * @returns Promise that resolves with true if user is logged in, false otherwise
   */
  async isLoggedIn(): Promise<boolean> {
    return !this.page.url().includes(this.url) && 
           await this.page.isVisible('[data-testid="user-profile"]');
  }

  /**
   * Wait for successful login and redirect
   * @returns Promise that resolves when login is successful
   */
  async waitForLoginSuccess(): Promise<void> {
    // Wait for redirect away from login page
    await this.page.waitForURL(url => !url.pathname.includes('/login'));
    // Wait for dashboard elements to be visible
    await this.page.waitForSelector('[data-testid="dashboard-content"]', { state: 'visible' });
  }
}