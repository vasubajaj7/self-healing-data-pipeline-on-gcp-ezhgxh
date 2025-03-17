/// <reference types="cypress" />

// Define response types for better typing
interface LoginResponse {
  token?: string;
  user?: {
    id: string;
    email: string;
    name: string;
  };
  requiresMfa?: boolean;
  tempToken?: string;
  error?: string;
}

interface MfaVerifyResponse {
  token?: string;
  user?: {
    id: string;
    email: string;
    name: string;
  };
  error?: string;
}

interface ForgotPasswordResponse {
  message?: string;
  error?: string;
}

describe('Login Page', () => {
  // Reusable selectors for UI elements
  const selectors = {
    usernameInput: '[data-cy=username-input]',
    passwordInput: '[data-cy=password-input]',
    loginButton: '[data-cy=login-button]',
    rememberMeCheckbox: '[data-cy=remember-me]',
    forgotPasswordLink: '[data-cy=forgot-password-link]',
    errorAlert: '[data-cy=error-alert]',
    verificationCodeInput: '[data-cy=verification-code-input]',
    verifyButton: '[data-cy=verify-button]',
    forgotPasswordForm: '[data-cy=forgot-password-form]',
    emailInput: '[data-cy=email-input]',
    submitButton: '[data-cy=submit-button]',
    backToLoginLink: '[data-cy=back-to-login-link]',
    successMessage: '[data-cy=success-message]'
  };

  before(() => {
    // Configure test environment
    cy.fixture('users.json').as('userData');
  });

  beforeEach(() => {
    // Reset API interception and browser state before each test
    cy.clearCookies();
    cy.clearLocalStorage();
  });

  afterEach(() => {
    // Clean up any test artifacts
  });

  it('should display login form with all elements', () => {
    // Visit the login page
    cy.visit('/login');
    
    // Verify the application title is visible
    cy.contains('Self-Healing Data Pipeline').should('be.visible');
    
    // Verify the username input field is visible
    cy.get(selectors.usernameInput).should('be.visible');
    
    // Verify the password input field is visible
    cy.get(selectors.passwordInput).should('be.visible');
    
    // Verify the 'Remember me' checkbox is visible
    cy.get(selectors.rememberMeCheckbox).should('be.visible');
    
    // Verify the login button is visible
    cy.get(selectors.loginButton).should('be.visible');
    
    // Verify the 'Forgot password' link is visible
    cy.get(selectors.forgotPasswordLink).should('be.visible');
  });

  it('should successfully login with valid credentials', () => {
    // Get test data from fixture
    cy.get('@userData').then((userData: any) => {
      const { validUser } = userData;
      
      // Use custom login command
      cy.login(validUser.email, validUser.password);
      
      // Verify successful navigation to dashboard
      cy.url().should('include', '/dashboard');
    });
  });

  it('should display error message with invalid credentials', () => {
    // Mock API response for invalid credentials
    cy.mockApiResponse('/api/auth/login', {
      statusCode: 401,
      body: {
        error: 'Invalid email or password'
      }
    });
    
    // Visit the login page
    cy.visit('/login');
    
    // Type invalid username and password
    cy.get(selectors.usernameInput).type('invalid@example.com');
    cy.get(selectors.passwordInput).type('wrongpassword');
    
    // Click the login button
    cy.get(selectors.loginButton).click();
    
    // Wait for API response
    cy.waitForApiResponse('/api/auth/login');
    
    // Verify error message is displayed
    cy.get(selectors.errorAlert)
      .should('be.visible')
      .and('contain', 'Invalid email or password');
  });

  it('should display error message for inactive account', () => {
    // Get test data from fixture
    cy.get('@userData').then((userData: any) => {
      const { inactiveUser } = userData;
      
      // Mock API response for inactive account
      cy.mockApiResponse('/api/auth/login', {
        statusCode: 403,
        body: {
          error: 'Your account is inactive'
        }
      });
      
      // Visit the login page
      cy.visit('/login');
      
      // Type credentials for inactive account
      cy.get(selectors.usernameInput).type(inactiveUser.email);
      cy.get(selectors.passwordInput).type(inactiveUser.password);
      
      // Click the login button
      cy.get(selectors.loginButton).click();
      
      // Wait for API response
      cy.waitForApiResponse('/api/auth/login');
      
      // Verify error message is displayed
      cy.get(selectors.errorAlert)
        .should('be.visible')
        .and('contain', 'Your account is inactive');
    });
  });

  it('should redirect to MFA verification when required', () => {
    // Get test data from fixture
    cy.get('@userData').then((userData: any) => {
      const { mfaUser } = userData;
      
      // Mock API response for MFA required
      cy.mockApiResponse('/api/auth/login', {
        statusCode: 200,
        body: {
          requiresMfa: true,
          tempToken: 'temp-token-for-mfa'
        }
      });
      
      // Visit the login page
      cy.visit('/login');
      
      // Type credentials for MFA-enabled account
      cy.get(selectors.usernameInput).type(mfaUser.email);
      cy.get(selectors.passwordInput).type(mfaUser.password);
      
      // Click the login button
      cy.get(selectors.loginButton).click();
      
      // Wait for API response
      cy.waitForApiResponse('/api/auth/login');
      
      // Verify MFA verification form is displayed
      cy.contains('Two-Factor Authentication').should('be.visible');
      
      // Verify verification code input field is visible
      cy.get(selectors.verificationCodeInput).should('be.visible');
      
      // Verify verify button is visible
      cy.get(selectors.verifyButton).should('be.visible');
    });
  });

  it('should complete MFA verification successfully', () => {
    // Get test data from fixture
    cy.get('@userData').then((userData: any) => {
      const { mfaUser } = userData;
      
      // Use custom loginWithMfa command
      cy.loginWithMfa(mfaUser.email, mfaUser.password, '123456');
      
      // Verify successful navigation to dashboard
      cy.url().should('include', '/dashboard');
    });
  });

  it('should display error for invalid MFA verification code', () => {
    // Get test data from fixture
    cy.get('@userData').then((userData: any) => {
      const { mfaUser } = userData;
      
      // Mock API response for MFA required
      cy.mockApiResponse('/api/auth/login', {
        statusCode: 200,
        body: {
          requiresMfa: true,
          tempToken: 'temp-token-for-mfa'
        }
      });
      
      // Mock API response for invalid MFA code
      cy.mockApiResponse('/api/auth/mfa/verify', {
        statusCode: 400,
        body: {
          error: 'Invalid verification code'
        }
      });
      
      // Visit the login page
      cy.visit('/login');
      
      // Type credentials for MFA-enabled account
      cy.get(selectors.usernameInput).type(mfaUser.email);
      cy.get(selectors.passwordInput).type(mfaUser.password);
      
      // Click the login button
      cy.get(selectors.loginButton).click();
      
      // Wait for login API response
      cy.waitForApiResponse('/api/auth/login');
      
      // Verify MFA verification form is displayed
      cy.contains('Two-Factor Authentication').should('be.visible');
      
      // Type invalid verification code
      cy.get(selectors.verificationCodeInput).type('999999');
      
      // Click the verify button
      cy.get(selectors.verifyButton).click();
      
      // Wait for MFA verify API response
      cy.waitForApiResponse('/api/auth/mfa/verify');
      
      // Verify error message is displayed
      cy.get(selectors.errorAlert)
        .should('be.visible')
        .and('contain', 'Invalid verification code');
    });
  });

  it('should navigate to forgot password form when link is clicked', () => {
    // Visit the login page
    cy.visit('/login');
    
    // Click the 'Forgot password' link
    cy.get(selectors.forgotPasswordLink).click();
    
    // Verify forgot password form is displayed
    cy.get(selectors.forgotPasswordForm).should('be.visible');
    
    // Verify email input field is visible
    cy.get(selectors.emailInput).should('be.visible');
    
    // Verify submit button is visible
    cy.get(selectors.submitButton).should('be.visible');
    
    // Verify back to login link is visible
    cy.get(selectors.backToLoginLink).should('be.visible');
  });

  it('should navigate back to login form from forgot password', () => {
    // Visit the login page
    cy.visit('/login');
    
    // Click the 'Forgot password' link
    cy.get(selectors.forgotPasswordLink).click();
    
    // Verify forgot password form is displayed
    cy.get(selectors.forgotPasswordForm).should('be.visible');
    
    // Click the 'Back to login' link
    cy.get(selectors.backToLoginLink).click();
    
    // Verify login form is displayed again
    cy.get(selectors.usernameInput).should('be.visible');
    cy.get(selectors.passwordInput).should('be.visible');
    cy.get(selectors.loginButton).should('be.visible');
  });

  it('should submit forgot password request successfully', () => {
    // Mock API response for forgot password success
    cy.mockApiResponse('/api/auth/forgot-password', {
      statusCode: 200,
      body: {
        message: 'Password reset email sent successfully'
      }
    });
    
    // Visit the login page
    cy.visit('/login');
    
    // Click the 'Forgot password' link
    cy.get(selectors.forgotPasswordLink).click();
    
    // Type valid email address
    cy.get(selectors.emailInput).type('user@example.com');
    
    // Click the submit button
    cy.get(selectors.submitButton).click();
    
    // Wait for API response
    cy.waitForApiResponse('/api/auth/forgot-password');
    
    // Verify success message is displayed
    cy.get(selectors.successMessage)
      .should('be.visible')
      .and('contain', 'Password reset email sent');
  });

  it('should maintain form values after failed login attempt', () => {
    // Mock API response for invalid credentials
    cy.mockApiResponse('/api/auth/login', {
      statusCode: 401,
      body: {
        error: 'Invalid email or password'
      }
    });
    
    // Visit the login page
    cy.visit('/login');
    
    // Type username 'test@example.com'
    cy.get(selectors.usernameInput).type('test@example.com');
    
    // Type password 'password123'
    cy.get(selectors.passwordInput).type('password123');
    
    // Click the login button
    cy.get(selectors.loginButton).click();
    
    // Wait for API response
    cy.waitForApiResponse('/api/auth/login');
    
    // Verify error message is displayed
    cy.get(selectors.errorAlert).should('be.visible');
    
    // Verify username field still contains 'test@example.com'
    cy.get(selectors.usernameInput).should('have.value', 'test@example.com');
    
    // Verify password field is cleared for security
    cy.get(selectors.passwordInput).should('have.value', '');
  });

  it('should redirect to original URL after successful login', () => {
    // Set local storage to simulate redirect URL '/pipeline-management'
    cy.window().then((win) => {
      win.localStorage.setItem('redirectUrl', '/pipeline-management');
    });
    
    // Mock API response for successful login
    cy.mockApiResponse('/api/auth/login', {
      statusCode: 200,
      body: {
        token: 'valid-auth-token',
        user: {
          id: '123',
          email: 'user@example.com',
          name: 'Test User'
        }
      }
    });
    
    // Visit the login page
    cy.visit('/login');
    
    // Type valid username and password
    cy.get(selectors.usernameInput).type('user@example.com');
    cy.get(selectors.passwordInput).type('password123');
    
    // Click the login button
    cy.get(selectors.loginButton).click();
    
    // Wait for API response
    cy.waitForApiResponse('/api/auth/login');
    
    // Verify navigation to '/pipeline-management' instead of default dashboard
    cy.url().should('include', '/pipeline-management');
  });
});