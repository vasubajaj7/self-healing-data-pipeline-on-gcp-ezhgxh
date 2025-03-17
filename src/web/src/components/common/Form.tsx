import React from 'react';
import { Formik, Form as FormikForm, FormikProps, FormikHelpers } from 'formik'; // formik ^2.2.9
import { Box, Paper, Typography, styled } from '@mui/material'; // @mui/material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11
import { lightTheme } from '../../theme/theme';

/**
 * Props interface for the Form component
 */
interface FormProps<T extends object> {
  initialValues: T;
  validationSchema?: yup.ObjectSchema<any>;
  onSubmit: (values: T, helpers: FormikHelpers<T>) => void | Promise<void>;
  children: React.ReactNode | ((props: FormikProps<T>) => React.ReactNode);
  title?: string;
  subtitle?: string;
  enableReinitialize?: boolean;
  validateOnChange?: boolean;
  validateOnBlur?: boolean;
  validateOnMount?: boolean;
  elevation?: number;
  variant?: 'outlined' | 'elevation';
  padding?: number | string;
  width?: string | number;
  maxWidth?: string | number;
  className?: string;
}

/**
 * Styled Paper component for the form container
 */
const StyledFormContainer = styled(Paper)<{ width: string | number; maxWidth: string | number }>(
  ({ theme, width, maxWidth }) => ({
    width,
    maxWidth,
    margin: '0 auto',
    overflow: 'hidden',
    borderRadius: theme.shape.borderRadius,
    backgroundColor: theme.palette.background.paper,
  })
);

/**
 * Styled Formik Form component
 */
const StyledForm = styled(FormikForm)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(2),
}));

/**
 * Styled Typography component for form title
 */
const FormTitle = styled(Typography)(({ theme }) => ({
  fontWeight: 500,
  marginBottom: theme.spacing(1),
  fontSize: '1.25rem',
  color: theme.palette.text.primary,
}));

/**
 * Styled Typography component for form subtitle
 */
const FormSubtitle = styled(Typography)(({ theme }) => ({
  color: theme.palette.text.secondary,
  marginBottom: theme.spacing(2),
  fontSize: '0.875rem',
}));

/**
 * A reusable Form component that provides consistent form handling,
 * validation, and submission capabilities for the self-healing data pipeline application.
 * Extends Formik with custom styling and additional functionality.
 * 
 * @template T The type of form values
 * @param props - Component props
 * @returns Form component with configured Formik integration
 */
function Form<T extends object>({
  initialValues,
  validationSchema,
  onSubmit,
  children,
  title,
  subtitle,
  enableReinitialize = false,
  validateOnChange = true,
  validateOnBlur = true,
  validateOnMount = false,
  elevation = 1,
  variant = 'elevation',
  padding = 3,
  width = '100%',
  maxWidth = 'none',
  className,
}: FormProps<T>): React.ReactElement {
  // Generate a unique ID for the form title if present
  const formTitleId = React.useMemo(() => 
    title ? `form-title-${Math.random().toString(36).substring(2, 9)}` : undefined, 
    [title]
  );
  
  return (
    <StyledFormContainer
      elevation={elevation}
      variant={variant}
      width={width}
      maxWidth={maxWidth}
      className={className}
      aria-labelledby={formTitleId}
    >
      <Box padding={padding}>
        {title && <FormTitle id={formTitleId} variant="h5">{title}</FormTitle>}
        {subtitle && <FormSubtitle variant="subtitle1">{subtitle}</FormSubtitle>}
        
        <Formik
          initialValues={initialValues}
          validationSchema={validationSchema}
          onSubmit={onSubmit}
          enableReinitialize={enableReinitialize}
          validateOnChange={validateOnChange}
          validateOnBlur={validateOnBlur}
          validateOnMount={validateOnMount}
        >
          {(formik) => (
            <StyledForm noValidate>
              {typeof children === 'function' ? children(formik) : children}
            </StyledForm>
          )}
        </Formik>
      </Box>
    </StyledFormContainer>
  );
}

export default Form;