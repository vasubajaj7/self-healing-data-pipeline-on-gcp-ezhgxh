import React from 'react';
import { Box, Typography, Link, Divider } from '@mui/material';
import { styled } from '@mui/material/styles';
import useMediaQuery from '@mui/material/useMediaQuery';
import { useThemeContext } from '../../contexts/ThemeContext';
import { API_VERSION } from '../../config/constants';

// Styled components for the footer
const FooterContainer = styled(Box)(({ theme }) => ({
  padding: theme.spacing(3),
  borderTop: `1px solid ${theme.palette.mode === 'light' ? theme.palette.grey[300] : theme.palette.grey[800]}`,
  backgroundColor: theme.palette.mode === 'light' ? theme.palette.grey[100] : theme.palette.background.paper,
  width: '100%',
}));

const FooterContent = styled(Box)(({ theme }) => ({
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  maxWidth: '1200px',
  margin: '0 auto',
  [theme.breakpoints.down('sm')]: {
    flexDirection: 'column',
    alignItems: 'flex-start',
    gap: theme.spacing(2),
  },
}));

const FooterSection = styled(Box)(({ theme }) => ({
  margin: theme.spacing(1),
  display: 'flex',
  flexDirection: 'column',
  [theme.breakpoints.down('sm')]: {
    marginLeft: 0,
    marginRight: 0,
  },
}));

const FooterLink = styled(Link)(({ theme }) => ({
  color: theme.palette.mode === 'light' ? theme.palette.primary.main : theme.palette.primary.light,
  marginRight: theme.spacing(3),
  textDecoration: 'none',
  '&:hover': {
    textDecoration: 'underline',
  },
  '&:last-child': {
    marginRight: 0,
  },
}));

/**
 * Footer component that renders at the bottom of every page
 * @returns Rendered footer component
 */
const Footer = (): JSX.Element => {
  // Get current theme from context
  const { mode } = useThemeContext();
  
  // Check if viewport is mobile
  const isMobile = useMediaQuery((theme: any) => theme.breakpoints.down('sm'));
  
  // Get current year for copyright
  const currentYear = new Date().getFullYear();

  return (
    <FooterContainer>
      <FooterContent>
        <FooterSection>
          <Typography variant="body2" color="textSecondary">
            &copy; {currentYear} Self-Healing Data Pipeline. All rights reserved.
          </Typography>
        </FooterSection>

        {!isMobile && <Divider orientation="vertical" flexItem />}

        <FooterSection sx={{ alignItems: isMobile ? 'flex-start' : 'center' }}>
          <Typography variant="body2" color="textSecondary">
            Version: {API_VERSION}
          </Typography>
        </FooterSection>

        {!isMobile && <Divider orientation="vertical" flexItem />}

        <FooterSection sx={{ 
          flexDirection: isMobile ? 'column' : 'row', 
          alignItems: isMobile ? 'flex-start' : 'center' 
        }}>
          <FooterLink href="/documentation" aria-label="Documentation">
            Documentation
          </FooterLink>
          <FooterLink href="/support" aria-label="Support">
            Support
          </FooterLink>
          <FooterLink href="/privacy-policy" aria-label="Privacy Policy">
            Privacy Policy
          </FooterLink>
        </FooterSection>
      </FooterContent>
    </FooterContainer>
  );
};

export default Footer;