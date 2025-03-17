# Self-Healing Data Pipeline Web Interface

## Overview

This directory contains the web frontend for the Self-Healing Data Pipeline project. The web interface provides comprehensive dashboards and management tools for monitoring, configuring, and administering the data pipeline system.

## Technology Stack

- **Framework**: React 18 with TypeScript
- **Build Tool**: Vite
- **State Management**: React Context API
- **Styling**: Styled Components
- **Testing**: Jest and React Testing Library
- **API Communication**: Axios
- **Real-time Updates**: WebSocket
- **Visualization**: D3.js and React-based chart components
- **Internationalization**: i18next

## Project Structure

```
src/
├── assets/         # Static assets like images and icons
├── components/     # Reusable UI components
│   ├── common/     # Generic UI components
│   ├── charts/     # Data visualization components
│   ├── layout/     # Layout components
│   ├── dashboard/  # Dashboard-specific components
│   ├── quality/    # Data quality components
│   ├── pipeline/   # Pipeline management components
│   ├── alert/      # Alert management components
│   ├── selfHealing/# Self-healing configuration components
│   └── config/     # System configuration components
├── contexts/       # React context providers
├── hooks/          # Custom React hooks
├── pages/          # Page components
├── routes/         # Routing configuration
├── services/       # API and service integrations
├── theme/          # Theme configuration
├── types/          # TypeScript type definitions
├── utils/          # Utility functions
└── locales/        # Internationalization resources
```

## Getting Started

### Prerequisites

- Node.js 16.x or higher
- npm 8.x or higher

### Installation

```bash
# Install dependencies
npm install
```

### Development

```bash
# Start development server
npm run dev

# Run tests
npm test

# Run linting
npm run lint

# Run type checking
npm run type-check
```

### Building for Production

```bash
# Build production bundle
npm run build

# Preview production build
npm run preview
```

## Environment Configuration

The application uses environment variables for configuration. Create the following files for different environments:

- `.env.development` - Development environment
- `.env.test` - Testing environment
- `.env.production` - Production environment

Example configuration:

```
VITE_API_BASE_URL=http://localhost:8000/api
VITE_WS_URL=ws://localhost:8000/ws
VITE_AUTH_ENABLED=true
```

## Key Features

### Dashboard

The main dashboard provides a comprehensive overview of the pipeline health, data quality metrics, self-healing activities, and active alerts. It includes real-time updates and interactive visualizations.

### Pipeline Management

Manage and monitor data pipelines with detailed execution history, DAG visualization, and control capabilities.

### Data Quality

View data quality metrics, validation results, and quality trends across datasets. Configure validation rules and thresholds.

### Self-Healing

Configure self-healing rules, monitor healing activities, and manage AI models used for automated correction.

### Alerting

View and manage alerts, configure notification channels, and set up alert rules.

### Configuration

Manage system configuration, data sources, and integration settings.

## Accessibility

The interface is designed with accessibility in mind, following WCAG 2.1 guidelines. Key considerations include:

- Sufficient color contrast (minimum 4.5:1 ratio)
- Keyboard navigation support
- Screen reader compatibility with ARIA labels
- Resizable text without layout breaking
- Multiple indicators for critical alerts (color, icon, text)

## Browser Compatibility

The application supports the following browsers:

- Chrome (latest 2 versions)
- Firefox (latest 2 versions)
- Safari (latest 2 versions)
- Edge (latest 2 versions)

## Contributing

Please refer to the main project's CONTRIBUTING.md file for guidelines on contributing to this project.

## License

This project is licensed under the terms specified in the LICENSE file in the root directory.