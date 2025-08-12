# Synctool Dashboard UI

A modern React-based dashboard for monitoring and managing Synctool sync jobs.

## Features

- **Real-time Monitoring**: Live updates of job statuses and runs
- **Job Management**: View job configurations and strategies
- **Run Details**: Detailed metrics and logs for each sync run
- **Responsive Design**: Works on desktop, tablet, and mobile devices
- **Modern UI**: Built with React, TypeScript, and Tailwind CSS

## Tech Stack

- **React 18** - UI library
- **TypeScript** - Type safety
- **Vite** - Build tool and dev server
- **Tailwind CSS** - Styling framework
- **React Router** - Client-side routing
- **Axios** - HTTP client
- **Lucide React** - Icons
- **Recharts** - Data visualization

## Getting Started

### Prerequisites

- Node.js 16+ 
- npm or yarn
- Synctool API server running on port 8000

### Installation

1. Install dependencies:
```bash
npm install
```

2. Start the development server:
```bash
npm run dev
```

The dashboard will be available at http://localhost:3000

### API Configuration

The dashboard expects the Synctool API to be running at `http://localhost:8000`. If your API is running on a different port or host, update the proxy configuration in `vite.config.ts`:

```typescript
server: {
  port: 3000,
  proxy: {
    '/api': {
      target: 'http://your-api-host:port',
      changeOrigin: true,
    },
  },
},
```

## Development

### Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run preview` - Preview production build
- `npm run lint` - Run ESLint
- `npm run type-check` - Run TypeScript type checking

### Project Structure

```
src/
├── components/          # Reusable UI components
│   ├── ui/             # Basic UI components (Card, Button, etc.)
│   ├── layout/         # Layout components
│   ├── jobs/           # Job-related components
│   └── runs/           # Run-related components
├── pages/              # Page components
├── services/           # API service layer
├── types/              # TypeScript type definitions
├── hooks/              # Custom React hooks
├── utils/              # Utility functions
└── main.tsx           # Application entry point
```

### Key Components

#### Layout
- `Layout` - Main application layout with sidebar navigation

#### Jobs
- `JobsList` - Grid view of all sync jobs
- `JobCard` - Individual job card with status and metrics
- `JobDetailPage` - Detailed view of a specific job

#### Runs
- `RunsTable` - Table view of sync runs with filtering
- `RunDetailPage` - Detailed view of a specific run
- `LogConsole` - Real-time log viewer

#### UI Components
- `StatusBadge` - Color-coded status indicators
- `LoadingSpinner` - Loading states
- `Card` - Consistent card layout

### API Integration

The dashboard integrates with the Synctool API through the `apiService` class:

```typescript
import apiService from '../services/api';

// Get all jobs
const jobs = await apiService.getJobs();

// Get job details
const job = await apiService.getJobDetail(jobName);

// Get runs
const runs = await apiService.getAllRuns({ limit: 20 });
```

### Styling

The dashboard uses Tailwind CSS for styling with a custom design system:

- **Colors**: Primary (blue), Success (green), Warning (yellow), Error (red)
- **Typography**: Inter font family
- **Components**: Consistent spacing and styling patterns

## Production Build

1. Build the application:
```bash
npm run build
```

2. The built files will be in the `dist/` directory

3. Serve the static files using any web server:
```bash
# Using a simple HTTP server
npx serve dist

# Using nginx (example config)
server {
    listen 80;
    root /path/to/dist;
    index index.html;
    
    location / {
        try_files $uri $uri/ /index.html;
    }
    
    location /api/ {
        proxy_pass http://localhost:8000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Features Overview

### Dashboard
- System overview with key metrics
- Recent jobs and runs
- Real-time status updates

### Jobs Management
- View all configured sync jobs
- Job configuration details
- Strategy management
- Real-time status monitoring

### Runs Monitoring
- View all sync runs across jobs
- Filter by status, job, or time period
- Detailed run metrics and statistics
- Real-time log streaming

### Run Details
- Complete run information
- Performance metrics
- Partition success/failure rates
- Live log console with filtering

## Browser Support

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## Contributing

1. Follow the existing code style and patterns
2. Use TypeScript for all new code
3. Add proper error handling
4. Test on different screen sizes
5. Update documentation as needed

## Troubleshooting

### Common Issues

1. **API Connection Errors**
   - Ensure the Synctool API is running
   - Check the proxy configuration in `vite.config.ts`
   - Verify CORS settings on the API server

2. **Build Errors**
   - Run `npm run type-check` to identify TypeScript issues
   - Ensure all dependencies are installed
   - Clear node_modules and reinstall if needed

3. **Styling Issues**
   - Ensure Tailwind CSS is properly configured
   - Check for conflicting CSS classes
   - Verify PostCSS configuration

### Performance Tips

- The dashboard automatically polls for updates every 30 seconds
- Use the refresh buttons for immediate updates
- Large datasets are paginated to improve performance
- Logs are limited to prevent memory issues
