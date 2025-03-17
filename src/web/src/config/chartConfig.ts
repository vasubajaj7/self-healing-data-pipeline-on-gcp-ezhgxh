/**
 * Chart Configuration
 * 
 * Configuration settings for charts used throughout the self-healing data pipeline web interface.
 * Provides default chart options, color schemes, animation settings, and utility functions
 * for consistent chart rendering across the application.
 */

import { ChartOptions } from 'chart.js'; // version: ^4.3.0
import merge from 'lodash/merge'; // version: ^4.17.21
import { chart as chartColors, status as statusColors } from '../theme/colors';
import { CHART_ANIMATION_ENABLED } from './constants';

/**
 * Default chart configurations for different chart types
 */
export const chartDefaults = {
  // Line chart defaults
  line: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    scales: {
      x: {
        grid: {
          display: true,
          drawBorder: true,
          drawOnChartArea: true,
          drawTicks: true,
          color: 'rgba(0, 0, 0, 0.1)'
        },
        ticks: {
          maxRotation: 0
        }
      },
      y: {
        beginAtZero: true,
        grid: {
          color: 'rgba(0, 0, 0, 0.1)'
        }
      }
    },
    elements: {
      line: {
        tension: 0.4,
        borderWidth: 2,
        fill: false
      },
      point: {
        radius: 3,
        hitRadius: 10,
        hoverRadius: 5
      }
    }
  },
  
  // Bar chart defaults
  bar: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    scales: {
      x: {
        grid: {
          display: false
        }
      },
      y: {
        beginAtZero: true,
        grid: {
          color: 'rgba(0, 0, 0, 0.1)'
        }
      }
    },
    borderWidth: 1,
    borderRadius: 4,
    barPercentage: 0.7,
    categoryPercentage: 0.8
  },
  
  // Pie chart defaults
  pie: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    cutout: 0,
    borderWidth: 1,
    borderColor: 'white'
  },
  
  // Doughnut chart defaults
  doughnut: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    cutout: '60%',
    borderWidth: 1,
    borderColor: 'white'
  },
  
  // Area chart (special case of line chart with fill)
  area: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    scales: {
      x: {
        grid: {
          display: true,
          drawBorder: true,
          drawOnChartArea: true,
          drawTicks: true,
          color: 'rgba(0, 0, 0, 0.1)'
        },
        ticks: {
          maxRotation: 0
        }
      },
      y: {
        beginAtZero: true,
        grid: {
          color: 'rgba(0, 0, 0, 0.1)'
        }
      }
    },
    elements: {
      line: {
        tension: 0.4,
        borderWidth: 2,
        fill: true
      },
      point: {
        radius: 3,
        hitRadius: 10,
        hoverRadius: 5
      }
    }
  },
  
  // Scatter chart defaults
  scatter: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    scales: {
      x: {
        grid: {
          display: true,
          color: 'rgba(0, 0, 0, 0.1)'
        }
      },
      y: {
        grid: {
          color: 'rgba(0, 0, 0, 0.1)'
        }
      }
    },
    elements: {
      point: {
        radius: 5,
        hoverRadius: 7,
        hitRadius: 10
      }
    }
  },
  
  // Radar chart defaults
  radar: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    scales: {
      r: {
        beginAtZero: true,
        angleLines: {
          display: true,
          color: 'rgba(0, 0, 0, 0.1)'
        },
        grid: {
          color: 'rgba(0, 0, 0, 0.1)'
        }
      }
    },
    elements: {
      line: {
        tension: 0.1,
        borderWidth: 2,
        fill: true
      },
      point: {
        radius: 3,
        hitRadius: 10,
        hoverRadius: 5
      }
    }
  },
  
  // Bubble chart defaults
  bubble: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
        font: {
          size: 14,
          weight: 'bold'
        }
      }
    },
    scales: {
      x: {
        grid: {
          display: true,
          color: 'rgba(0, 0, 0, 0.1)'
        }
      },
      y: {
        grid: {
          color: 'rgba(0, 0, 0, 0.1)'
        }
      }
    },
    elements: {
      point: {
        hoverRadius: 7
      }
    }
  }
};

/**
 * Color schemes for different chart contexts
 */
export const chartColorSchemes = {
  // Pipeline-related charts (status, execution times, etc.)
  pipeline: [
    chartColors.blue,
    chartColors.teal,
    chartColors.cyan,
    chartColors.indigo,
    chartColors.purple
  ],
  
  // Data quality related charts
  quality: [
    chartColors.green,
    chartColors.lime,
    chartColors.teal,
    chartColors.cyan,
    chartColors.blue
  ],
  
  // Self-healing related charts
  healing: [
    chartColors.purple,
    chartColors.indigo,
    chartColors.blue,
    chartColors.cyan,
    chartColors.teal
  ],
  
  // Alert and monitoring related charts
  alert: [
    chartColors.red,
    chartColors.orange,
    chartColors.amber,
    chartColors.lime,
    chartColors.green
  ],
  
  // Status-specific colors
  status: {
    healthy: statusColors.healthy,
    warning: statusColors.warning,
    error: statusColors.error,
    inactive: statusColors.inactive,
    processing: statusColors.processing
  }
};

/**
 * Animation configurations for different chart types
 */
export const chartAnimations = {
  // Line chart animations
  line: {
    tension: {
      duration: 1000,
      easing: 'easeOutQuart',
      from: 0,
      to: 0.4,
      loop: false
    }
  },
  
  // Bar chart animations
  bar: {
    numbers: {
      type: 'number',
      properties: ['y'],
      from: 0,
      to: 1,
      duration: 800,
      easing: 'easeOutQuad'
    }
  },
  
  // Pie chart animations
  pie: {
    animateRotate: true,
    animateScale: true,
    duration: 800,
    easing: 'easeOutQuad'
  },
  
  // Doughnut chart animations
  doughnut: {
    animateRotate: true,
    animateScale: true,
    duration: 800,
    easing: 'easeOutQuad'
  }
};

/**
 * Responsive options for different screen sizes
 */
export const responsiveOptions = {
  defaults: {
    responsive: true,
    maintainAspectRatio: false
  },
  
  breakpoints: {
    small: {
      maxWidth: 600,
      options: {
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              boxWidth: 10,
              font: {
                size: 10
              }
            }
          }
        },
        scales: {
          x: {
            ticks: {
              maxRotation: 45,
              font: {
                size: 10
              }
            }
          },
          y: {
            ticks: {
              font: {
                size: 10
              }
            }
          }
        }
      }
    },
    medium: {
      maxWidth: 1024,
      options: {
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              boxWidth: 12,
              font: {
                size: 12
              }
            }
          }
        }
      }
    },
    large: {
      minWidth: 1025,
      options: {
        plugins: {
          legend: {
            position: 'right',
            labels: {
              boxWidth: 15,
              font: {
                size: 14
              }
            }
          }
        }
      }
    }
  }
};

/**
 * Default tooltip configuration
 */
export const tooltipDefaults = {
  enabled: true,
  mode: 'index',
  intersect: false,
  position: 'nearest',
  backgroundColor: 'rgba(0, 0, 0, 0.8)',
  titleColor: '#ffffff',
  bodyColor: '#ffffff',
  padding: {
    top: 10,
    right: 15,
    bottom: 10,
    left: 15
  }
};

/**
 * Default legend configuration
 */
export const legendDefaults = {
  display: true,
  position: 'top',
  align: 'center',
  labels: {
    boxWidth: 15,
    padding: 15,
    usePointStyle: true
  }
};

/**
 * Get chart configuration with defaults and custom options merged
 * 
 * @param chartType - Type of chart (line, bar, pie, etc.)
 * @param customOptions - Custom options to merge with defaults
 * @returns Merged chart configuration
 */
export const getChartConfig = (chartType: string, customOptions?: Record<string, any>): ChartOptions => {
  // Get default config for the specified chart type, fallback to empty object
  const baseConfig = chartDefaults[chartType as keyof typeof chartDefaults] || {};
  
  // Apply common plugins like tooltips and legend
  const configWithPlugins = merge({}, baseConfig, {
    plugins: {
      tooltip: tooltipDefaults,
      legend: legendDefaults
    }
  });
  
  // Apply responsive options
  const responsiveConfig = merge({}, configWithPlugins, getResponsiveOptions(chartType));
  
  // Apply animation settings
  const animatedConfig = merge({}, responsiveConfig, {
    animation: getChartAnimation(chartType, CHART_ANIMATION_ENABLED)
  });
  
  // Merge with custom options if provided
  return customOptions ? merge({}, animatedConfig, customOptions) : animatedConfig;
};

/**
 * Get color scheme for a specific chart context
 * 
 * @param context - Chart context (pipeline, quality, healing, alert)
 * @param count - Number of colors needed
 * @returns Array of color hex codes
 */
export const getChartColorScheme = (context: string, count: number): string[] => {
  // Get the base color scheme for the specified context
  const baseScheme = chartColorSchemes[context as keyof typeof chartColorSchemes] || chartColorSchemes.pipeline;
  
  // If requested count exceeds available colors, generate additional colors
  if (count <= baseScheme.length) {
    return baseScheme.slice(0, count);
  }
  
  // Generate additional colors by cycling through the base scheme
  const result: string[] = [...baseScheme];
  const remaining = count - baseScheme.length;
  
  for (let i = 0; i < remaining; i++) {
    result.push(baseScheme[i % baseScheme.length]);
  }
  
  return result;
};

/**
 * Get animation configuration for charts
 * 
 * @param chartType - Type of chart (line, bar, pie, etc.)
 * @param enabled - Whether animations should be enabled
 * @returns Animation configuration
 */
export const getChartAnimation = (chartType: string, enabled: boolean = CHART_ANIMATION_ENABLED): object => {
  if (!enabled) {
    return { duration: 0 }; // Disable animations
  }
  
  // Get animation config for the specified chart type, fallback to standard duration
  return chartAnimations[chartType as keyof typeof chartAnimations] || { duration: 800 };
};

/**
 * Get responsive configuration options for charts
 * 
 * @param chartType - Type of chart
 * @returns Responsive configuration
 */
export const getResponsiveOptions = (chartType: string): object => {
  // Start with default responsive options
  const baseOptions = responsiveOptions.defaults;
  
  // Add chart-type specific responsive behavior
  const chartTypeOptions = {};
  
  // Apply special settings for pie and doughnut charts
  if (chartType === 'pie' || chartType === 'doughnut') {
    Object.assign(chartTypeOptions, {
      plugins: {
        legend: {
          position: 'bottom'
        }
      }
    });
  }
  
  // Build responsive configuration
  return merge({}, baseOptions, chartTypeOptions, {
    responsive: true,
    maintainAspectRatio: false
  });
};