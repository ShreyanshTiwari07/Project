const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  underscore: '\x1b[4m',
  blink: '\x1b[5m',
  reverse: '\x1b[7m',
  hidden: '\x1b[8m',
  fgBlack: '\x1b[30m',
  fgRed: '\x1b[31m',
  fgGreen: '\x1b[32m',
  fgYellow: '\x1b[33m',
  fgBlue: '\x1b[34m',
  fgMagenta: '\x1b[35m',
  fgCyan: '\x1b[36m',
  fgWhite: '\x1b[37m',
  bgBlack: '\x1b[40m',
  bgRed: '\x1b[41m',
  bgGreen: '\x1b[42m',
  bgYellow: '\x1b[43m',
  bgBlue: '\x1b[44m',
  bgMagenta: '\x1b[45m',
  bgCyan: '\x1b[46m',
  bgWhite: '\x1b[47m'
};

let debug = false;

/**
 * Sets the debug flag.
 * @param {boolean} value - The value to set for the debug flag.
 */
function setDebugFlag(value) {
  debug = value;
}

/**
 * Prints logs with color-coded output based on the log level and debug flag.
 * @param {string} message - The log message to print.
 * @param {string} level - The log level (debug, info, warn, error).
 */
function log(message, level = 'info') {
  const timestamp = new Date().toISOString();
  const color = getColor(level);
  const logMessage = `[${timestamp}] ${color}[${level.toUpperCase()}]${colors.reset} ${message}`;

  if (level === 'debug' && !debug) {
    // Skip debug logs if the debug flag is false
    return;
  }

  switch (level) {
    case 'debug':
      console.debug(logMessage);
      break;
    case 'info':
      console.log(logMessage);
      break;
    case 'warn':
      console.warn(logMessage);
      break;
    case 'error':
      console.error(logMessage);
      break;
    default:
      console.log(logMessage);
      break;
  }
}

/**
 * Returns the color code based on the log level.
 * @param {string} level - The log level.
 * @returns {string} - The color code.
 */
function getColor(level) {
  switch (level) {
    case 'debug':
      return colors.fgCyan;
    case 'info':
      return colors.fgGreen;
    case 'warn':
      return colors.fgYellow;
    case 'error':
      return colors.fgRed;
    default:
      return colors.fgWhite;
  }
}

module.exports = {
  setDebugFlag,
  log,
};
