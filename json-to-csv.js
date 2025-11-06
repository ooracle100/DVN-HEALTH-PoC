// json-array-to-csv-stream.js
// Usage: node json-array-to-csv-stream.js <input.json> <output.csv>

const fs = require('fs');
const path = require('path');

function formatForExcel(value) {
  if (value === null || value === undefined) return '';

  // If it's an array, join with semicolons so Excel keeps it in one cell.
  if (Array.isArray(value)) {
    const joined = value.map(v => {
      // preserve ints and hex strings as-is
      return String(v);
    }).join(';');
    // Excel-friendly formula-style string so Excel won't convert or split.
    return `="[ ${joined} ]"`;
  }

  if (typeof value === 'object') {
    // For nested objects, keep JSON string compact (no newlines) inside wrapper
    const j = JSON.stringify(value);
    // If you prefer dot-notation flattening, replace this with a flattener.
    return `="${j}"`;
  }

  if (typeof value === 'boolean') return String(value).toLowerCase();

  // For numbers or strings, wrap to avoid Excel auto-formatting (scientific notation)
  return `="${String(value)}"`;
}

/**
 * Build ordered union of keys from all objects. Keep order from first item,
 * then append keys found later that aren't present yet.
 */
function buildHeaders(arr) {
  if (!Array.isArray(arr) || arr.length === 0) return [];
  const seen = new Set();
  const headers = [];

  // Start with keys in first object in their order
  const firstKeys = Object.keys(arr[0]);
  for (const k of firstKeys) {
    seen.add(k);
    headers.push(k);
  }

  // Then scan remaining objects and append unseen keys in the order encountered
  for (let i = 1; i < arr.length; i++) {
    const obj = arr[i];
    if (obj && typeof obj === 'object') {
      for (const k of Object.keys(obj)) {
        if (!seen.has(k)) {
          seen.add(k);
          headers.push(k);
        }
      }
    }
  }
  return headers;
}

/**
 * Write CSV using a write stream to support large files.
 * This writes values WITHOUT additional CSV quoting so Excel-friendly formulas stay formulas.
 * (Because we use wrappers like ="...".)
 */
function writeCsvStreamed(arr, outPath) {
  if (!Array.isArray(arr) || arr.length === 0) {
    throw new Error('Input must be a non-empty array of objects.');
  }

  const headers = buildHeaders(arr);
  const ws = fs.createWriteStream(outPath, { encoding: 'utf8' });

  // header line
  ws.write(headers.join(',') + '\n');

  for (const obj of arr) {
    // ensure a consistent order of columns
    const row = headers.map(h => {
      const val = obj && Object.prototype.hasOwnProperty.call(obj, h) ? obj[h] : null;
      return formatForExcel(val);
    });
    ws.write(row.join(',') + '\n');
  }

  ws.end();
}

// CLI runner
if (require.main === module) {
  const argv = process.argv.slice(2);
  if (argv.length < 2) {
    console.error('Usage: node json-array-to-csv-stream.js <input.json> <output.csv>');
    process.exit(2);
  }
  const [inputPath, outputPath] = argv.map(p => path.resolve(p));
  const raw = fs.readFileSync(inputPath, 'utf8');
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    console.error('Error parsing JSON:', e.message);
    process.exit(2);
  }

  // normalize to array
  const arr = Array.isArray(parsed) ? parsed : [parsed];

  try {
    writeCsvStreamed(arr, outputPath);
    console.log('Wrote CSV to', outputPath);
  } catch (err) {
    console.error('Error writing CSV:', err);
    process.exit(2);
  }
}

module.exports = { writeCsvStreamed, formatForExcel, buildHeaders };
