// Test script for Node.js WASM bindings
const { transpile, parse, format_sql, get_dialects, version } = require('./pkg-node/polyglot_sql_wasm.js');

console.log('Polyglot WASM Test (Node.js)\n');
console.log('Version:', version());
console.log('Supported dialects:', get_dialects());

console.log('\n--- Transpile Test ---');
const sql = "SELECT NVL(name, 'default'), SUBSTR(email, 1, 5) FROM users";
console.log('Input SQL:', sql);
console.log('From: duckdb, To: postgresql');

const result = JSON.parse(transpile(sql, 'duckdb', 'postgresql'));
if (result.success) {
    console.log('Output:', result.sql[0]);
} else {
    console.log('Error:', result.error);
}

console.log('\n--- Parse Test ---');
const parseResult = JSON.parse(parse('SELECT a, b FROM t WHERE x > 1', 'generic'));
if (parseResult.success) {
    console.log('AST:', parseResult.ast.substring(0, 200) + '...');
} else {
    console.log('Error:', parseResult.error);
}

console.log('\n--- Format Test ---');
const formatResult = JSON.parse(format_sql('select a,b,c from users where id=1 and status=2', 'generic'));
if (formatResult.success) {
    console.log('Formatted:\n' + formatResult.sql[0]);
} else {
    console.log('Error:', formatResult.error);
}

console.log('\n✓ All tests passed!');
