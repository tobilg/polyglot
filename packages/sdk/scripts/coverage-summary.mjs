import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const summaryPath = resolve(process.argv[2] ?? 'coverage/coverage-summary.json');
const summary = JSON.parse(readFileSync(summaryPath, 'utf8'));
const total = summary.total;

console.log('## TypeScript SDK coverage');
console.log('');
console.log('| Metric | Covered | Total | Percent |');
console.log('| --- | ---: | ---: | ---: |');
for (const metric of ['statements', 'branches', 'functions', 'lines']) {
  const values = total[metric];
  console.log(
    `| ${metric[0].toUpperCase()}${metric.slice(1)} | ${values.covered} | ${values.total} | ${values.pct}% |`,
  );
}
