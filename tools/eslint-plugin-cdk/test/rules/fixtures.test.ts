import { ESLint } from 'eslint';
import * as fs from 'fs-extra';
import * as path from 'path';

const rulesDirPlugin = require('eslint-plugin-rulesdir');
rulesDirPlugin.RULES_DIR = path.join(__dirname, '../../lib/rules');

let linter: ESLint;

const outputRoot = path.join(process.cwd(), '.test-output');
fs.mkdirpSync(outputRoot);

const fixturesRoot = path.join(__dirname, 'fixtures');

fs.readdirSync(fixturesRoot).filter(f => fs.lstatSync(path.join(fixturesRoot, f)).isDirectory()).forEach(d => {
  describe(d, () => {
    const fixturesDir = path.join(fixturesRoot, d);

    beforeAll(() => {
      linter = new ESLint({
        baseConfig: {
          parser: '@typescript-eslint/parser',
        },
        overrideConfigFile: path.join(fixturesDir, 'eslintrc.js'),
        rulePaths: [
          path.join(__dirname, '../../lib/rules'),
        ],
        fix: true,
      });
    });

    const outputDir = path.join(outputRoot, d);
    fs.mkdirpSync(outputDir);

    const fixtureFiles = fs.readdirSync(fixturesDir).filter(f => f.endsWith('.ts') && !f.endsWith('.expected.ts'));

    fixtureFiles.forEach(f => {
      test(f, async (done) => {
        const originalFilePath = path.join(fixturesDir, f);
        const expectedFixedFilePath = path.join(fixturesDir, `${path.basename(f, '.ts')}.expected.ts`);
        const expectedErrorFilepath = path.join(fixturesDir, `${path.basename(f, '.ts')}.error.txt`);
        const fix = fs.existsSync(expectedFixedFilePath);
        const checkErrors = fs.existsSync(expectedErrorFilepath);
        if (fix && checkErrors) {
          done.fail(`Expected only a fixed file or an expected error. Both ${expectedFixedFilePath} and ${expectedErrorFilepath} are present.`);
          return;
        } else if (fix) {
          const actualFile = await lintAndFix(originalFilePath, outputDir);
          const actual = await fs.readFile(actualFile, { encoding: 'utf8' });
          const expected = await fs.readFile(expectedFixedFilePath, { encoding: 'utf8' });
          if (actual !== expected) {
            done.fail(`Linted file did not match expectations. Expected: ${expectedFixedFilePath}. Actual: ${actualFile}`);
            return;
          }
          done();
          return;
        } else if (checkErrors) {
          const result = await lintAndGetErrorCountAndMessage(originalFilePath)
          const expectedErrorMessage = await fs.readFile(expectedErrorFilepath, { encoding: 'utf8' });
          if (result.errorMessage !== expectedErrorMessage) {
            done.fail(`Error mesage from linter did not match expectations. Linted file: ${path.join(fixturesDir, f)}. \nExpected error message: ${expectedErrorMessage} \nActual error message: ${result.errorMessage}`);
            return;
          }
          done();
          return;
        } else {
          done.fail(`Expected fixed file or expected error file not found.`);
        }
        done();
      });
    });
  });
});

async function lintAndFix(file: string, outputDir: string) {
  const newPath = path.join(outputDir, path.basename(file))
  let result = await linter.lintFiles(file);
  const hasFixes = result.find(r => typeof(r.output) === 'string') !== undefined;
  if (hasFixes) {
    await ESLint.outputFixes(result.map(r => {
      r.filePath = newPath;
      return r;
    }));
  } else {
    // If there are no fixes, copy the input file as output
    await fs.copyFile(file, newPath);
  }
  return newPath;
}

async function lintAndGetErrorCountAndMessage(file: string) {
  const result = await linter.lintFiles(file);
  let errorCount = 0;
  let errorMessage: string | undefined = undefined;
  if (result.length === 1) {
    errorCount = result[0].errorCount;
    if (result[0].messages.length === 1) {
      errorMessage = result[0].messages[0].message;
    }
  };
  return {
    errorCount,
    errorMessage,
  };
}
