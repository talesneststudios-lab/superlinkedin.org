/**
 * Build guide/SuperLinkedIn-User-Guide.pdf from guide/user-guide.html.
 *
 * Uses the locally installed Google Chrome / Chromium via Playwright (no browser download).
 * Windows: ensure Chrome is installed, or set PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH.
 */
const path = require('path');
const fs = require('fs');
const { pathToFileURL } = require('url');

let chromium;
try {
    ({ chromium } = require('playwright-core'));
} catch (e) {
    console.error('Missing playwright-core. Run: npm install');
    process.exit(1);
}

(async () => {
    const guideDir = path.join(__dirname, '..', 'guide');
    const htmlPath = path.join(guideDir, 'user-guide.html');
    const outPdf = path.join(guideDir, 'SuperLinkedIn-User-Guide.pdf');

    if (!fs.existsSync(htmlPath)) {
        console.error('Missing file:', htmlPath);
        process.exit(1);
    }

    const launchOpts = {};
    if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
        launchOpts.executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
    } else {
        launchOpts.channel = 'chrome';
    }

    let browser;
    try {
        browser = await chromium.launch(launchOpts);
    } catch (e1) {
        try {
            browser = await chromium.launch({ channel: 'msedge' });
        } catch (e2) {
            console.error('Could not launch Chrome or Edge for PDF export.');
            console.error('Install Google Chrome, or run: npx playwright install chromium');
            console.error('Or set PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH to your browser exe.');
            process.exit(1);
        }
    }

    const page = await browser.newPage();
    await page.goto(pathToFileURL(htmlPath).href, { waitUntil: 'load' });
    await page.pdf({
        path: outPdf,
        format: 'A4',
        printBackground: true,
        margin: { top: '14mm', bottom: '14mm', left: '12mm', right: '12mm' },
    });
    await browser.close();
    console.log('Wrote PDF:', outPdf);
})();
