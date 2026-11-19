// Optional browser acceptance check. Requires Playwright and a Chromium installation.
const { chromium } = require("playwright");
const { spawn } = require("node:child_process");
const crypto = require("node:crypto");
const fs = require("node:fs");
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const { once } = require("node:events");
const assert = require("node:assert/strict");

async function main() {
  const socket = net.createServer();
  socket.listen(0, "127.0.0.1");
  await once(socket, "listening");
  const port = socket.address().port;
  await new Promise(resolve => socket.close(resolve));
  const state = fs.mkdtempSync(path.join(os.tmpdir(), "video-lab-ui-"));
  const secret = () => crypto.randomBytes(32).toString("hex");
  const admin = secret();
  const server = spawn(process.env.LAB_PYTHON || "python", [
    "coordinator.py", "--port", String(port), "--db", path.join(state, "test.sqlite")
  ], { cwd: path.resolve(__dirname, ".."), windowsHide: true, env: {
    ...process.env, LAB_ADMIN_TOKEN: admin, LAB_OPERATOR_TOKEN: secret(),
    LAB_VIEWER_TOKEN: secret(), LAB_WORKER_TOKENS: JSON.stringify({w: secret()})
  } });
  const stopped = once(server, "exit");
  let browser;
  try {
    const origin = "http://127.0.0.1:" + port;
    let ready = false;
    for (let i = 0; i < 80 && !ready; i++) {
      try { ready = (await fetch(origin)).ok; } catch {}
      if (!ready) await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.ok(ready, "coordinator did not become ready");
    browser = await chromium.launch({ headless: true, executablePath: process.env.LAB_CHROMIUM });
    const page = await browser.newPage();
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.goto(origin);
    await page.locator("#session-button").click();
    await page.locator("input[name=token]").fill(admin);
    await page.locator("#auth-form button.primary").click();
    await page.locator("#new-job").click();
    await page.locator("[name=name]").fill("Проверка интерфейса");
    await page.locator("[name=range]").fill("[1,3]");
    await page.locator("#job-form button.primary").click();
    await page.locator("#job-title").filter({ hasText: "Проверка интерфейса" }).waitFor();
    await page.locator("[data-tab=workers]").click();
    await page.locator("#workers-view").waitFor();
    await page.locator("[data-tab=audit]").click();
    await page.locator("#audit-list").getByText("create", { exact: true }).waitFor();
    await page.setViewportSize({ width: 390, height: 844 });
    await page.locator("[data-tab=jobs]").click();
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    assert.deepEqual(errors, []);
    console.log("Passed login, job creation, workers, audit and mobile layout.");
  } finally {
    if (browser) await browser.close();
    server.kill();
    await stopped;
    if (path.dirname(state) === os.tmpdir() && path.basename(state).startsWith("video-lab-ui-")) {
      fs.rmSync(state, { recursive: true });
    }
  }
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
