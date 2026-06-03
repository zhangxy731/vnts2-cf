// 此文件由 gen-version.js 自动生成，请勿手动修改
import { execSync } from "child_process";
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// 获取日期（YYYY-MM-DD）
const now = new Date();
const y = now.getFullYear();
const m = String(now.getMonth() + 1).padStart(2, "0");
const d = String(now.getDate()).padStart(2, "0");
const dateStr = `${y}-${m}-${d}`;

// 尝试获取 git 短哈希
let hash = "";
try {
  hash = execSync("git rev-parse --short HEAD", {
    encoding: "utf-8",
    cwd: join(__dirname, ".."),
    timeout: 3000,
  }).trim();
} catch {
  // git 不可用，使用备用标记
  hash = "nogit";
}

const version = hash ? `${dateStr}_${hash}` : dateStr;

const content = `// 自动生成，请勿手动修改
export const SERVER_VERSION = ${JSON.stringify(version)};
`;

const outPath = join(__dirname, "..", "src", "version.js");
writeFileSync(outPath, content, "utf-8");
console.log(`[gen-version] SERVER_VERSION = ${version}`);
