const path = require("path");

/** @type {import('next').NextConfig} */
const nextConfig = {
  turbopack: {
    root: path.join(__dirname, "../.."),
  },
  experimental: {
    turbopackFileSystemCacheForDev: true,
    turbopackFileSystemCacheForBuild: true,
  },
  transpilePackages: ["@polyglot-sql/sdk", "playground-shared"],
};

module.exports = nextConfig;
