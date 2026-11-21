import { defineConfig } from "astro/config";
export default defineConfig({
  site: "https://bess-atlas.yeah-cool.io",
  output: "static",
  outDir: "../.astro-build",
  vite: {
    build: {
      rolldownOptions: {
        output: {
          codeSplitting: {
            groups: [
              { name: "charts", test: /node_modules\/echarts/ },
              { name: "chart-renderer", test: /node_modules\/zrender/ },
              {
                name: "map",
                test: /node_modules\/(leaflet|supercluster|kdbush)/,
              },
            ],
          },
        },
      },
    },
  },
});
