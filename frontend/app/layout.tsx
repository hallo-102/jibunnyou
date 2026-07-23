import type { Metadata } from "next";
import "./globals.css";

const themeInitializationScript = `
(() => {
  try {
    const stored = localStorage.getItem("keiba-ai-studio-theme");
    const preference = ["light", "dark", "system"].includes(stored || "") ? stored : "system";
    const resolved = preference === "system"
      ? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light")
      : preference;
    document.documentElement.dataset.theme = resolved;
  } catch {
    document.documentElement.dataset.theme = "light";
  }
})();
`;

export const metadata: Metadata = {
  title: "Keiba AI Studio",
  description: "Race data operations dashboard"
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ja" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeInitializationScript }} />
      </head>
      <body>{children}</body>
    </html>
  );
}
