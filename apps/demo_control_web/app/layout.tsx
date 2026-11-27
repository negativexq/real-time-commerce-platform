import "./globals.css";
import type {Metadata} from "next";
import Script from "next/script";
import type {ReactNode} from "react";
import {AppShell} from "../components/app-shell";
import {themeBootScript} from "../components/theme-toggle";

export const metadata: Metadata = {
  title: "Commerce Control Center",
  description: "Local event platform operations console",
};

export default function Layout({children}: {children: ReactNode}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <Script id="theme-boot" strategy="beforeInteractive">
          {themeBootScript}
        </Script>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
