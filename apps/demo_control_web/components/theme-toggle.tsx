"use client";

import {Moon, Sun} from "lucide-react";
import {useEffect, useState} from "react";

export const THEME_STORAGE_KEY = "commerce-console-theme";

/** Runs before paint (inlined in <head>) so a saved dark theme never flashes light. */
export const themeBootScript = `try{if(localStorage.getItem("${THEME_STORAGE_KEY}")==="dark")document.documentElement.classList.add("dark")}catch(e){}`;

export function ThemeToggle() {
  const [dark, setDark] = useState(false);

  useEffect(() => {
    setDark(document.documentElement.classList.contains("dark"));
  }, []);

  function toggle() {
    const next = !dark;
    document.documentElement.classList.toggle("dark", next);
    try {
      localStorage.setItem(THEME_STORAGE_KEY, next ? "dark" : "light");
    } catch {
      // Storage can be unavailable (private mode); the toggle still works for this page.
    }
    setDark(next);
  }

  return (
    <button
      type="button"
      onClick={toggle}
      aria-pressed={dark}
      className="inline-flex items-center gap-2 rounded-[var(--radius-control)] px-3 py-2 text-sm font-medium text-muted transition-colors hover:bg-raised hover:text-ink [&_svg]:size-4"
    >
      {dark ? <Sun aria-hidden="true" /> : <Moon aria-hidden="true" />}
      {dark ? "Light theme" : "Dark theme"}
    </button>
  );
}
