import "./globals.css";
import {AppShell} from "../components/app-shell";
export const metadata={title:"Commerce Control Center",description:"Local event platform operations console"};
export default function Layout({children}:{children:React.ReactNode}){return <html lang="en"><body><AppShell>{children}</AppShell></body></html>}
