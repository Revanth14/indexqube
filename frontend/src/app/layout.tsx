import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "IndexQube - Index Calculation Infrastructure",
  description:
    "Financial technology platform for factor-based, volatility control, and defined outcome index calculation.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
