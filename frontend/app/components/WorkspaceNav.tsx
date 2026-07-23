"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export const WORKSPACE_ROUTES = [
  { href: "/#dashboard", pathname: "/", label: "概要" },
  { href: "/races#race-workspace", pathname: "/races", label: "レース・予想" },
  { href: "/analysis#ai-analysis", pathname: "/analysis", label: "AI比較" },
  { href: "/bets#bet-planning", pathname: "/bets", label: "買い目候補" },
  { href: "/performance#performance", pathname: "/performance", label: "成績分析" },
  { href: "/operations#operations", pathname: "/operations", label: "ジョブ・品質" }
] as const;

const ROUTE_BY_ANCHOR: Record<string, string> = Object.fromEntries(
  WORKSPACE_ROUTES.map((route) => [`#${route.href.split("#")[1]}`, route.href])
);

export function workspaceHref(anchor: string) {
  // 既存の画面内anchorを、対応する領域別URLへ変換する。
  return ROUTE_BY_ANCHOR[anchor] || anchor;
}

export default function WorkspaceNav() {
  const pathname = usePathname();

  return (
    <nav className="workspaceNav" aria-label="主要画面">
      {WORKSPACE_ROUTES.map((route) => (
        <Link
          aria-current={pathname === route.pathname ? "page" : undefined}
          href={route.href}
          key={route.pathname}
        >
          {route.label}
        </Link>
      ))}
    </nav>
  );
}
