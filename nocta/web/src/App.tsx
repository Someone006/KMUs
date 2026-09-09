import { BrowserRouter, Route, Routes, Link, useLocation } from "react-router-dom";
import { Customer } from "./surfaces/Customer";
import { RundeRoom } from "./surfaces/RundeRoom";
import { Ops } from "./surfaces/Ops";
import { Courier, CourierPicker } from "./surfaces/Courier";

/** Demo switcher. A real deployment ships these as three separate apps. */
function SurfaceSwitcher() {
  const { pathname } = useLocation();
  if (pathname === "/") return null;
  return (
    <nav className="flex items-center justify-center gap-1 border-b border-hairline py-2 text-[12px]">
      {[["/", "Kunde"], ["/ops", "Leitstand"], ["/courier", "Kurier"]].map(([to, label]) => (
        <Link
          key={to}
          to={to}
          className="rounded-lg px-3 py-1.5 font-semibold transition-colors"
          style={{
            color: pathname.startsWith(to) && to !== "/" ? "var(--ink)" : "var(--faint)",
            background: pathname.startsWith(to) && to !== "/" ? "var(--raised)" : "transparent",
          }}
        >
          {label}
        </Link>
      ))}
    </nav>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <SurfaceSwitcher />
      <Routes>
        <Route path="/" element={<Customer />} />
        <Route path="/r/:code" element={<RundeRoom />} />
        <Route path="/ops" element={<Ops />} />
        <Route path="/courier" element={<CourierPicker />} />
        <Route path="/courier/:id" element={<Courier />} />
        <Route path="*" element={<Customer />} />
      </Routes>
    </BrowserRouter>
  );
}
