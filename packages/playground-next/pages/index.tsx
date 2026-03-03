import dynamic from "next/dynamic";
import type { ComponentType } from "react";

const FormatDemo = dynamic(
  () => import("../components/FormatDemo"),
  { ssr: false }
) as ComponentType;

export default function Home() {
  return <FormatDemo />;
}
