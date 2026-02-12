import { Alert, AlertDescription } from "@/components/ui/alert";
import { AlertCircle } from "lucide-react";

interface ErrorDisplayProps {
  error: string | null;
}

export function ErrorDisplay({ error }: ErrorDisplayProps) {
  if (!error) return null;

  return (
    <Alert variant="destructive" className="border-red-900/50 bg-red-950/30">
      <AlertCircle className="size-4" />
      <AlertDescription className="font-mono text-sm">{error}</AlertDescription>
    </Alert>
  );
}
