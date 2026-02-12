import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DIALECT_DISPLAY_NAMES } from "@/lib/constants";

interface DialectSelectProps {
  value: string;
  onValueChange: (value: string) => void;
  dialects: string[];
  label?: string;
}

export function DialectSelect({
  value,
  onValueChange,
  dialects,
  label,
}: DialectSelectProps) {
  return (
    <div className="flex items-center gap-2">
      {label && (
        <span className="text-sm text-muted-foreground whitespace-nowrap">
          {label}
        </span>
      )}
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger className="w-[200px]">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {[...dialects]
            .sort((a, b) =>
              (DIALECT_DISPLAY_NAMES[a] ?? a).localeCompare(
                DIALECT_DISPLAY_NAMES[b] ?? b
              )
            )
            .map((d) => (
              <SelectItem key={d} value={d}>
                {DIALECT_DISPLAY_NAMES[d] ?? d}
              </SelectItem>
            ))}
        </SelectContent>
      </Select>
    </div>
  );
}
