import type { RenderedSeat, SeatMap as SeatMapType } from "../realtime/types";

const STATUS_COLOR: Record<RenderedSeat["status"], string> = {
  AVAILABLE: "#2ecc71",
  HELD: "#f5a623",
  BOOKED: "#7f8c8d",
};

const SEAT_SIZE = 22;
const SEAT_GAP = 6;
const ROW_LABEL_WIDTH = 28;
const SECTION_HEADER_HEIGHT = 24;
const SECTION_GAP = 20;

interface SeatMapProps {
  seats: SeatMapType;
  mySeatIds: ReadonlySet<number>;
  onSeatClick: (seatId: number) => void;
}

interface Positioned {
  seat: RenderedSeat;
  x: number;
  y: number;
}

interface SectionHeader {
  section: string;
  textY: number;
}

interface RowLabelPosition {
  key: string;
  rowLabel: string;
  textY: number;
}

interface Layout {
  positioned: Positioned[];
  sectionHeaders: SectionHeader[];
  rowLabels: RowLabelPosition[];
  width: number;
  height: number;
}

function layout(seats: SeatMapType): Layout {
  const bySection = new Map<string, RenderedSeat[]>();
  for (const seat of seats.values()) {
    const list = bySection.get(seat.section) ?? [];
    list.push(seat);
    bySection.set(seat.section, list);
  }

  const positioned: Positioned[] = [];
  const sectionHeaders: SectionHeader[] = [];
  const rowLabels: RowLabelPosition[] = [];
  let y = 0;
  let maxWidth = 0;

  for (const section of [...bySection.keys()].sort()) {
    const sectionSeats = bySection.get(section)!;
    const byRow = new Map<string, RenderedSeat[]>();
    for (const seat of sectionSeats) {
      const list = byRow.get(seat.rowLabel) ?? [];
      list.push(seat);
      byRow.set(seat.rowLabel, list);
    }
    // Baseline sits within the header's own band, well clear of the
    // first row placed immediately after it.
    sectionHeaders.push({ section, textY: y + SECTION_HEADER_HEIGHT - 8 });
    y += SECTION_HEADER_HEIGHT;
    for (const row of [...byRow.keys()].sort()) {
      const rowSeats = [...byRow.get(row)!].sort((a, b) => a.seatNumber - b.seatNumber);
      rowLabels.push({ key: `${section}:${row}`, rowLabel: row, textY: y + SEAT_SIZE - 6 });
      for (const seat of rowSeats) {
        const x = ROW_LABEL_WIDTH + (seat.seatNumber - 1) * (SEAT_SIZE + SEAT_GAP);
        positioned.push({ seat, x, y });
        maxWidth = Math.max(maxWidth, x + SEAT_SIZE);
      }
      y += SEAT_SIZE + SEAT_GAP;
    }
    y += SECTION_GAP;
  }

  return { positioned, sectionHeaders, rowLabels, width: maxWidth + SEAT_GAP, height: y };
}

/** Real SVG, per SPEC.md section 9: seats change colour live, driven
 * entirely by the seat map passed in -- this component has no state of
 * its own beyond layout geometry.
 */
export function SeatMap({ seats, mySeatIds, onSeatClick }: SeatMapProps) {
  const { positioned, sectionHeaders, rowLabels, width, height } = layout(seats);

  if (positioned.length === 0) {
    return <p>Waiting for seat data…</p>;
  }

  return (
    <svg width={width} height={height} role="img" aria-label="Seat map">
      {sectionHeaders.map(({ section, textY }) => (
        <text key={section} x={0} y={textY} fontSize={14} fontWeight="bold">
          Section {section}
        </text>
      ))}
      {rowLabels.map(({ key, rowLabel, textY }) => (
        <text key={key} x={0} y={textY} fontSize={11}>
          {rowLabel}
        </text>
      ))}
      {positioned.map(({ seat, x, y }) => {
        const mine = mySeatIds.has(seat.id);
        const clickable = seat.status === "AVAILABLE" || mine;
        return (
          <rect
            key={seat.id}
            x={x}
            y={y}
            width={SEAT_SIZE}
            height={SEAT_SIZE}
            rx={4}
            fill={STATUS_COLOR[seat.status]}
            stroke={mine ? "#2c3e50" : "none"}
            strokeWidth={mine ? 3 : 0}
            style={{ cursor: clickable ? "pointer" : "default" }}
            onClick={clickable ? () => onSeatClick(seat.id) : undefined}
          >
            <title>
              {seat.section}
              {seat.rowLabel}-{seat.seatNumber}: {seat.status}
            </title>
          </rect>
        );
      })}
    </svg>
  );
}
