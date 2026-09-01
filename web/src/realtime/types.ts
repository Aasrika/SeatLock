// Wire shapes exactly as app/realtime/hub.py sends them (see that
// module's docstring for the two message types). snake_case fields
// mirror the JSON on the wire deliberately, rather than being
// camelCased at the boundary -- one fewer place for a field-name typo
// to silently produce `undefined`.

export type SeatStatus = "AVAILABLE" | "HELD" | "BOOKED";

export interface SnapshotSeat {
  id: number;
  section: string;
  row_label: string;
  seat_number: number;
  status: SeatStatus;
  hold_expires_at: string | null;
  version: number;
}

export interface SnapshotMessage {
  type: "snapshot";
  event_id: number;
  sections: string[];
  server_time: string;
  seats: SnapshotSeat[];
}

export interface DiffSeatEntry {
  id: number;
  status: SeatStatus;
  hold_expires_at: string | null;
  version: number;
}

export interface DiffMessage {
  type: "diff";
  event_id: number;
  section: string;
  seats: DiffSeatEntry[];
}

export type ServerMessage = SnapshotMessage | DiffMessage;

// The client's own rendering model -- static layout fields (section,
// row, number) only ever come from a snapshot; a diff entry never
// carries them (see app/realtime/hub.py's flush loop), so this shape
// keeps them optional-in-spirit by requiring a SnapshotSeat to create
// one and only ever updating the mutable fields afterward.
export interface RenderedSeat {
  id: number;
  section: string;
  rowLabel: string;
  seatNumber: number;
  status: SeatStatus;
  holdExpiresAt: string | null;
  version: number;
}

export type SeatMap = Map<number, RenderedSeat>;
