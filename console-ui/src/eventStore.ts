import { create } from 'zustand'

export const MAX_HISTORY = 2000
const MAX_DETAIL_HISTORY = 64
const MAX_WORKER_SAMPLES = 128

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected' | 'error'
export type TaskState = 'queued' | 'running' | 'idle' | 'done' | 'cancelled'

export type RuntimeEvent =
  | { type: 'task_spawned'; id: number; name: string | null; location: Location; parent: number | null }
  | { type: 'task_poll_start'; id: number; worker: number; at_ns: number }
  | { type: 'task_poll_end'; id: number; worker: number; duration_ns: number; result: 'ready' | 'pending' | 'panicked' }
  | { type: 'task_woken'; id: number; by: WakeSource }
  | { type: 'task_dropped'; id: number; total_polls: number; total_busy_ns: number; total_idle_ns: number }
  | { type: 'task_aborted'; id: number }
  | { type: 'worker_park'; worker: number; timeout_ns: number | null }
  | { type: 'worker_unpark'; worker: number; reason: string }
  | { type: 'worker_steal'; thief: number; victim: number; count: number }
  | { type: 'queue_depth'; worker: number; local: number; global: number; lifo: boolean }
  | { type: 'io_registered'; fd: number; interest: string; task: number }
  | { type: 'io_ready'; fd: number; readiness: string; woke: number[] }
  | { type: 'timer_set'; id: number; deadline_ns: number; task: number }
  | { type: 'timer_fired'; id: number; lateness_ns: number }
  | { type: 'timer_cancelled'; id: number }
  | { type: 'blocking_detected'; task: number; poll_duration_ns: number; location: Location }
  | { type: 'budget_exhausted'; task: number }
  | { type: 'resource_contended'; kind: string; holder: number; waiters: number[] }

export interface Location {
  file: string
  line: number
}

export type WakeSource =
  | { kind: 'io'; fd: number }
  | { kind: 'timer'; id: number }
  | { kind: 'task'; id: number }
  | { kind: 'channel'; channel: string }
  | { kind: 'external' }

export interface TimelineSegment {
  state: 'queued' | 'running' | 'idle'
  start: number
  end: number | null
}

export interface WakeRecord {
  sequence: number
  source: string
}

export interface TaskRecord {
  id: number
  name: string
  parent: number | null
  state: TaskState
  worker: number
  polls: number
  busyNs: number
  idleNs: number
  scheduled: number
  location: Location | null
  warnings: string[]
  pollDurationsNs: number[]
  wakeSources: string[]
  wakeHistory: WakeRecord[]
  segments: TimelineSegment[]
}

export interface WorkerSample {
  sequence: number
  local: number
  global: number
}

export interface WorkerRecord {
  id: number
  polls: number
  busyNs: number
  parks: number
  steals: number
  localQueue: number
  globalQueue: number
  samples: WorkerSample[]
}

export interface Projection {
  tasks: Record<string, TaskRecord>
  workers: Record<string, WorkerRecord>
  wakeEdges: Array<{ from: number; to: number; sequence: number }>
  warningCount: number
  sequence: number
}

export interface EventStore extends Projection {
  events: RuntimeEvent[]
  status: ConnectionStatus
  error: string | null
  paused: boolean
  lastEventAt: number | null
  ingest: (event: RuntimeEvent) => void
  setStatus: (status: ConnectionStatus, error?: string | null) => void
  setPaused: (paused: boolean) => void
  connect: () => void
  disconnect: () => void
}

const initialProjection = (): Projection => ({
  tasks: {},
  workers: {},
  wakeEdges: [],
  warningCount: 0,
  sequence: 0,
})

export function appendBounded<T>(items: T[], item: T, limit = MAX_HISTORY): T[] {
  if (limit <= 0) return []
  const next = items.length >= limit ? items.slice(items.length - limit + 1) : items.slice()
  next.push(item)
  return next
}

function taskRecord(id: number): TaskRecord {
  return {
    id,
    name: '<unnamed>',
    parent: null,
    state: 'queued',
    worker: 0,
    polls: 0,
    busyNs: 0,
    idleNs: 0,
    scheduled: 1,
    location: null,
    warnings: [],
    pollDurationsNs: [],
    wakeSources: [],
    wakeHistory: [],
    segments: [],
  }
}

function workerRecord(id: number): WorkerRecord {
  return { id, polls: 0, busyNs: 0, parks: 0, steals: 0, localQueue: 0, globalQueue: 0, samples: [] }
}

function warning(task: TaskRecord, value: string): void {
  if (!task.warnings.includes(value)) task.warnings.push(value)
}

function closeSegment(task: TaskRecord, sequence: number): void {
  const open = task.segments[task.segments.length - 1]
  if (open?.end === null) open.end = sequence
}

function addSegment(task: TaskRecord, state: TimelineSegment['state'], sequence: number): void {
  closeSegment(task, sequence)
  task.segments.push({ state, start: sequence, end: null })
}

function wakeLabel(source: WakeSource): string {
  switch (source.kind) {
    case 'io': return `io:${source.fd}`
    case 'timer': return `timer:${source.id}`
    case 'task': return `task:${source.id}`
    case 'channel': return `channel:${source.channel}`
    case 'external': return 'external'
  }
}

/** Apply one wire event to the small, render-ready browser projection. */
export function applyEvent(projection: Projection, event: RuntimeEvent, sequence: number): Projection {
  const next: Projection = {
    tasks: { ...projection.tasks },
    workers: { ...projection.workers },
    wakeEdges: projection.wakeEdges.slice(),
    warningCount: projection.warningCount,
    sequence,
  }
  const getTask = (id: number): TaskRecord => {
    const task = { ...(next.tasks[String(id)] ?? taskRecord(id)) }
    task.warnings = task.warnings.slice()
    task.pollDurationsNs = task.pollDurationsNs.slice()
    task.wakeSources = task.wakeSources.slice()
    task.wakeHistory = task.wakeHistory.slice()
    task.segments = task.segments.map((segment) => ({ ...segment }))
    next.tasks[String(id)] = task
    return task
  }
  const getWorker = (id: number): WorkerRecord => {
    const worker = { ...(next.workers[String(id)] ?? workerRecord(id)) }
    worker.samples = worker.samples.slice()
    next.workers[String(id)] = worker
    return worker
  }
  const rememberDuration = (task: TaskRecord, duration: number): void => {
    task.pollDurationsNs = appendBounded(task.pollDurationsNs, duration, MAX_DETAIL_HISTORY)
  }

  switch (event.type) {
    case 'task_spawned': {
      const task = getTask(event.id)
      task.name = event.name ?? '<unnamed>'
      task.parent = event.parent
      task.location = event.location
      task.state = 'queued'
      if (task.segments.length === 0) addSegment(task, 'queued', sequence)
      break
    }
    case 'task_poll_start': {
      const task = getTask(event.id)
      task.worker = event.worker
      task.state = 'running'
      addSegment(task, 'running', sequence)
      getWorker(event.worker)
      break
    }
    case 'task_poll_end': {
      const task = getTask(event.id)
      task.worker = event.worker
      task.polls += 1
      task.busyNs += event.duration_ns
      rememberDuration(task, event.duration_ns)
      task.state = event.result === 'ready' ? 'done' : event.result === 'pending' ? 'idle' : 'cancelled'
      if (event.result === 'panicked') warning(task, 'panic')
      const worker = getWorker(event.worker)
      worker.polls += 1
      worker.busyNs += event.duration_ns
      closeSegment(task, sequence)
      if (event.result === 'pending') task.segments.push({ state: 'idle', start: sequence, end: null })
      break
    }
    case 'task_woken': {
      const task = getTask(event.id)
      task.state = 'queued'
      task.scheduled += 1
      const source = wakeLabel(event.by)
      task.wakeSources = appendBounded(task.wakeSources, source, MAX_DETAIL_HISTORY)
      task.wakeHistory = appendBounded(task.wakeHistory, { sequence, source }, MAX_DETAIL_HISTORY)
      addSegment(task, 'queued', sequence)
      if (event.by.kind === 'task') next.wakeEdges.push({ from: event.by.id, to: event.id, sequence })
      break
    }
    case 'task_dropped': {
      const task = getTask(event.id)
      task.state = 'done'
      task.polls = event.total_polls
      task.busyNs = event.total_busy_ns
      task.idleNs = event.total_idle_ns
      closeSegment(task, sequence)
      break
    }
    case 'task_aborted': {
      const task = getTask(event.id)
      task.state = 'cancelled'
      closeSegment(task, sequence)
      break
    }
    case 'worker_park': getWorker(event.worker).parks += 1; break
    case 'worker_unpark': getWorker(event.worker); break
    case 'worker_steal': {
      getWorker(event.thief).steals += event.count
      getWorker(event.victim)
      break
    }
    case 'queue_depth': {
      const worker = getWorker(event.worker)
      worker.localQueue = event.local
      worker.globalQueue = event.global
      worker.samples = appendBounded(worker.samples, { sequence, local: event.local, global: event.global }, MAX_WORKER_SAMPLES)
      break
    }
    case 'blocking_detected': {
      const task = getTask(event.task)
      warning(task, 'blocking')
      rememberDuration(task, event.poll_duration_ns)
      break
    }
    case 'budget_exhausted': warning(getTask(event.task), 'budget'); break
    case 'io_registered': getTask(event.task); break
    case 'io_ready': event.woke.forEach((id) => getTask(id)); break
    case 'timer_set': getTask(event.task); break
    case 'timer_fired':
    case 'timer_cancelled':
    case 'resource_contended':
      break
  }
  next.warningCount = Object.values(next.tasks).reduce((count, task) => count + task.warnings.length, 0)
  return next
}

export function projectEvents(events: RuntimeEvent[]): Projection {
  return events.reduce((projection, event, index) => applyEvent(projection, event, index + 1), initialProjection())
}

const eventTypes = new Set([
  'task_spawned', 'task_poll_start', 'task_poll_end', 'task_woken', 'task_dropped', 'task_aborted',
  'worker_park', 'worker_unpark', 'worker_steal', 'queue_depth', 'io_registered', 'io_ready',
  'timer_set', 'timer_fired', 'timer_cancelled', 'blocking_detected', 'budget_exhausted', 'resource_contended',
])

export function parseRuntimeEvent(value: unknown): RuntimeEvent | null {
  if (typeof value !== 'object' || value === null || !('type' in value)) return null
  const type = (value as { type: unknown }).type
  return typeof type === 'string' && eventTypes.has(type) ? value as RuntimeEvent : null
}

let socket: WebSocket | null = null

export const useEventStore = create<EventStore>((set, get) => ({
  ...initialProjection(),
  events: [],
  status: 'disconnected',
  error: null,
  paused: false,
  lastEventAt: null,
  ingest: (event) => set((state) => {
    const events = appendBounded(state.events, event)
    if (state.paused) return { events, lastEventAt: Date.now() }
    return { ...state, ...applyEvent(state, event, state.sequence + 1), events, lastEventAt: Date.now() }
  }),
  setStatus: (status, error = null) => set({ status, error }),
  setPaused: (paused) => set((state) => paused
    ? { paused }
    : { paused, ...projectEvents(state.events) }),
  connect: () => {
    socket?.close()
    const configured = import.meta.env.VITE_BRIDGE_URL as string | undefined
    const url = configured ?? `ws://${window.location.hostname || '127.0.0.1'}:9001/ws`
    set({ status: 'connecting', error: null })
    const nextSocket = new WebSocket(url)
    socket = nextSocket
    nextSocket.onopen = () => set({ status: 'connected', error: null })
    nextSocket.onmessage = async (message) => {
      const raw = typeof message.data === 'string' ? message.data : await new Blob([message.data]).text()
      try {
        const value = JSON.parse(raw) as unknown
        if (typeof value === 'object' && value !== null && 'type' in value && (value as { type: unknown }).type === 'bridge_status') {
          const status = value as { state?: string; reason?: string }
          set({ status: status.state === 'error' ? 'error' : 'disconnected', error: status.reason ?? null })
          return
        }
        const event = parseRuntimeEvent(value)
        if (event) get().ingest(event)
      } catch {
        set({ status: 'error', error: 'received invalid JSON from bridge' })
      }
    }
    nextSocket.onerror = () => set({ status: 'error', error: 'WebSocket connection failed' })
    nextSocket.onclose = () => {
      if (socket === nextSocket) {
        socket = null
        set((state) => state.status === 'error' ? state : { status: 'disconnected' })
      }
    }
  },
  disconnect: () => {
    socket?.close()
    socket = null
    set({ status: 'disconnected' })
  },
}))
