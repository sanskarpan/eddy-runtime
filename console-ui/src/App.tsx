import { useEffect, useState } from 'react'
import { useEventStore, type RuntimeEvent, type TaskRecord, type WorkerRecord } from './eventStore'
import './App.css'

type View = 'tasks' | 'workers' | 'wake' | 'polls' | 'metrics' | 'activity'

const BLOCKING_THRESHOLD_NS = 100_000_000

function formatDuration(ns: number): string {
  if (ns >= 1_000_000_000) return `${(ns / 1_000_000_000).toFixed(2)}s`
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(2)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(2)}us`
  return `${Math.round(ns)}ns`
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(0)}%`
}

function statusLabel(status: string): string {
  return status === 'connected' ? 'LIVE' : status.toUpperCase()
}

function percentile(values: number[], fraction: number): number {
  if (values.length === 0) return 0
  const sorted = values.slice().sort((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)]
}

function durationStats(values: number[]) {
  return { p50: percentile(values, 0.5), p99: percentile(values, 0.99), max: Math.max(0, ...values) }
}

function isInSubtree(task: TaskRecord, root: number, tasks: Record<string, TaskRecord>): boolean {
  const seen = new Set<number>()
  let current: TaskRecord | undefined = task
  while (current && !seen.has(current.id)) {
    if (current.id === root) return true
    seen.add(current.id)
    current = current.parent === null ? undefined : tasks[String(current.parent)]
  }
  return false
}

interface MetricPoint {
  sequence: number
  active: number
  queue: number
  stealRate: number
  parkRate: number
  busyRatio: number
}

function buildMetricPoints(events: RuntimeEvent[]): MetricPoint[] {
  const active = new Set<number>()
  const queues = new Map<number, number>()
  const workers = new Set<number>()
  let steals = 0
  let parks = 0
  let busyNs = 0
  let firstTimeNs: number | null = null
  let currentTimeNs = 0

  return events.map((event, index) => {
    switch (event.type) {
      case 'task_spawned': active.add(event.id); break
      case 'task_dropped':
      case 'task_aborted': active.delete(event.id); break
      case 'task_poll_start':
        workers.add(event.worker)
        currentTimeNs = event.at_ns
        firstTimeNs ??= event.at_ns
        break
      case 'task_poll_end':
        workers.add(event.worker)
        busyNs += event.duration_ns
        break
      case 'queue_depth':
        workers.add(event.worker)
        queues.set(event.worker, event.local + event.global)
        break
      case 'worker_steal':
        workers.add(event.thief)
        workers.add(event.victim)
        steals += event.count
        break
      case 'worker_park':
        workers.add(event.worker)
        parks += 1
        break
      case 'worker_unpark': workers.add(event.worker); break
      default: break
    }
    const elapsedNs = firstTimeNs === null ? 0 : Math.max(1, currentTimeNs - firstTimeNs)
    const busyRatio = elapsedNs === 0 || workers.size === 0 ? 0 : Math.min(1, busyNs / (elapsedNs * workers.size))
    return {
      sequence: index + 1,
      active: active.size,
      queue: [...queues.values()].reduce((total, value) => total + value, 0),
      stealRate: steals / (index + 1) * 100,
      parkRate: parks / (index + 1) * 100,
      busyRatio,
    }
  })
}

function App() {
  const [view, setView] = useState<View>('tasks')
  const [selectedTask, setSelectedTask] = useState<number | null>(null)
  const [subtreeRoot, setSubtreeRoot] = useState<number | null>(null)
  const { connect, disconnect } = useEventStore()
  const status = useEventStore((state) => state.status)
  const error = useEventStore((state) => state.error)
  const paused = useEventStore((state) => state.paused)
  const events = useEventStore((state) => state.events)
  const tasks = useEventStore((state) => state.tasks)
  const workers = useEventStore((state) => state.workers)
  const wakeEdges = useEventStore((state) => state.wakeEdges)
  const sequence = useEventStore((state) => state.sequence)
  const warningCount = useEventStore((state) => state.warningCount)
  const setPaused = useEventStore((state) => state.setPaused)

  useEffect(() => {
    connect()
    return disconnect
  }, [connect, disconnect])

  const taskList = Object.values(tasks).sort((left, right) => right.busyNs - left.busyNs || left.id - right.id)
  const workerList = Object.values(workers).sort((left, right) => left.id - right.id)
  const visibleTasks = subtreeRoot === null ? taskList : taskList.filter((task) => isInSubtree(task, subtreeRoot, tasks))
  const selected = (selectedTask === null ? visibleTasks[0] : tasks[String(selectedTask)]) ?? visibleTasks[0]

  const selectTask = (id: number) => {
    setSelectedTask(id)
    setView('tasks')
  }

  return (
    <div className="console-shell">
      <header className="topbar">
        <div className="brand-lockup">
          <span className="brand-mark">e</span>
          <div><strong>eddy / observatory</strong><span>runtime signal console</span></div>
        </div>
        <div className="topbar-actions">
          <span className={`status status-${status}`}><i /> {statusLabel(status)}</span>
          <button className="button button-quiet" type="button" onClick={() => setPaused(!paused)}>{paused ? 'Resume stream' : 'Pause stream'}</button>
          <button className="button button-accent" type="button" onClick={connect} disabled={status === 'connecting'}>{status === 'connecting' ? 'Connecting...' : 'Reconnect'}</button>
        </div>
      </header>

      <main className="content">
        <section className="intro-row">
          <div>
            <p className="eyebrow">PHASE 15 / LIVE TELEMETRY</p>
            <h1>See the runtime breathe.</h1>
            <p className="lede">Task scheduling, worker pressure, and wake paths from one bounded event stream.</p>
          </div>
          <div className="connection-note">
            <span className="signal-line" />
            <div><strong>{status === 'connected' ? 'Receiving runtime events' : 'Waiting for bridge'}</strong><span>{error ?? 'ws://127.0.0.1:9001/ws'}</span></div>
          </div>
        </section>

        <section className="metric-grid" aria-label="Runtime summary">
          <Metric label="tracked tasks" value={taskList.length} detail={`${taskList.filter((task) => task.state === 'running').length} running now`} />
          <Metric label="active workers" value={workerList.length} detail={`${workerList.reduce((total, worker) => total + worker.polls, 0)} polls observed`} />
          <Metric label="event buffer" value={events.length} detail="2,000 event capacity" />
          <Metric label="warnings" value={warningCount} detail={warningCount ? 'inspect task lanes' : 'no anomalies detected'} alert={warningCount > 0} />
        </section>

        <nav className="view-tabs" aria-label="Console views">
          <Tab active={view === 'tasks'} onClick={() => setView('tasks')}>Task lifecycle</Tab>
          <Tab active={view === 'workers'} onClick={() => setView('workers')}>Worker pressure</Tab>
          <Tab active={view === 'wake'} onClick={() => setView('wake')}>Wake graph</Tab>
          <Tab active={view === 'polls'} onClick={() => setView('polls')}>Poll distribution</Tab>
          <Tab active={view === 'metrics'} onClick={() => setView('metrics')}>Runtime metrics</Tab>
          <Tab active={view === 'activity'} onClick={() => setView('activity')}>Event stream</Tab>
        </nav>

        {view === 'tasks' && <TaskView allTasks={taskList} tasks={visibleTasks} timelineEnd={events.length} selected={selected} onSelect={setSelectedTask} subtreeRoot={subtreeRoot} onSubtreeRoot={setSubtreeRoot} />}
        {view === 'workers' && <WorkerView workers={workerList} />}
        {view === 'wake' && <WakeGraph tasks={taskList} edges={wakeEdges} timelineEnd={sequence} selected={selectedTask} onSelect={selectTask} />}
        {view === 'polls' && <PollDistributionView tasks={taskList} onSelect={selectTask} />}
        {view === 'metrics' && <RuntimeMetricsView events={events} />}
        {view === 'activity' && <ActivityView events={events} />}
      </main>
      <footer><span>EDDY INSTRUMENTATION</span><span>{paused ? 'VIEW PAUSED' : 'STREAMING'} - history capped at 2,000</span></footer>
    </div>
  )
}

function Metric({ label, value, detail, alert = false }: { label: string; value: number; detail: string; alert?: boolean }) {
  return <div className={`metric ${alert ? 'metric-alert' : ''}`}><span>{label}</span><strong>{value.toLocaleString()}</strong><small>{detail}</small></div>
}

function Tab({ active, onClick, children }: { active: boolean; onClick: () => void; children: string }) {
  return <button className={`tab ${active ? 'tab-active' : ''}`} type="button" onClick={onClick}>{children}</button>
}

function TaskView({ allTasks, tasks, timelineEnd, selected, onSelect, subtreeRoot, onSubtreeRoot }: { allTasks: TaskRecord[]; tasks: TaskRecord[]; timelineEnd: number; selected?: TaskRecord; onSelect: (id: number) => void; subtreeRoot: number | null; onSubtreeRoot: (id: number | null) => void }) {
  const [zoom, setZoom] = useState(1)
  const [pan, setPan] = useState(0)
  const maxSequence = Math.max(1, timelineEnd, ...allTasks.flatMap((task) => task.segments.flatMap((segment) => [segment.start, segment.end ?? segment.start])))
  const span = Math.max(1, maxSequence / zoom)
  const maxPan = Math.max(0, maxSequence - span)
  const windowStart = pan * maxPan
  const windowEnd = windowStart + span

  return <div className="view-grid task-grid">
    <section className="panel timeline-panel">
      <PanelHeading title="Task lifecycle" detail="queue -> poll -> yield" />
      <div className="timeline-tools">
        <label>task subtree
          <select value={subtreeRoot ?? ''} onChange={(event) => onSubtreeRoot(event.target.value ? Number(event.target.value) : null)}>
            <option value="">all tasks</option>
            {allTasks.map((task) => <option key={task.id} value={task.id}>#{task.id} {task.name}</option>)}
          </select>
        </label>
        <div className="zoom-tools"><span>time axis</span><button className="icon-button" type="button" onClick={() => setZoom((value) => Math.max(1, value / 2))} aria-label="Zoom out">-</button><b>{zoom.toFixed(1)}x</b><button className="icon-button" type="button" onClick={() => setZoom((value) => Math.min(16, value * 2))} aria-label="Zoom in">+</button><button className="button button-quiet" type="button" onClick={() => { setZoom(1); setPan(0) }}>reset</button></div>
      </div>
      <div className="axis"><span>{Math.round(windowStart)}</span><span>{Math.round((windowStart + windowEnd) / 2)}</span><span>{Math.round(windowEnd)} / {maxSequence}</span></div>
      {maxPan > 0 && <label className="pan-control">pan <input type="range" min="0" max="1" step="0.01" value={pan} onChange={(event) => setPan(Number(event.target.value))} aria-label="Pan time axis" /></label>}
      {tasks.length === 0 ? <EmptyState title="No tasks yet" detail="Start an instrumented eddy runtime to populate the lanes." /> : <div className="lanes">
        {tasks.map((task) => <button className={`lane ${selected?.id === task.id ? 'lane-selected' : ''}`} key={task.id} type="button" onClick={() => onSelect(task.id)}>
          <span className="lane-label"><b>#{task.id}</b><span>{task.name}</span></span>
          <span className="lane-track">{task.segments.map((segment, index) => {
            const end = segment.end ?? maxSequence
            const left = Math.max(segment.start, windowStart)
            const right = Math.min(end, windowEnd)
            if (right <= left) return null
            return <i className={`segment segment-${segment.state}`} key={`${task.id}-${index}`} style={{ left: `${(left - windowStart) / span * 100}%`, width: `${Math.max(1.5, (right - left) / span * 100)}%` }} title={`${segment.state} - event ${segment.start}`} />
          })}</span>
          <span className={`state state-${task.state}`}>{task.state}</span>
        </button>)}
      </div>}
      <div className="legend"><span><i className="swatch swatch-queued" />queued</span><span><i className="swatch swatch-running" />running</span><span><i className="swatch swatch-idle" />idle</span></div>
    </section>
    <TaskDetail task={selected} />
  </div>
}

function TaskDetail({ task }: { task?: TaskRecord }) {
  if (!task) return <section className="panel detail-panel"><EmptyState title="Select a task" detail="Task metrics and wake history will appear here." /></section>
  const stats = durationStats(task.pollDurationsNs)
  return <section className="panel detail-panel">
    <PanelHeading title={`Task #${task.id}`} detail={task.location ? `${task.location.file}:${task.location.line}` : 'location unavailable'} />
    <h2>{task.name}</h2>
    <div className="detail-stats"><span><b>{task.polls}</b> polls</span><span><b>{formatDuration(task.busyNs)}</b> busy</span><span><b>{task.scheduled}</b> scheduled</span></div>
    {task.warnings.length > 0 && <div className="warning-box">{task.warnings.map((warning) => <span key={warning}>! {warning}</span>)}</div>}
    <div className="detail-section"><label>poll durations</label><div className="duration-bars">{task.pollDurationsNs.length === 0 ? <small>No completed polls</small> : task.pollDurationsNs.slice(-16).map((duration, index) => <i key={`${duration}-${index}`} style={{ height: `${Math.max(8, Math.min(100, duration / 1_000_000))}%` }} title={formatDuration(duration)} />)}</div><div className="mini-stats"><span>p50 {formatDuration(stats.p50)}</span><span>p99 {formatDuration(stats.p99)}</span><span>max {formatDuration(stats.max)}</span></div></div>
    <div className="detail-section"><label>wake sources</label><p className="wake-list">{task.wakeSources.length ? task.wakeSources.slice(-6).join(' - ') : 'No wakes recorded'}</p></div>
  </section>
}

function WorkerView({ workers }: { workers: WorkerRecord[] }) {
  const peak = Math.max(1, ...workers.flatMap((worker) => worker.samples.map((sample) => sample.local)))
  return <section className="panel worker-panel"><PanelHeading title="Worker pressure" detail="local queue depth over the retained stream" />{workers.length === 0 ? <EmptyState title="No worker samples yet" detail="Queue depth events will form the heatmap." /> : <div className="worker-table">
    {workers.map((worker) => <div className="worker-row" key={worker.id}><span className="worker-name">worker {worker.id}</span><div className="heatmap">{worker.samples.length ? worker.samples.slice(-48).map((sample, index) => <i key={`${sample.sequence}-${index}`} style={{ opacity: 0.2 + sample.local / peak * 0.8 }} title={`local ${sample.local} - global ${sample.global}`} />) : <small>no queue samples</small>}</div><span className="worker-meta">{worker.localQueue} local - {worker.steals} steals</span></div>)}
  </div>}</section>
}

interface WakeGraphProps {
  tasks: TaskRecord[]
  edges: Array<{ from: number; to: number; sequence: number }>
  timelineEnd: number
  selected: number | null
  onSelect: (id: number) => void
}

function cycleNodes(ids: number[], edges: WakeGraphProps['edges']): Set<number> {
  const adjacency = new Map<number, number[]>()
  ids.forEach((id) => adjacency.set(id, []))
  edges.forEach((edge) => adjacency.get(edge.from)?.push(edge.to))
  const state = new Map<number, number>()
  const stack: number[] = []
  const cycles = new Set<number>()
  const visit = (id: number) => {
    state.set(id, 1)
    stack.push(id)
    for (const next of adjacency.get(id) ?? []) {
      if (state.get(next) === 1) {
        const start = stack.indexOf(next)
        stack.slice(start).forEach((value) => cycles.add(value))
      } else if (!state.has(next)) visit(next)
    }
    stack.pop()
    state.set(id, 2)
  }
  ids.forEach((id) => { if (!state.has(id)) visit(id) })
  return cycles
}

function WakeGraph({ tasks, edges, timelineEnd, selected, onSelect }: WakeGraphProps) {
  const maxSequence = Math.max(1, timelineEnd, ...edges.map((edge) => edge.sequence), ...tasks.flatMap((task) => task.wakeHistory.map((wake) => wake.sequence)))
  const [scrubStart, setScrubStart] = useState(1)
  const [scrubEnd, setScrubEnd] = useState(maxSequence)
  const [followLatest, setFollowLatest] = useState(true)
  const windowStart = Math.min(scrubStart, maxSequence)
  const windowEnd = followLatest ? maxSequence : Math.max(windowStart, Math.min(scrubEnd, maxSequence))
  const selectStart = (value: number) => {
    setFollowLatest(false)
    setScrubStart(Math.min(value, windowEnd))
  }
  const selectEnd = (value: number) => {
    setFollowLatest(false)
    setScrubEnd(Math.max(value, windowStart))
  }
  const taskById = new Map(tasks.map((task) => [task.id, task]))
  const graphEdges = edges.filter((edge) => edge.sequence >= windowStart && edge.sequence <= windowEnd && taskById.has(edge.from) && taskById.has(edge.to))
  const windowWakes = tasks.flatMap((task) => task.wakeHistory.filter((wake) => wake.sequence >= windowStart && wake.sequence <= windowEnd).map((wake) => ({ task, wake })))
  const incoming = new Set(graphEdges.map((edge) => edge.to))
  const nodes = tasks.filter((task) => graphEdges.some((edge) => edge.from === task.id || edge.to === task.id) || windowWakes.some(({ task: wakeTask }) => wakeTask.id === task.id))
  const cycles = cycleNodes(nodes.map((task) => task.id), graphEdges)
  const depths = new Map<number, number>()
  const adjacency = new Map<number, number[]>()
  graphEdges.forEach((edge) => adjacency.set(edge.from, [...(adjacency.get(edge.from) ?? []), edge.to]))
  const assignDepth = (id: number, depth: number, path: Set<number>) => {
    if (path.has(id)) return
    if ((depths.get(id) ?? -1) >= depth) return
    depths.set(id, depth)
    const nextPath = new Set(path).add(id)
    ;(adjacency.get(id) ?? []).forEach((next) => assignDepth(next, depth + 1, nextPath))
  }
  nodes.filter((task) => !incoming.has(task.id)).forEach((task) => assignDepth(task.id, 0, new Set()))
  nodes.forEach((task) => { if (!depths.has(task.id)) assignDepth(task.id, 0, new Set()) })
  const columns = new Map<number, TaskRecord[]>()
  nodes.forEach((task) => columns.set(depths.get(task.id) ?? 0, [...(columns.get(depths.get(task.id) ?? 0) ?? []), task]))
  const positions = new Map<number, { x: number; y: number }>()
  columns.forEach((column, depth) => column.forEach((task, index) => positions.set(task.id, { x: depth * 205 + 30, y: index * 68 + 30 })))
  const width = Math.max(700, ((Math.max(0, ...columns.keys()) + 1) * 205) + 30)
  const height = Math.max(250, Math.max(0, ...[...columns.values()].map((column) => column.length)) * 68 + 50)

  return <section className="panel graph-panel"><PanelHeading title="Wake causality" detail={`${windowWakes.length} wakes - ${graphEdges.length} task edges in selected window`} />
    <div className="wake-scrubber" aria-label="Wake graph time scrubber">
      <div className="wake-scrubber-heading"><div><label>time scrubber</label><strong>events {windowStart} - {windowEnd} / {maxSequence}</strong></div>{followLatest ? <span className="scrubber-live">following latest</span> : <button className="button button-quiet" type="button" onClick={() => setFollowLatest(true)}>follow latest</button>}</div>
      <div className="scrubber-controls">
        <label>from event {windowStart}<input type="range" min="1" max={windowEnd} step="1" value={windowStart} onChange={(event) => selectStart(Number(event.target.value))} aria-label="Start of wake event window" /></label>
        <label>through event {windowEnd}<input type="range" min={windowStart} max={maxSequence} step="1" value={windowEnd} onChange={(event) => selectEnd(Number(event.target.value))} aria-label="End of wake event window" /></label>
      </div>
      <div className="scrubber-axis"><span>oldest</span><span>latest</span></div>
    </div>
    {nodes.length === 0 ? <EmptyState title="No wake paths in this window" detail="Move the time scrubber across the retained event history." /> : <>
    <div className="graph-key"><span><i className="graph-dot graph-root" />root wake</span><span><i className="graph-dot graph-cycle" />cycle</span><span>click a node for task detail</span></div>
    <div className="graph-scroll"><svg className="wake-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Task wake causality graph">
      <defs><marker id="wake-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0,0 L7,3.5 L0,7 z" fill="currentColor" /></marker></defs>
      {graphEdges.map((edge) => {
        const from = positions.get(edge.from)
        const to = positions.get(edge.to)
        if (!from || !to) return null
        const cyclic = cycles.has(edge.from) && cycles.has(edge.to)
        return <line className={cyclic ? 'graph-edge graph-edge-cycle' : 'graph-edge'} key={`${edge.from}-${edge.to}-${edge.sequence}`} x1={from.x + 130} y1={from.y + 18} x2={to.x} y2={to.y + 18} markerEnd="url(#wake-arrow)" />
      })}
      {nodes.map((task) => {
        const position = positions.get(task.id) ?? { x: 0, y: 0 }
        const root = !incoming.has(task.id) || windowWakes.some(({ task: wakeTask, wake }) => wakeTask.id === task.id && !wake.source.startsWith('task:'))
        return <g className={`graph-node ${root ? 'graph-node-root' : ''} ${cycles.has(task.id) ? 'graph-node-cycle' : ''} ${selected === task.id ? 'graph-node-selected' : ''}`} key={task.id} transform={`translate(${position.x}, ${position.y})`} onClick={() => onSelect(task.id)} role="button" tabIndex={0} onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') onSelect(task.id) }}>
          <rect width="130" height="36" rx="2" /><text x="9" y="15">#{task.id}</text><text className="graph-name" x="9" y="28">{task.name.slice(0, 18)}</text>
        </g>
      })}
    </svg></div>
  </>}</section>
}

function PollDistributionView({ tasks, onSelect }: { tasks: TaskRecord[]; onSelect: (id: number) => void }) {
  const rows = tasks.filter((task) => task.pollDurationsNs.length > 0).map((task) => ({ task, stats: durationStats(task.pollDurationsNs) })).sort((left, right) => right.stats.p99 - left.stats.p99 || right.stats.max - left.stats.max)
  return <section className="panel distribution-panel"><PanelHeading title="Poll duration distribution" detail="sorted by p99 · click a row for task detail" />
    <div className="distribution-key"><span><i className="key-bar" />poll samples</span><span><i className="key-line" />100ms blocking threshold</span><span>p50 / p99 / max shown at right</span></div>
    {rows.length === 0 ? <EmptyState title="No completed polls" detail="Poll duration distributions will appear as tasks yield." /> : <div className="distribution-list">{rows.map(({ task, stats }) => {
      const chartMax = Math.max(BLOCKING_THRESHOLD_NS, stats.max)
      const bars = task.pollDurationsNs.slice(-32)
      return <button className="distribution-row" type="button" key={task.id} onClick={() => onSelect(task.id)}>
        <span className="distribution-label"><b>#{task.id}</b><span>{task.name}</span></span>
        <span className="distribution-chart">{bars.map((duration, index) => <i key={`${duration}-${index}`} style={{ height: `${Math.max(8, duration / chartMax * 100)}%` }} title={formatDuration(duration)} />)}<i className="threshold-line" style={{ left: `${BLOCKING_THRESHOLD_NS / chartMax * 100}%` }} /><i className="marker-line marker-p50" style={{ left: `${stats.p50 / chartMax * 100}%` }} /><i className="marker-line marker-p99" style={{ left: `${stats.p99 / chartMax * 100}%` }} /></span>
        <span className="distribution-stats"><span>p50 <b>{formatDuration(stats.p50)}</b></span><span>p99 <b>{formatDuration(stats.p99)}</b></span><span>max <b>{formatDuration(stats.max)}</b></span></span>
      </button>
    })}</div>}
  </section>
}

function MetricChart({ label, points, value, color = 'var(--cyan)' }: { label: string; points: Array<{ sequence: number; value: number }>; value: (number: number) => string; color?: string }) {
  const max = Math.max(1, ...points.map((point) => point.value))
  const plotted = points.slice(-100)
  const coordinates = plotted.map((point, index) => `${plotted.length <= 1 ? 300 : index / (plotted.length - 1) * 600},${112 - point.value / max * 96}`).join(' ')
  const latest = plotted.at(-1)?.value ?? 0
  return <div className="metric-chart"><div className="chart-heading"><label>{label}</label><b>{value(latest)}</b></div><svg viewBox="0 0 600 120" preserveAspectRatio="none" aria-label={`${label} chart`}><path className="chart-guide" d="M0 16H600 M0 64H600 M0 112H600" /><polyline points={coordinates} style={{ stroke: color }} /></svg><div className="chart-axis"><span>0</span><span>{value(max)}</span></div></div>
}

function RuntimeMetricsView({ events }: { events: RuntimeEvent[] }) {
  const rawPoints = buildMetricPoints(events)
  const points = rawPoints.map((point) => ({ sequence: point.sequence, active: point.active, queue: point.queue, stealRate: point.stealRate, parkRate: point.parkRate, busyRatio: point.busyRatio }))
  const lateness = events.filter((event): event is Extract<RuntimeEvent, { type: 'timer_fired' }> => event.type === 'timer_fired').map((event) => event.lateness_ns)
  const latenessStats = durationStats(lateness)
  const timerMax = Math.max(1, latenessStats.max)
  const timerBins = Array.from({ length: 12 }, () => 0)
  lateness.forEach((duration) => { timerBins[Math.min(timerBins.length - 1, Math.floor(duration / timerMax * timerBins.length))] += 1 })
  const timerPeak = Math.max(1, ...timerBins)
  return <div className="metrics-view">
    <div className="metric-charts"><MetricChart label="active tasks" points={points.map((point) => ({ sequence: point.sequence, value: point.active }))} value={(number) => `${number.toFixed(0)}`} color="var(--green)" /><MetricChart label="queue depth" points={points.map((point) => ({ sequence: point.sequence, value: point.queue }))} value={(number) => `${number.toFixed(0)}`} /><MetricChart label="steal rate / 100 events" points={points.map((point) => ({ sequence: point.sequence, value: point.stealRate }))} value={(number) => `${number.toFixed(1)}`} color="var(--yellow)" /><MetricChart label="park rate / 100 events" points={points.map((point) => ({ sequence: point.sequence, value: point.parkRate }))} value={(number) => `${number.toFixed(1)}`} color="var(--yellow)" /><MetricChart label="worker busy ratio" points={points.map((point) => ({ sequence: point.sequence, value: point.busyRatio }))} value={formatPercent} color="var(--green)" /></div>
    <section className="panel timer-panel"><PanelHeading title="Timer lateness" detail={`${lateness.length} fired timers`} />{lateness.length === 0 ? <EmptyState title="No timer firings yet" detail="Timer lateness will validate wheel accuracy." /> : <><div className="timer-bars">{timerBins.map((count, index) => <i key={index} style={{ height: `${Math.max(5, count / timerPeak * 100)}%` }} title={`${count} timers`} />)}</div><div className="mini-stats"><span>p50 {formatDuration(latenessStats.p50)}</span><span>p99 {formatDuration(latenessStats.p99)}</span><span>max {formatDuration(latenessStats.max)}</span></div></>}</section>
  </div>
}

function ActivityView({ events }: { events: RuntimeEvent[] }) {
  return <section className="panel activity-panel"><PanelHeading title="Event stream" detail={`${events.length.toLocaleString()} retained events`} />{events.length === 0 ? <EmptyState title="Stream is quiet" detail="Decoded runtime events will appear here." /> : <div className="activity-list">{events.slice(-80).reverse().map((event, index) => <div className="activity-row" key={`${event.type}-${index}`}><span className="activity-index">{events.length - index}</span><b>{event.type.replaceAll('_', ' ')}</b><code>{'id' in event ? `task ${String(event.id)}` : 'worker' in event ? `worker ${String(event.worker)}` : ''}</code></div>)}</div>}</section>
}

function PanelHeading({ title, detail }: { title: string; detail: string }) {
  return <div className="panel-heading"><div><h2>{title}</h2><span>{detail}</span></div><span className="panel-dot" /></div>
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return <div className="empty-state"><span className="empty-mark">~</span><strong>{title}</strong><span>{detail}</span></div>
}

export default App
