import React, { useEffect, useState, useRef } from 'react';
import { 
  Activity, 
  Server, 
  AlertCircle, 
  CheckCircle2, 
  Clock, 
  ListTodo, 
  Play, 
  XOctagon, 
  Ban, 
  RotateCcw
} from 'lucide-react';

interface Task {
  id: string;
  status: string;
  priority: number;
  payload: any;
  result: any;
  error_message: string | null;
  retries: number;
  max_retries: number;
  created_at: string;
  updated_at: string;
}

interface Worker {
  worker_id: string;
  hostname: string;
  status: string;
  last_heartbeat: string;
}

interface QueueStats {
  tasks: {
    pending: number;
    processing: number;
    scheduled: number;
    dead_letter: number;
  };
  workers: {
    total: number;
    healthy: number;
    unhealthy: number;
  };
}

const statusColors: Record<string, string> = {
  PENDING: 'var(--status-pending)',
  QUEUED: 'var(--status-queued)',
  RUNNING: 'var(--status-running)',
  COMPLETED: 'var(--status-completed)',
  FAILED: 'var(--status-failed)',
  RETRYING: 'var(--status-retrying)',
  CANCEL_REQUESTED: 'var(--status-cancelled)',
  CANCELLED: 'var(--status-cancelled)',
  DEAD_LETTER: 'var(--status-failed)'
};

const statusIcons: Record<string, React.ReactNode> = {
  PENDING: <Clock size={16} />,
  QUEUED: <ListTodo size={16} />,
  RUNNING: <Play size={16} />,
  COMPLETED: <CheckCircle2 size={16} />,
  FAILED: <AlertCircle size={16} />,
  RETRYING: <RotateCcw size={16} />,
  CANCEL_REQUESTED: <Ban size={16} />,
  CANCELLED: <XOctagon size={16} />,
  DEAD_LETTER: <AlertCircle size={16} />
};

export default function App() {
  const [stats, setStats] = useState<QueueStats | null>(null);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [connected, setConnected] = useState(false);
  
  const wsRef = useRef<WebSocket | null>(null);

  const fetchInitialData = async () => {
    try {
      const [statsRes, workersRes, tasksRes] = await Promise.all([
        fetch('/api/queue/stats'),
        fetch('/api/workers'),
        fetch('/api/tasks?limit=15')
      ]);
      if (statsRes.ok) setStats(await statsRes.json());
      if (workersRes.ok) setWorkers(await workersRes.json());
      if (tasksRes.ok) setTasks(await tasksRes.json());
    } catch (e) {
      console.error("Failed to fetch initial data", e);
    }
  };

  useEffect(() => {
    fetchInitialData();
    const interval = setInterval(fetchInitialData, 5000);
    
    // WebSocket setup
    const wsUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/metrics`;
    
    const connectWs = () => {
      const ws = new WebSocket(wsUrl);
      
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        setTimeout(connectWs, 2000); // Reconnect
      };
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'TASK_EVENT') {
            // const eventType = data.event; // e.g., task.created, task.queued, task.started
            const payload = data.payload;
            
            // Update tasks list if it's already there
            setTasks(prev => {
              const idx = prev.findIndex(t => t.id === payload.id);
              if (idx >= 0) {
                const newTasks = [...prev];
                newTasks[idx] = { ...newTasks[idx], ...payload, updated_at: new Date().toISOString() };
                return newTasks;
              } else {
                // Fetch new tasks to maintain order, or just prepend
                return [
                  { 
                    id: payload.id, 
                    status: payload.status, 
                    priority: payload.priority, 
                    payload: {}, 
                    result: null, 
                    error_message: null, 
                    retries: 0, 
                    max_retries: 3, 
                    created_at: new Date().toISOString(), 
                    updated_at: new Date().toISOString() 
                  },
                  ...prev
                ].slice(0, 15);
              }
            });
            
            // Refresh stats to ensure sync
            fetchInitialData();
          }
        } catch (e) {
          console.error("Failed to process WS message", e);
        }
      };
      wsRef.current = ws;
    };
    
    connectWs();
    
    return () => {
      clearInterval(interval);
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  const handleCreateTask = async (type: string, priority: number = 2) => {
    try {
      await fetch('/api/tasks/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          priority,
          payload: { type, duration: 2 }
        })
      });
      fetchInitialData();
    } catch (e) {
      console.error(e);
    }
  };

  const handleCancelTask = async (taskId: string) => {
    try {
      await fetch(`/api/tasks/${taskId}/cancel`, { method: 'POST' });
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <div className="container">
      <div className="flex justify-between items-center" style={{ marginBottom: '2rem' }}>
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Activity className="glow-text" color="var(--accent-primary)" />
            Antigravity Task Queue
          </h1>
          <p className="text-muted text-sm" style={{ marginTop: '0.25rem' }}>
            Fault-Tolerant Distributed Task Processing
          </p>
        </div>
        
        <div className="flex items-center gap-2">
          <div style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: connected ? '#10b981' : '#ef4444' }} className={connected ? 'pulse' : ''} />
          <span className="text-sm font-medium" style={{ color: connected ? '#10b981' : '#ef4444' }}>
            {connected ? 'Live' : 'Reconnecting...'}
          </span>
        </div>
      </div>
      
      {/* Action Bar */}
      <div className="panel flex gap-4 items-center" style={{ marginBottom: '2rem' }}>
        <span className="text-sm font-semibold text-muted">TEST ACTIONS:</span>
        <button className="btn btn-primary" onClick={() => handleCreateTask('sleep', 2)}>
          + Submit Normal Task
        </button>
        <button className="btn btn-primary" onClick={() => handleCreateTask('sleep', 0)} style={{ background: '#ef4444' }}>
          + Submit Critical Task
        </button>
        <button className="btn btn-primary" onClick={() => handleCreateTask('fail', 2)} style={{ background: '#f97316' }}>
          + Submit Failing Task
        </button>
      </div>
      
      {/* Stats Grid */}
      <div className="grid grid-cols-4 gap-6" style={{ marginBottom: '2rem' }}>
        <div className="panel">
          <h3 className="text-sm font-medium text-muted uppercase tracking-wider flex items-center gap-2">
            <ListTodo size={16} /> Pending / Queued
          </h3>
          <div className="stat-value" style={{ color: 'var(--status-queued)' }}>
            {stats ? stats.tasks.pending : '-'}
          </div>
        </div>
        
        <div className="panel">
          <h3 className="text-sm font-medium text-muted uppercase tracking-wider flex items-center gap-2">
            <Play size={16} /> Processing
          </h3>
          <div className="stat-value" style={{ color: 'var(--status-running)' }}>
            {stats ? stats.tasks.processing : '-'}
          </div>
        </div>
        
        <div className="panel">
          <h3 className="text-sm font-medium text-muted uppercase tracking-wider flex items-center gap-2">
            <AlertCircle size={16} /> Dead Letter
          </h3>
          <div className="stat-value" style={{ color: 'var(--status-failed)' }}>
            {stats ? stats.tasks.dead_letter : '-'}
          </div>
        </div>
        
        <div className="panel">
          <h3 className="text-sm font-medium text-muted uppercase tracking-wider flex items-center gap-2">
            <Server size={16} /> Active Workers
          </h3>
          <div className="stat-value" style={{ color: '#10b981' }}>
            {stats ? stats.workers.healthy : '-'}
            <span className="text-sm text-muted font-normal" style={{ marginLeft: '0.5rem' }}>
              / {stats ? stats.workers.total : '-'} total
            </span>
          </div>
        </div>
      </div>
      
      <div className="grid grid-cols-3 gap-6">
        {/* Recent Tasks */}
        <div className="panel" style={{ gridColumn: 'span 2' }}>
          <h2 className="text-lg font-bold mb-4" style={{ marginBottom: '1rem' }}>Recent Tasks</h2>
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th>Type</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {tasks.length === 0 ? (
                  <tr>
                    <td colSpan={6} style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
                      No tasks found.
                    </td>
                  </tr>
                ) : (
                  tasks.map(t => (
                    <tr key={t.id}>
                      <td className="text-xs font-medium" style={{ opacity: 0.8 }}>
                        {t.id.substring(0, 8)}...
                      </td>
                      <td>
                        <span 
                          className="badge flex items-center gap-1" 
                          style={{ 
                            color: statusColors[t.status] || 'white',
                            backgroundColor: `${statusColors[t.status]}20` || 'rgba(255,255,255,0.1)',
                            border: `1px solid ${statusColors[t.status]}40`
                          }}
                        >
                          {statusIcons[t.status]}
                          {t.status}
                        </span>
                      </td>
                      <td className="text-sm">
                        {t.priority === 0 ? 'CRITICAL' : t.priority === 1 ? 'HIGH' : t.priority === 2 ? 'NORMAL' : 'LOW'}
                      </td>
                      <td className="text-sm font-medium">
                        {t.payload?.type || 'unknown'}
                      </td>
                      <td className="text-xs text-muted">
                        {new Date(t.created_at).toLocaleTimeString()}
                      </td>
                      <td>
                        {['PENDING', 'QUEUED', 'RUNNING'].includes(t.status) && (
                          <button 
                            className="btn btn-danger text-xs" 
                            style={{ padding: '0.25rem 0.5rem' }}
                            onClick={() => handleCancelTask(t.id)}
                          >
                            Cancel
                          </button>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
        
        {/* Workers List */}
        <div className="panel">
          <h2 className="text-lg font-bold mb-4" style={{ marginBottom: '1rem' }}>Worker Nodes</h2>
          <div className="flex flex-col gap-2">
            {workers.length === 0 ? (
              <p className="text-muted text-sm text-center">No active workers.</p>
            ) : (
              workers.map(w => (
                <div key={w.worker_id} className="flex justify-between items-center" style={{ padding: '0.75rem', background: 'rgba(255,255,255,0.02)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
                  <div className="flex items-center gap-3">
                    <div style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: w.status === 'ONLINE' ? '#10b981' : '#ef4444' }} />
                    <div>
                      <div className="text-sm font-medium">{w.hostname}</div>
                      <div className="text-xs text-muted font-mono">{w.worker_id.substring(0, 8)}</div>
                    </div>
                  </div>
                  <div className="text-xs text-muted">
                    {new Date(w.last_heartbeat).toLocaleTimeString()}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
