
import { useEffect, useRef, useState } from 'react'
import { io, type Socket } from 'socket.io-client'
import './App.css'

type ActivationStatus =
  | 'PENDING'
  | 'IN_PROGRESS'
  | 'ACTIVE'
  | 'FAILED'

type HistoryEvent = {
  eventType: string
  at: string
  eventId: string
}

type Activation = {
  _id: string
  customerId: string
  planId: string
  status: ActivationStatus
  history: HistoryEvent[]
  updatedAt?: string
}

const API_URL = 'http://localhost:3000'

function App() {
  const [customerId, setCustomerId] = useState('C-9002')
  const [planId, setPlanId] = useState('PLAN-FIBRA')
  const [simulateFailure, setSimulateFailure] =
    useState('none')

  const [activation, setActivation] =
    useState<Activation | null>(null)

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [connected, setConnected] = useState(false)

  const socketRef = useRef<Socket | null>(null)
  const activationIdRef = useRef<string | null>(null)

  // Evitar que una respuesta HTTP antigua
  // reemplace un estado mas reciente.
  function applyActivation(next: Activation) {
    if (next._id !== activationIdRef.current) return

    setActivation((previous) => {
      if (!previous || previous._id !== next._id) {
        return next
      }

      const previousTime = Date.parse(
        previous.updatedAt ?? '',
      )
      const nextTime = Date.parse(next.updatedAt ?? '')

      if (
        Number.isFinite(previousTime) &&
        Number.isFinite(nextTime) &&
        nextTime < previousTime
      ) {
        return previous
      }

      if (
        nextTime === previousTime &&
        next.history.length < previous.history.length
      ) {
        return previous
      }

      return next
    })
  }

  async function fetchActivation(id: string) {
    try {
      const response = await fetch(
        `${API_URL}/activations/${id}`,
      )

      if (!response.ok) {
        throw new Error('No se pudo consultar la activación')
      }

      const data: Activation = await response.json()
      applyActivation(data)
    } catch (err) {
      if (id === activationIdRef.current) {
        setError(
          err instanceof Error
            ? err.message
            : 'Error consultando la activación',
        )
      }
    }
  }

  // Conexion WebSocket: se establece una sola vez
  // mientras la pantalla este abierta.
  useEffect(() => {
    const socket = io(API_URL, {
      transports: ['websocket'],
      reconnection: true,
    })

    socketRef.current = socket

    const handleConnect = () => {
      setConnected(true)

      const id = activationIdRef.current

      if (id) {
        socket.emit('watch-activation', id)

        // Recuperar eventos perdidos durante
        // una posible desconexion.
        void fetchActivation(id)
      }
    }

    const handleDisconnect = () => {
      setConnected(false)
    }

    const handleUpdate = (data: Activation) => {
      applyActivation(data)
    }

    socket.on('connect', handleConnect)
    socket.on('disconnect', handleDisconnect)
    socket.on('activation:updated', handleUpdate)

    if (socket.connected) {
      handleConnect()
    }

    return () => {
      socket.off('connect', handleConnect)
      socket.off('disconnect', handleDisconnect)
      socket.off('activation:updated', handleUpdate)
      socket.disconnect()
      socketRef.current = null
    }
  }, [])

  async function createActivation() {
    if (!customerId.trim()) {
      setError('Ingresá un identificador de cliente.')
      return
    }

    setLoading(true)
    setError('')

    try {
      const response = await fetch(
        `${API_URL}/activations`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            customerId,
            planId,
            simulateFailure,
          }),
        },
      )

      if (!response.ok) {
        throw new Error('No se pudo crear la contratación')
      }

      const data: { activationId: string } =
        await response.json()

      const previousId = activationIdRef.current

      if (previousId && socketRef.current) {
        socketRef.current.emit(
          'unwatch-activation',
          previousId,
        )
      }

      activationIdRef.current = data.activationId
      setActivation(null)

      if (socketRef.current?.connected) {
        socketRef.current.emit(
          'watch-activation',
          data.activationId,
        )
      }

      // Primero suscribirse y luego recuperar
      // el historial existente mediante HTTP.
      await fetchActivation(data.activationId)
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : 'Ocurrió un error inesperado',
      )
    } finally {
      setLoading(false)
    }
  }

  return (
    <main className="dashboard">
      <header className="header">
        <div>
          <p className="eyebrow">
            SISTEMAS DISTRIBUIDOS · 2026
          </p>
          <h1>Activation POC</h1>
          <p>
            Activación de servicios mediante Kafka
            y patrón Saga
          </p>
        </div>

        <span className="system-badge">
          {connected
            ? '● WebSocket conectado'
            : '○ WebSocket desconectado'}
        </span>
      </header>

      <section className="panel">
        <h2>Nueva contratación</h2>
        <p>
          Solicitá un servicio y observá cómo
          evoluciona la saga en tiempo real.
        </p>

        <div className="form-grid">
          <label>
            Cliente
            <input
              value={customerId}
              onChange={(e) =>
                setCustomerId(e.target.value)
              }
              placeholder="C-9002"
            />
          </label>

          <label>
            Plan
            <select
              value={planId}
              onChange={(e) =>
                setPlanId(e.target.value)
              }
            >
              <option value="PLAN-FIBRA">
                PLAN-FIBRA
              </option>
              <option value="PLAN-FIBRA-PLUS">
                PLAN-FIBRA-PLUS
              </option>
              <option value="PLAN-PREMIUM">
                PLAN-PREMIUM
              </option>
            </select>
          </label>

          <label>
            Simular resultado
            <select
              value={simulateFailure}
              onChange={(e) =>
                setSimulateFailure(e.target.value)
              }
            >
              <option value="none">
                Sin fallo
              </option>
              <option value="billing">
                Fallo de facturación
              </option>
              <option value="provisioning">
                Fallo de aprovisionamiento
              </option>
            </select>
          </label>
        </div>

        <button
          className="primary-button"
          onClick={() => void createActivation()}
          disabled={loading}
        >
          {loading
            ? 'Enviando solicitud...'
            : 'Solicitar activación'}
        </button>

        {error && (
          <p className="error">{error}</p>
        )}
      </section>

      <section className="panel">
        <h2>Seguimiento de la saga</h2>

        {!activation ? (
          <div className="empty-state">
            Todavía no hay una activación
            seleccionada.
          </div>
        ) : (
          <>
            <div className="activation-summary">
              <div>
                <p className="eyebrow">
                  IDENTIFICADOR DE ACTIVACIÓN
                </p>
                <p className="activation-id">
                  {activation._id}
                </p>
                <p>
                  {activation.customerId} ·{' '}
                  {activation.planId}
                </p>
              </div>

              <span
                className={`status status-${activation.status.toLowerCase()}`}
              >
                {activation.status}
              </span>
            </div>

            <div className="timeline">
              {activation.history.map(
                (event, index) => (
                  <div
                    className="timeline-item"
                    key={`${event.eventId}-${index}`}
                  >
                    <div className="timeline-dot" />
                    <div>
                      <strong>
                        {event.eventType}
                      </strong>
                      <p>
                        {new Date(
                          event.at,
                        ).toLocaleString('es-AR')}
                      </p>
                    </div>
                  </div>
                ),
              )}
            </div>
          </>
        )}
      </section>

      <footer>
        POC de activación · NestJS · Kafka ·
        MongoDB · React · Socket.IO
      </footer>
    </main>
  )
}

export default App
