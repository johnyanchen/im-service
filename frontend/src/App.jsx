import { useState } from 'react'
import './index.css'
import Login from './components/Login'
import Chat from './components/Chat'

export default function App() {
  const [auth, setAuth] = useState(() => {
    const saved = sessionStorage.getItem('auth')
    return saved ? JSON.parse(saved) : null
  })

  const handleAuth = (data) => {
    sessionStorage.setItem('auth', JSON.stringify(data))
    setAuth(data)
  }

  const handleLogout = () => {
    sessionStorage.removeItem('auth')
    setAuth(null)
  }

  if (!auth) return <Login onAuth={handleAuth} />
  return <Chat auth={auth} onLogout={handleLogout} />
}
