import { createContext, useContext, useEffect, useState } from 'react'
import { authSession, authLogin, authLogout } from '../api.js'

const AuthContext = createContext({ canManage: false, admin_enabled: false, readonly: true })
export function AuthProvider({ children }) {
  const [session, setSession] = useState({ authenticated: false, admin_enabled: false, readonly: true })
  const [loading, setLoading] = useState(true)
  const [sessionError, setSessionError] = useState('')
  const [revision, setRevision] = useState(0)
  const retrySession = () => setRevision(value => value + 1)
  useEffect(() => {
    let active = true
    setLoading(true); setSessionError('')
    authSession().then(data => { if (active) setSession(data) })
      .catch(() => { if (active) setSessionError('无法确认当前访问身份，管理操作暂不可用。') })
      .finally(() => { if (active) setLoading(false) })
    return () => { active = false }
  }, [revision])
  const login = async (username, password) => setSession(await authLogin(username, password))
  const logout = async () => setSession(await authLogout())
  return <AuthContext.Provider value={{ ...session, loading, login, logout, sessionError, retrySession, canManage: !sessionError && session.authenticated && session.admin_enabled && !session.readonly }}>{children}</AuthContext.Provider>
}
export const useAuth = () => useContext(AuthContext)
