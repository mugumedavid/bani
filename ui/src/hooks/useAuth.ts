import { useAppStore } from '../store/appStore';

const STORAGE_KEY = 'bani_auth_token';

export function useAuth() {
  const token = useAppStore((s) => s.authToken);
  const setToken = useAppStore((s) => s.setAuthToken);

  function login(newToken: string) {
    sessionStorage.setItem(STORAGE_KEY, newToken);
    setToken(newToken);
  }

  function logout() {
    sessionStorage.removeItem(STORAGE_KEY);
    setToken(null);
  }

  return {
    token,
    isAuthenticated: token !== null,
    login,
    logout,
  };
}
