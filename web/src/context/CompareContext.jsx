import { createContext, useContext, useState, useCallback } from 'react'

const CompareContext = createContext(null)

export function CompareProvider({ children }) {
  // [{fund_id, product_name, product_code}]
  const [compareList, setCompareList] = useState([])

  const toggle = useCallback((fund) => {
    setCompareList(prev => {
      const exists = prev.some(f => f.fund_id === fund.fund_id)
      if (exists) return prev.filter(f => f.fund_id !== fund.fund_id)
      if (prev.length >= 8) return prev // max 8
      return [...prev, { fund_id: fund.fund_id, product_name: fund.product_name, product_code: fund.product_code }]
    })
  }, [])

  const remove = useCallback((fundId) => {
    setCompareList(prev => prev.filter(f => f.fund_id !== fundId))
  }, [])

  const clear = useCallback(() => setCompareList([]), [])

  const isSelected = useCallback((fundId) => {
    return compareList.some(f => f.fund_id === fundId)
  }, [compareList])

  return (
    <CompareContext.Provider value={{ compareList, toggle, remove, clear, isSelected }}>
      {children}
    </CompareContext.Provider>
  )
}

export function useCompare() {
  return useContext(CompareContext)
}
