import Icon from './Icon.jsx'
export default function PageState({ loading, error, title, children, onRetry }) {
  return <div className="page-state" role={error ? 'alert' : 'status'}><Icon name={error ? 'warning' : loading ? 'refresh' : 'analytics'} className={loading ? 'animate-spin' : ''} /><h2>{error ? '暂时无法载入' : loading ? '正在整理数据' : title || '暂无数据'}</h2><p>{error || children || (loading ? '请稍候，正在读取最新有效记录。' : '此筛选条件下还没有可展示的记录。')}</p>{onRetry && <button className="button-secondary" onClick={onRetry}>重新载入</button>}</div>
}
